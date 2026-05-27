"""
Espía de precios v2.

Cambios estructurales sobre la versión anterior:
  1. Saneamiento contextual: elimina del snippet las frases conocidas que
     contaminan el regex (despacho, envío, fraccionado, "por N ml", etc.)
     ANTES de extraer precios.
  2. Validación contra costo neto Mediven: si se entrega, descarta cualquier
     precio que esté bajo el costo+IVA (es físicamente imposible que el
     retail venda más barato que tu propio mayorista) o sobre 8x el costo.
  3. Filtro de outliers por IQR (cuartiles) + refuerzo por mediana, más
     robusto que "0.5x a 2x la mediana cruda" cuando hay basura.
  4. Aprovecha el campo "shopping" de Serper cuando viene en la respuesta
     (precios estructurados, sin necesidad de regex).
  5. Sanity check final: si la mediana competitiva queda bajo costo+IVA,
     se descarta el estudio (devuelve None) y el sistema cae a Monopolio.
"""

import os
import json
import re
import statistics
import requests
from urllib.parse import urlparse
from dotenv import load_dotenv

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv(os.path.join(BASE_DIR, ".env"))

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
ARCHIVO_MERCADO = os.path.join(BASE_DIR, "data", "precios_mercado.json")

# ============================================================
#   CONFIGURACIÓN
# ============================================================
DOMINIOS_IGNORADOS = [
    "farmaciaslf.cl", "mercadolibre.cl", "falabella.com",
    "paris.cl", "ripley.cl", "linio.cl", "aliexpress.com",
    "amazon.com", "ebay.com", "wish.com", "temu.com",
]

FARMACIAS_CONOCIDAS = {
    "farmex.cl": "Farmex", "salcobrand.cl": "Salcobrand", "cruzverde.cl": "Cruz Verde",
    "ahumada.cl": "Ahumada", "fraccion.cl": "Fracción", "ecofarmacias.cl": "EcoFarmacias",
    "farmaciaelquimico.cl": "El Químico", "drsimi.cl": "Dr. Simi", "cofar.cl": "Cofar",
    "profar.cl": "Profar", "pharol.cl": "Pharol", "redfarma.cl": "Redfarma",
    "maicao.cl": "Maicao", "farmaciasknop.com": "Knop", "galenica.cl": "Galénica",
    "lider.cl": "Lider", "super.lider.cl": "Lider", "jumbo.cl": "Jumbo", "preunic.cl": "Preunic",
    "mercadofarma.cl": "MercadoFarma", "openfarma.cl": "OpenFarma", "farmagran.cl": "Farmagran",
}

# Validación absoluta (cuando no tenemos costo Mediven de referencia)
PRECIO_ABS_MIN = 800
PRECIO_ABS_MAX = 250_000

# Validación relativa al costo Mediven (cuando sí lo tenemos)
# Un precio retail razonable está entre 1.1x y 8x el costo+IVA del mayorista.
FACTOR_PISO_VS_COSTO = 1.10
FACTOR_TECHO_VS_COSTO = 8.0

# Cantidad mínima de fuentes válidas para que un estudio sea confiable
MIN_FUENTES_VALIDAS = 2

# ============================================================
#   SANEAMIENTO CONTEXTUAL DEL SNIPPET
# ============================================================
# Patrones que identifican fragmentos donde el "$N" NO es precio del producto.
# Cada patrón captura su contexto + el $N adyacente, y lo borramos del snippet.
PATRONES_BASURA = [
    # "Despacho gratis sobre $25.000", "Envío desde $1.500"
    re.compile(r'\bdespacho[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\benv[ií]os?\b[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bgratis\s+(?:sobre|desde|a\s+partir)[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),

    # Precios fraccionados / por unidad de medida
    re.compile(r'precio\s+(?:por\s+)?(?:unidad\s+de\s+medida|fraccionad[ao])[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\$\s?\d[\d.]*\s+por\s+\d+\s*(?:ml|gr?|c[aá]psulas?|comprimidos?|unidad(?:es)?|tabletas?)\b', re.IGNORECASE),
    re.compile(r'\bpor\s+\d+\s*(?:ml|gr?|c[aá]psulas?|comprimidos?|unidad(?:es)?|tabletas?)[^.$]{0,20}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bfraccionad[ao][^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),

    # Convenios / beneficios (precios subsidiados, no representan mercado)
    re.compile(r'\bmetlife[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bisapre[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bconvenio[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bbeneficio[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),

    # Descuentos genéricos sin contexto de producto ("Ahorra $500")
    re.compile(r'\bahorra[s]?\b[^.$]{0,20}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bdescuento\s+de\b[^.$]{0,20}?\$\s?\d[\d.]*', re.IGNORECASE),

    # Compras mínimas
    re.compile(r'\bcompras?\s+(?:sobre|superiores|mayores|m[ií]nim[ao]s?)\b[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
]

def sanear_snippet(texto):
    """Quita del snippet los fragmentos con $N que no son precio del producto."""
    limpio = texto
    for patron in PATRONES_BASURA:
        limpio = patron.sub(" ", limpio)
    return limpio

# ============================================================
#   NORMALIZACIÓN DEL NOMBRE PARA LA QUERY
# ============================================================
TRADUCCIONES = {
    r'\bCOM\b': 'comprimidos', r'\bCAP\b': 'capsulas', r'\bJBE\b': 'jarabe',
    r'\bINY\b': 'inyectable', r'\bFCO\b': 'frasco', r'\bAMP\b': 'ampollas',
    r'\bCRE\b': 'crema', r'\bAER\b': 'aerosol', r'\bUDS\b': 'unidades',
    r'\bUND\b': 'unidades', r'\bSAB\b': 'sabor', r'\bPVO\b': 'polvo',
    r'\bSBR\b': 'sobres', r'\bLOC\b': 'locion', r'\bGTS\b': 'gotas',
    r'\bUNG\b': 'unguento', r'\bSUP\b': 'supositorios', r'\bSOL\b': 'solucion',
    r'\bSUSP\b': 'suspension', r'\bACO\b': 'acondicionador', r'\bSH\b': 'shampoo',
    r'\bMATIF\b': 'matificante', r'\bSPY\b': 'spray', r'\bCOMP\b': 'comprimidos',
    r'\bSHA\b': 'shampoo', r'\bCEP\b': 'cepillo', r'\bDEN\b': 'dental',
    r'\bDENT\b': 'dental', r'\bTOA\b': 'toalla', r'\bUF\b': 'ultra fina',
    r'\bC/A\b': 'con alas', r'\bS/A\b': 'sin alas', r'\bJAB\b': 'jabon',
    r'\bOFT\b': 'oftalmica', r'\bPED\b': 'pediatrico', r'\bOSC\b': 'oscuro',
}

HOLDINGS_BASURA = [
    "BEIERSDORF", "GSK", "PERFUMERIA", "DURANDIN", "PROCTER & GAMBL",
    "PROCTER", "GAMBL", "LOREAL VICHY LA", "LOREAL", "VICHY",
    "CMPC", "TISSUE", "CONSUMO", "OTC", "LASTRADE", "JOHNSON", "DENTAID",
]

def construir_query(nombre_producto, laboratorio):
    nombre = nombre_producto.replace('+', ' ').replace('/', ' ').split("(")[0].strip()
    for patron, real in TRADUCCIONES.items():
        nombre = re.sub(patron, real, nombre, flags=re.IGNORECASE)
    nombre = re.sub(r'\b(X|x|PARA|EL|LA|LOS|LAS|DE|CON)\b', '', nombre, flags=re.IGNORECASE)
    nombre = " ".join(nombre.split())
    if len(nombre.split()) > 6:
        nombre = " ".join(nombre.split()[:6])

    lab = laboratorio.upper()
    for h in HOLDINGS_BASURA:
        lab = lab.replace(h, "").strip()

    if lab and lab.lower() not in nombre.lower():
        return f'{nombre} {lab} precio farmacia'
    return f'{nombre} precio farmacia'

# ============================================================
#   EXTRACCIÓN DE PRECIOS
# ============================================================
REGEX_PRECIO = re.compile(r'\$\s?(\d{1,3}(?:[.,]\d{3})+|\d{3,6})')

def extraer_precios_de_texto(texto):
    """Saca todos los números con formato de precio del texto."""
    crudos = REGEX_PRECIO.findall(texto)
    precios = []
    for m in crudos:
        num = int(re.sub(r'[.,]', '', m))
        precios.append(num)
    return precios

def validar_precio(precio, costo_neto_mediven=None):
    """Verifica que el precio sea plausible para el producto."""
    if costo_neto_mediven and costo_neto_mediven > 0:
        costo_iva = costo_neto_mediven * 1.19
        piso = costo_iva * FACTOR_PISO_VS_COSTO
        techo = costo_iva * FACTOR_TECHO_VS_COSTO
        return piso <= precio <= techo
    return PRECIO_ABS_MIN <= precio <= PRECIO_ABS_MAX

def reducir_precios_de_link(precios, costo_neto_mediven=None):
    """
    Reduce varios precios extraídos de un mismo snippet a UN precio.
    Estrategia:
      - 1 precio: ese
      - 2 precios: el mayor (el menor suele ser despacho o fraccionado)
      - 3+ precios: mediana (más robusto contra basura mezclada con producto)
    """
    validos = [p for p in precios if validar_precio(p, costo_neto_mediven)]
    if not validos:
        return None
    if len(validos) == 1:
        return validos[0]
    if len(validos) == 2:
        return max(validos)
    return int(statistics.median(validos))

# ============================================================
#   FILTRO DE OUTLIERS (IQR + refuerzo por mediana)
# ============================================================
def filtrar_outliers_iqr(precios):
    """
    Filtro de outliers de doble cinturón:
      1. Regla IQR estándar (Q1 - 1.5*IQR, Q3 + 1.5*IQR).
      2. Refuerzo vs mediana: ningún precio puede ser >3x o <1/3 la mediana.
         Esto cubre el caso donde el IQR se infla por outliers acumulados a
         un lado y el filtro estándar deja pasar basura.
    """
    if len(precios) < 3:
        return list(precios)
    ordenados = sorted(precios)
    mediana = statistics.median(ordenados)

    if len(ordenados) >= 4:
        q1, _, q3 = statistics.quantiles(ordenados, n=4)
        iqr = q3 - q1
        limite_iqr_inf = q1 - 1.5 * iqr
        limite_iqr_sup = q3 + 1.5 * iqr
    else:
        limite_iqr_inf = float("-inf")
        limite_iqr_sup = float("inf")

    limite_mediana_inf = mediana / 3
    limite_mediana_sup = mediana * 3

    return [
        p for p in ordenados
        if limite_iqr_inf <= p <= limite_iqr_sup
        and limite_mediana_inf <= p <= limite_mediana_sup
    ]

# ============================================================
#   APROVECHAR SHOPPING RESULTS DE SERPER
# ============================================================
def precios_desde_shopping(data, costo_neto_mediven=None):
    """
    Si Serper devolvió un bloque 'shopping', sacamos precios estructurados
    (no requieren regex porque vienen con campo 'price' parseado).
    """
    resultados = []
    for item in data.get("shopping", []):
        price_raw = item.get("price") or ""
        precios = extraer_precios_de_texto(price_raw)
        if not precios:
            continue
        precio = precios[0]
        if not validar_precio(precio, costo_neto_mediven):
            continue
        fuente = item.get("source") or item.get("title") or "Shopping"
        # Saltamos shopping items de dominios ignorados
        if any(ig in fuente.lower() for ig in DOMINIOS_IGNORADOS):
            continue
        resultados.append({"farmacia": fuente, "precio": precio})
    return resultados

# ============================================================
#   FUNCIÓN PRINCIPAL
# ============================================================
def buscar_precio_competencia(nombre_producto, laboratorio="", costo_neto_mediven=None):
    """
    Busca el precio competitivo en el mercado para un producto.

    Args:
        nombre_producto: descripción del producto (de Mediven).
        laboratorio: laboratorio (opcional, mejora la query).
        costo_neto_mediven: costo neto del mayorista (opcional pero recomendado).
            Si se entrega, valida que los precios encontrados estén en un rango
            plausible respecto al costo+IVA.

    Returns:
        dict con {detalle, minimo, mediana_competitiva, fuentes_validas} o None
        si no se pudo construir un estudio confiable.
    """
    if not SERPER_API_KEY:
        return None

    query = construir_query(nombre_producto, laboratorio)
    payload = json.dumps({"q": query, "gl": "cl", "hl": "es"})
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}

    try:
        resp = requests.post(
            "https://google.serper.dev/search",
            headers=headers, data=payload, timeout=10
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
    except Exception:
        return None

    precios_encontrados = []
    dominios_vistos = set()

    # 1) Aprovechar el bloque 'shopping' si vino (más confiable)
    for item in precios_desde_shopping(data, costo_neto_mediven):
        precios_encontrados.append(item)

    # 2) Parsear los resultados orgánicos con saneamiento contextual
    for organic in data.get("organic", []):
        link = organic.get("link", "")
        snippet_raw = (organic.get("snippet", "") + " " + organic.get("title", ""))
        dominio = urlparse(link).netloc.replace('www.', '').lower()

        if any(ig in dominio for ig in DOMINIOS_IGNORADOS):
            continue
        if dominio in dominios_vistos:
            continue

        # SANEAMOS antes de extraer
        snippet_limpio = sanear_snippet(snippet_raw)
        precios_link = extraer_precios_de_texto(snippet_limpio)

        precio_final = reducir_precios_de_link(precios_link, costo_neto_mediven)
        if precio_final is None:
            continue

        farmacia = FARMACIAS_CONOCIDAS.get(dominio, dominio.capitalize())
        precios_encontrados.append({"farmacia": farmacia, "precio": precio_final})
        dominios_vistos.add(dominio)

    if not precios_encontrados:
        return None

    # 3) Filtro de outliers por IQR + refuerzo por mediana
    todos = [p["precio"] for p in precios_encontrados]
    validos_iqr = filtrar_outliers_iqr(todos)
    set_validos = list(validos_iqr)  # copia mutable para marcar

    detalle = []
    for p in precios_encontrados:
        if p["precio"] in set_validos:
            detalle.append({**p, "estado": "🟢 Válido"})
            set_validos.remove(p["precio"])
        else:
            detalle.append({**p, "estado": "🔴 Descartado (outlier IQR)"})

    if len(validos_iqr) < MIN_FUENTES_VALIDAS:
        # Muestra muy chica → no confiamos. Mejor caer a Monopolio.
        return None

    minimo = min(validos_iqr)
    mediana = int(statistics.median(validos_iqr))

    # 4) Sanity check final contra costo Mediven
    if costo_neto_mediven and costo_neto_mediven > 0:
        costo_iva = costo_neto_mediven * 1.19
        if mediana < costo_iva * FACTOR_PISO_VS_COSTO:
            # Mediana absurda (más barata que tu propio mayorista) → datos corruptos
            return None

    return {
        "detalle": detalle,
        "minimo": minimo,
        "mediana_competitiva": mediana,
        "fuentes_validas": len(validos_iqr),
    }
