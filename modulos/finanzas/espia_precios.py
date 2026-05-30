"""
Espía de precios v4 — Fuentes filtradas + endpoint Shopping de Serper.

Cambios estructurales sobre v2:
  1. DOMINIOS_IGNORADOS ampliado:
     - Dr. Simi (marca propia ultra-barata, sesga la mediana)
     - Yapo (venta entre particulares, no es farmacia)
     - Farmacias Populares municipales y Cenabast (precios subsidiados)
  2. FARMACIAS_CONOCIDAS ampliado con nuevos actores reales:
     Farmacias Curie, Araucomed, Farmalisto, Chile SPA, otras.
  3. Limpieza más agresiva de la query enviada a Google:
     - 'CHILE' agregado a HOLDINGS_BASURA (es laboratorio genérico ambiguo
       con el país, sesga la búsqueda).
     - Nuevos SUFIJOS_FARMACEUTICOS_RUIDO: HFA, DSS, ADL, BE, DM, MR, XR,
       SR, CR, ER, LA, FTE — sufijos técnicos que Google interpreta mal.
  4. Doble fuente con Serper:
     - Llamada principal a /search (como antes).
     - Si quedan pocas fuentes (<4), llamada complementaria a /shopping
       (endpoint dedicado de Google Shopping con precios estructurados).
     Esto consume 1-2 créditos por SKU según necesidad.

Mantiene todo el resto de v2 (saneamiento contextual, IQR + mediana,
validación vs costo Mediven, sanity check final).
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
#   FUENTES IGNORADAS
# ============================================================
# Dominios cuyos precios NO representan el mercado comercial real.
DOMINIOS_IGNORADOS = [
    # Propio
    "farmaciaslf.cl",
    # E-commerce general (ruido)
    "mercadolibre.cl", "falabella.com", "paris.cl", "ripley.cl", "linio.cl",
    "aliexpress.com", "amazon.com", "ebay.com", "wish.com", "temu.com",
    # Marca propia ultra-barata: sesga la mediana a la baja, no es
    # competencia directa en el segmento que apuntamos.
    "drsimi.cl",
    # Venta entre particulares (precios sospechosos, productos no regulados)
    "yapo.cl", "yapo.com",
    # Farmacias Populares municipales y Cenabast (precios subsidiados,
    # no representan el mercado privado contra el que competimos).
    "farmaciaspopulares.cl", "farmaciapopular.cl", "farmaciapopular.com",
    "cenabast.cl", "cenabast.gob.cl",
    "minsal.cl", "ispch.cl",
]

# ============================================================
#   FARMACIAS RECONOCIDAS (mapeo dominio → nombre legible)
# ============================================================
# Estas son las farmacias del segmento comercial privado contra las que
# realmente competimos. Si el espía encuentra un dominio aquí, lo usa.
# Si encuentra otro dominio (no en esta lista pero tampoco ignorado),
# también lo acepta y lo etiqueta con el dominio capitalizado.
FARMACIAS_CONOCIDAS = {
    # Cadenas grandes
    "cruzverde.cl": "Cruz Verde",
    "salcobrand.cl": "Salcobrand",
    "ahumada.cl": "Ahumada", "farmaciasahumada.cl": "Ahumada",
    # Medianas / regionales
    "farmex.cl": "Farmex",
    "fraccion.cl": "Fracción",
    "ecofarmacias.cl": "EcoFarmacias",
    "farmaciaelquimico.cl": "El Químico",
    "cofar.cl": "Cofar",
    "profar.cl": "Profar",
    "pharol.cl": "Pharol",
    "redfarma.cl": "Redfarma",
    "maicao.cl": "Maicao",
    "farmaciasknop.com": "Knop",
    "galenica.cl": "Galénica",
    "preunic.cl": "Preunic",
    # Supermercados (con sección farmacia)
    "lider.cl": "Lider", "super.lider.cl": "Lider",
    "jumbo.cl": "Jumbo",
    # Online especializadas
    "mercadofarma.cl": "MercadoFarma",
    "openfarma.cl": "OpenFarma",
    "farmagran.cl": "Farmagran",
    # === NUEVAS (v4) ===
    "farmaciascurie.cl": "Farmacias Curie",
    "farmaciaschilespa.cl": "Farmacias Chile SPA",
    "araucomed.com": "Araucomed", "farmacia.araucomed.com": "Araucomed",
    "farmalisto.cl": "Farmalisto",
    "farmacityrx.cl": "Farmacity",
    "boticadr.cl": "Botica Dr.",
    "farmaciamejor.cl": "Farmacia Mejor",
    "farmacia2030.cl": "Farmacia 2030",
    # === .com chilenos legítimos (v5) — no usan .cl pero son nacionales ===
    "buhochile.com": "Buho Chile",
    "med-chile.com": "Med Chile",
    "autotest-chile.com": "Autotest Chile",
    "mrgreencl.com": "Mr Green",
    "market-care.com": "Market Care", "tienda.market-care.com": "Market Care",
}

# ============================================================
#   FILTRO DE PAÍS (v5): solo mercado chileno
# ============================================================
# .com chilenos legítimos que NO usan .cl (whitelist)
COM_CHILENOS_WHITELIST = {
    "buhochile.com", "araucomed.com", "farmacia.araucomed.com",
    "med-chile.com", "autotest-chile.com", "mrgreencl.com",
    "market-care.com", "tienda.market-care.com",
}

# Plataformas que NO son comercio formal (Instagram, Facebook, etc.)
REDES_Y_NO_COMERCIO = [
    "instagram", "facebook", "tiktok", "threads", "youtube", "twitter",
    "linkedin", "pinterest", "scribd", "ubereats", "rappi",
]

# TLD internacionales explícitos (Argentina, Colombia, México, etc.)
RE_TLD_INTL = re.compile(
    r'\.com\.(ar|co|mx|pe|br|uy|ve|ec|bo|py|gt|sv|do|pa|cr|ni|hn)\b'
    r'|\.(ar|mx|pe|br|uy|ve|ec|bo|py|es|us|pt|co)$',
    re.IGNORECASE
)


def fuente_es_chilena(nombre_o_dominio):
    """
    Decide si una fuente representa el mercado comercial chileno.

    Acepta: dominios .cl, la whitelist de .com chilenos, y nombres
    legibles ya mapeados (sin punto). Rechaza: TLD internacionales,
    redes sociales y cualquier otro .com no listado (conservador).
    """
    s = str(nombre_o_dominio).lower().strip().replace("www.", "")
    if not s:
        return False
    if any(rs in s for rs in REDES_Y_NO_COMERCIO):
        return False
    if RE_TLD_INTL.search(s):
        return False
    if s.endswith(".cl"):
        return True
    if s in COM_CHILENOS_WHITELIST:
        return True
    if "." not in s:  # nombre legible mapeado ("Cruz Verde", etc.)
        return True
    return False

# ============================================================
#   VALIDACIÓN DE PRECIOS
# ============================================================
PRECIO_ABS_MIN = 800
PRECIO_ABS_MAX = 3_000_000   # v7: era 250.000 (rechazaba productos caros sin costo, ej. GLP-1)

# v7: PISO DE SANIDAD (no de rentabilidad).
# El espía debe capturar el precio de mercado REAL, sin sesgarlo por el costo
# de la farmacia. Antes el piso era costo_iva × 1.10, lo que descartaba los
# precios competitivos de las grandes cadenas (que compran más barato que
# Mediven) y dejaba productos premium con medianas infladas o en Monopolio.
# Ahora el piso solo descarta basura evidente (fracciones, errores): acepta
# precios desde el 40% del costo c/IVA. La decisión de "no vender bajo mi
# costo" es responsabilidad del MOTOR (precios.py), no del espía.
FACTOR_PISO_SANIDAD = float(os.getenv("FACTOR_PISO_SANIDAD", "0.40"))
FACTOR_TECHO_VS_COSTO = 8.0

MIN_FUENTES_VALIDAS = 2

# Si después de /search tenemos menos fuentes que esto, complementamos
# con una llamada a /shopping (consume 1 crédito extra de Serper).
UMBRAL_LLAMADA_SHOPPING = int(os.getenv("UMBRAL_LLAMADA_SHOPPING", "4"))

# ============================================================
#   SANEAMIENTO CONTEXTUAL DEL SNIPPET (sin cambios v2)
# ============================================================
PATRONES_BASURA = [
    re.compile(r'\bdespacho[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\benv[ií]os?\b[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bgratis\s+(?:sobre|desde|a\s+partir)[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'precio\s+(?:por\s+)?(?:unidad\s+de\s+medida|fraccionad[ao])[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\$\s?\d[\d.]*\s+por\s+\d+\s*(?:ml|gr?|c[aá]psulas?|comprimidos?|unidad(?:es)?|tabletas?)\b', re.IGNORECASE),
    re.compile(r'\bpor\s+\d+\s*(?:ml|gr?|c[aá]psulas?|comprimidos?|unidad(?:es)?|tabletas?)[^.$]{0,20}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bfraccionad[ao][^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bmetlife[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bisapre[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bconvenio[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bbeneficio[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bahorra[s]?\b[^.$]{0,20}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bdescuento\s+de\b[^.$]{0,20}?\$\s?\d[\d.]*', re.IGNORECASE),
    re.compile(r'\bcompras?\s+(?:sobre|superiores|mayores|m[ií]nim[ao]s?)\b[^.$]{0,40}?\$\s?\d[\d.]*', re.IGNORECASE),
]

def sanear_snippet(texto):
    limpio = texto
    for patron in PATRONES_BASURA:
        limpio = patron.sub(" ", limpio)
    return limpio

# ============================================================
#   CONSTRUCCIÓN DE QUERY (v4: limpieza mejorada)
# ============================================================
TRADUCCIONES = {
    r'\bCOM\b': 'comprimidos', r'\bCAP\b': 'capsulas', r'\bJBE\b': 'jarabe',
    r'\bINY\b': 'inyectable', r'\bFCO\b': 'frasco', r'\bAMP\b': 'ampollas',
    r'\bCRE\b': 'crema', r'\bAER\b': 'aerosol', r'\bUDS\b': 'unidades',
    r'\bUND\b': 'unidades', r'\bSAB\b': 'sabor', r'\bPVO\b': 'polvo',
    r'\bSBR\b': 'sobres', r'\bLOC\b': 'locion', r'\bGTS\b': 'gotas',
    r'\bUNG\b': 'unguento', r'\bSUP\b': 'supositorios', r'\bSOL\b': 'solucion',
    r'\bSUSP\b': 'suspension', r'\bSUS\b': 'suspension',
    r'\bACO\b': 'acondicionador', r'\bSH\b': 'shampoo',
    r'\bMATIF\b': 'matificante', r'\bSPY\b': 'spray', r'\bCOMP\b': 'comprimidos',
    r'\bSHA\b': 'shampoo', r'\bCEP\b': 'cepillo', r'\bDEN\b': 'dental',
    r'\bDENT\b': 'dental', r'\bTOA\b': 'toalla', r'\bUF\b': 'ultra fina',
    r'\bC/A\b': 'con alas', r'\bS/A\b': 'sin alas', r'\bJAB\b': 'jabon',
    r'\bOFT\b': 'oftalmica', r'\bPED\b': 'pediatrico', r'\bOSC\b': 'oscuro',
    r'\bINH\b': 'inhalador', r'\bINF\b': 'infantil', r'\bMAST\b': 'masticable',
}

# v4: Sufijos farmacéuticos que CONFUNDEN a Google y deben eliminarse de
# la query (no aportan al benchmark del precio del producto).
SUFIJOS_FARMACEUTICOS_RUIDO = [
    r'\bHFA[-\s]?LA\b',   # HFA-LA del propelente de inhaladores
    r'\bHFA\b',
    r'\bDSS\b',           # Dispositivo dosificador
    r'\bADL\b',           # Adulto (cuando viene como sufijo)
    r'\bBE\b',            # Bioequivalente
    r'\(BE\)',            # (BE) entre paréntesis
    r'\bDM\b', r'\(DM\)', # DM = ?
    r'\bMR\b', r'\bXR\b', r'\bSR\b', r'\bCR\b', r'\bER\b',  # Modified/Extended/Sustained release
    r'\bLA\b',            # Long acting
    r'\bFTE\b',           # Fuerte
    r'\bCMPT\b',          # Compuesto
    r'\bRET\b',           # Retard
    r'\bIV\b', r'\bIM\b', r'\bSC\b',  # Vías inyectables (no aportan al precio retail)
]

HOLDINGS_BASURA = [
    "BEIERSDORF", "GSK", "PERFUMERIA", "DURANDIN", "PROCTER & GAMBL",
    "PROCTER", "GAMBL", "LOREAL VICHY LA", "LOREAL", "VICHY",
    "CMPC", "TISSUE", "CONSUMO", "OTC", "LASTRADE", "JOHNSON", "DENTAID",
    # v4: 'CHILE' como laboratorio es ambiguo con el país de búsqueda.
    # Si lo dejamos, Google interpreta "CHILE" como ubicación y sesga.
    "CHILE",
]


def construir_query(nombre_producto, laboratorio):
    nombre = nombre_producto.replace('+', ' ').replace('/', ' ').split("(")[0].strip()

    # Traducir abreviaciones a palabras
    for patron, real in TRADUCCIONES.items():
        nombre = re.sub(patron, real, nombre, flags=re.IGNORECASE)

    # v4: Eliminar sufijos farmacéuticos ruidosos
    for patron in SUFIJOS_FARMACEUTICOS_RUIDO:
        nombre = re.sub(patron, ' ', nombre, flags=re.IGNORECASE)

    # v4: Eliminar también del nombre los holdings/labs basura
    # (ej: 'CHILE' viene como laboratorio embebido en la descripción
    # Mediven y, sin limpieza, sesga Google hacia el país)
    for h in HOLDINGS_BASURA:
        nombre = re.sub(rf'\b{re.escape(h)}\b', '', nombre, flags=re.IGNORECASE)

    # Eliminar conectores
    nombre = re.sub(r'\b(X|PARA|EL|LA|LOS|LAS|DE|CON)\b', '', nombre, flags=re.IGNORECASE)
    nombre = " ".join(nombre.split())
    if len(nombre.split()) > 6:
        nombre = " ".join(nombre.split()[:6])

    # Limpiar laboratorio (quitar holdings genéricos y palabras ambiguas)
    lab = (laboratorio or "").upper()
    for h in HOLDINGS_BASURA:
        lab = re.sub(rf'\b{re.escape(h)}\b', '', lab).strip()
    lab = " ".join(lab.split())

    if lab and lab.lower() not in nombre.lower():
        return f'{nombre} {lab} precio farmacia'
    return f'{nombre} precio farmacia'


# ============================================================
#   EXTRACCIÓN DE PRECIOS
# ============================================================
REGEX_PRECIO = re.compile(r'\$\s?(\d{1,3}(?:[.,]\d{3})+|\d{3,6})')

def extraer_precios_de_texto(texto):
    crudos = REGEX_PRECIO.findall(texto)
    precios = []
    for m in crudos:
        num = int(re.sub(r'[.,]', '', m))
        precios.append(num)
    return precios

def validar_precio(precio, costo_neto_mediven=None):
    if costo_neto_mediven and costo_neto_mediven > 0:
        costo_iva = costo_neto_mediven * 1.19
        piso = max(PRECIO_ABS_MIN, costo_iva * FACTOR_PISO_SANIDAD)
        techo = costo_iva * FACTOR_TECHO_VS_COSTO
        return piso <= precio <= techo
    return PRECIO_ABS_MIN <= precio <= PRECIO_ABS_MAX

def reducir_precios_de_link(precios, costo_neto_mediven=None):
    validos = [p for p in precios if validar_precio(p, costo_neto_mediven)]
    if not validos:
        return None
    if len(validos) == 1:
        return validos[0]
    if len(validos) == 2:
        return max(validos)
    return int(statistics.median(validos))


# ============================================================
#   FILTRO DE OUTLIERS (IQR + refuerzo por mediana, sin cambios v2)
# ============================================================
def filtrar_outliers_iqr(precios):
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
#   LLAMADAS A SERPER
# ============================================================
def _llamar_serper(endpoint, query):
    """
    Llama al endpoint de Serper indicado ('search' o 'shopping').
    Devuelve el JSON de respuesta o None si falla.
    """
    if not SERPER_API_KEY:
        return None
    url = f"https://google.serper.dev/{endpoint}"
    payload = json.dumps({"q": query, "gl": "cl", "hl": "es"})
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    try:
        resp = requests.post(url, headers=headers, data=payload, timeout=10)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


def precios_desde_shopping_items(items, costo_neto_mediven=None, dominios_vistos=None):
    """
    Parsea items del bloque/endpoint shopping de Serper.
    Cada item viene con campos estructurados: title, source, link, price.
    """
    if dominios_vistos is None:
        dominios_vistos = set()
    resultados = []
    for item in items or []:
        price_raw = item.get("price") or ""
        precios = extraer_precios_de_texto(price_raw)
        if not precios:
            continue
        precio = precios[0]
        if not validar_precio(precio, costo_neto_mediven):
            continue

        # Determinar fuente y dominio
        source = (item.get("source") or "").lower()
        link = item.get("link") or ""
        dominio = urlparse(link).netloc.replace('www.', '').lower() if link else source

        # Aplicar filtros de dominio
        # Aplicar filtros: ignorados explícitos + solo mercado chileno.
        # Para el país nos fiamos del DOMINIO (el source crudo de Serper
        # puede ser un nombre neutro que oculta el país real).
        if any(ig in source or ig in dominio for ig in DOMINIOS_IGNORADOS):
            continue
        identificador_pais = dominio if dominio else source
        if not fuente_es_chilena(identificador_pais):
            continue
        if dominio in dominios_vistos:
            continue

        farmacia = FARMACIAS_CONOCIDAS.get(dominio, source.title() or dominio.capitalize() or "Shopping")
        resultados.append({"farmacia": farmacia, "precio": precio})
        if dominio:
            dominios_vistos.add(dominio)
    return resultados


# ============================================================
#   FUNCIÓN PRINCIPAL
# ============================================================
def buscar_precio_competencia(nombre_producto, laboratorio="", costo_neto_mediven=None):
    """
    Busca el precio competitivo en el mercado para un producto.

    Flujo:
        1. Llama a Serper /search con query construida limpia.
        2. Procesa shopping items (si vinieron) + resultados orgánicos.
        3. Si quedan <UMBRAL_LLAMADA_SHOPPING fuentes válidas, complementa
           con una llamada al endpoint /shopping (precios estructurados).
        4. Filtra outliers (IQR + refuerzo por mediana).
        5. Sanity check final contra costo Mediven.

    Returns:
        dict {detalle, minimo, mediana_competitiva, fuentes_validas}
        o None si no se pudo construir un estudio confiable.
    """
    if not SERPER_API_KEY:
        return None

    query = construir_query(nombre_producto, laboratorio)
    data_search = _llamar_serper("search", query)
    if not data_search:
        return None

    precios_encontrados = []
    dominios_vistos = set()

    # 1) Shopping items que vinieron dentro de /search (si los hay)
    precios_encontrados.extend(
        precios_desde_shopping_items(
            data_search.get("shopping", []),
            costo_neto_mediven,
            dominios_vistos,
        )
    )

    # 2) Resultados orgánicos con saneamiento contextual
    for organic in data_search.get("organic", []):
        link = organic.get("link", "")
        snippet_raw = (organic.get("snippet", "") + " " + organic.get("title", ""))
        dominio = urlparse(link).netloc.replace('www.', '').lower()

        if any(ig in dominio for ig in DOMINIOS_IGNORADOS):
            continue
        if not fuente_es_chilena(dominio):
            continue
        if dominio in dominios_vistos:
            continue

        snippet_limpio = sanear_snippet(snippet_raw)
        precios_link = extraer_precios_de_texto(snippet_limpio)

        precio_final = reducir_precios_de_link(precios_link, costo_neto_mediven)
        if precio_final is None:
            continue

        farmacia = FARMACIAS_CONOCIDAS.get(dominio, dominio.capitalize())
        precios_encontrados.append({"farmacia": farmacia, "precio": precio_final})
        dominios_vistos.add(dominio)

    # 3) Si tenemos pocas fuentes, complementar con endpoint /shopping
    if len(precios_encontrados) < UMBRAL_LLAMADA_SHOPPING:
        data_shopping = _llamar_serper("shopping", query)
        if data_shopping:
            precios_encontrados.extend(
                precios_desde_shopping_items(
                    data_shopping.get("shopping", []),
                    costo_neto_mediven,
                    dominios_vistos,
                )
            )

    if not precios_encontrados:
        return None

    # 4) Filtro de outliers
    todos = [p["precio"] for p in precios_encontrados]
    validos_iqr = filtrar_outliers_iqr(todos)
    set_validos = list(validos_iqr)

    detalle = []
    for p in precios_encontrados:
        if p["precio"] in set_validos:
            detalle.append({**p, "estado": "🟢 Válido"})
            set_validos.remove(p["precio"])
        else:
            detalle.append({**p, "estado": "🔴 Descartado (outlier IQR)"})

    if len(validos_iqr) < MIN_FUENTES_VALIDAS:
        return None

    minimo = min(validos_iqr)
    mediana = int(statistics.median(validos_iqr))

    # 5) Sanity check final: descartar solo estudios basura (mediana absurdamente
    #    baja respecto al costo). Usa el piso de sanidad, NO el de rentabilidad:
    #    una mediana real bajo el costo es información válida (el motor decidirá
    #    qué hacer), no un dato para tirar a Monopolio.
    if costo_neto_mediven and costo_neto_mediven > 0:
        costo_iva = costo_neto_mediven * 1.19
        if mediana < costo_iva * FACTOR_PISO_SANIDAD:
            return None

    return {
        "detalle": detalle,
        "minimo": minimo,
        "mediana_competitiva": mediana,
        "fuentes_validas": len(validos_iqr),
    }
