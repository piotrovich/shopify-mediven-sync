"""
Segmentación de productos para fijación de precios.

Tres segmentos:
  - heroe: loss leaders, precio agresivo (-15% bajo el mínimo del mercado).
           Detectados AUTOMÁTICAMENTE en cada corrida desde el catálogo
           Mediven via precargar_heroes_desde_catalogo().
           Adicionalmente, si existe data/heroes.json, sus SKUs se suman
           como overrides manuales.
  - alto_costo: productos donde no podemos competir por precio.
                Detectados por costo Mediven > UMBRAL_ALTO_COSTO o por
                categoría terapéutica (oncológicos, biológicos, etc.).
  - regular: el resto del catálogo. Francotirador -3% bajo mediana.

CANDIDATOS_HEROES soporta un campo opcional 'excluir_patrones' para
descartar formas farmacéuticas no deseadas (supositorios, ampollas, etc.)
incluso cuando matchean los patrones positivos.
"""

import os
import re
import json

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ARCHIVO_HEROES_OVERRIDE = os.path.join(BASE_DIR, "data", "heroes.json")

UMBRAL_ALTO_COSTO = int(os.getenv("UMBRAL_ALTO_COSTO", "40000"))

# ============================================================
#   CANDIDATOS A HÉROE
# ============================================================
# Cada candidato define:
#   - nombre: descripción legible
#   - patrones: lista de regex que deben matchear TODOS en la descripción Mediven
#   - excluir_patrones (opcional): lista de regex; si CUALQUIERA matchea, descarta
#   - max_resultados: cuántos SKUs aceptar por categoría (los de menor costo)
CANDIDATOS_HEROES = [
    # === Analgésicos / Antiinflamatorios ===
    {"nombre": "Paracetamol 500mg comprimidos", "patrones": ["paracetamol", r"\b500\s*mg\b", r"\bcom\b|\bcomp"], "max_resultados": 3},
    {"nombre": "Ibuprofeno 400mg comprimidos", "patrones": ["ibuprofeno", r"\b400\s*mg\b"], "max_resultados": 3},
    {"nombre": "Ibuprofeno 600mg comprimidos",
     "patrones": ["ibuprofeno", r"\b600\s*mg\b"],
     "excluir_patrones": [r"\bsus\b|susp|jarabe|jbe|gotas|gts"],
     "max_resultados": 3},
    {"nombre": "Ibuprofeno 200mg comprimidos",
     "patrones": ["ibuprofeno", r"\b200\s*mg\b"],
     "excluir_patrones": [r"\bsus\b|susp|jarabe|jbe|gotas|gts|/\s*5\s*ml"],
     "max_resultados": 2},
    {"nombre": "Diclofenaco sodico 50mg comprimidos",
     "patrones": ["diclofenac", r"\b50\s*mg\b"],
     "excluir_patrones": [r"\bsup\b|supositor", r"\bamp\b|ampolla", r"\biny\b|inyectable", r"\bgel\b|crema|t[oó]pic"],
     "max_resultados": 3},
    {"nombre": "Naproxeno 550mg", "patrones": ["naproxeno", r"\b550\s*mg\b"], "max_resultados": 2},
    {"nombre": "Tramadol+Paracetamol 37,5/325mg",
     "patrones": ["tramadol", "paracetamol", r"37[,.]5"],
     "max_resultados": 2},

    # === Gastrointestinal ===
    {"nombre": "Omeprazol 20mg", "patrones": ["omeprazol", r"\b20\s*mg\b"], "max_resultados": 3},
    {"nombre": "Lansoprazol 30mg", "patrones": ["lansoprazol", r"\b30\s*mg\b"], "max_resultados": 2},
    {"nombre": "Famotidina 40mg", "patrones": ["famotidina", r"\b40\s*mg\b"], "max_resultados": 2},
    {"nombre": "Loperamida 2mg", "patrones": ["loperamida"], "max_resultados": 2},

    # === Antialérgicos ===
    {"nombre": "Loratadina 10mg", "patrones": ["loratadina", r"\b10\s*mg\b"], "max_resultados": 3},
    {"nombre": "Cetirizina 10mg", "patrones": ["cetirizina", r"\b10\s*mg\b"], "max_resultados": 3},
    {"nombre": "Desloratadina 5mg", "patrones": ["desloratadina"], "max_resultados": 2},

    # === Cardiovascular / Crónicos ===
    {"nombre": "Losartan 50mg", "patrones": ["losart", r"\b50\s*mg\b"], "max_resultados": 3},
    {"nombre": "Enalapril 10mg", "patrones": ["enalapril", r"\b10\s*mg\b"], "max_resultados": 2},
    {"nombre": "Amlodipino 5mg", "patrones": ["amlodipino", r"\b5\s*mg\b"], "max_resultados": 2},
    {"nombre": "Atorvastatina 20mg", "patrones": ["atorvastatina", r"\b20\s*mg\b"], "max_resultados": 3},
    {"nombre": "Atenolol 50mg", "patrones": ["atenolol"], "max_resultados": 2},
    {"nombre": "Hidroclorotiazida 50mg", "patrones": ["hidroclorotiazida", r"\b50\s*mg\b"], "max_resultados": 2},
    {"nombre": "Furosemida 40mg", "patrones": ["furosemida"], "max_resultados": 2},

    # === Endocrino ===
    {"nombre": "Metformina 850mg", "patrones": ["metformina", r"\b850\s*mg\b"], "max_resultados": 3},
    {"nombre": "Levotiroxina 50mcg", "patrones": ["levotiroxina", r"\b50\s*mcg\b|\b50\s*ug\b"], "max_resultados": 3},
    {"nombre": "Levotiroxina 100mcg", "patrones": ["levotiroxina", r"\b100\s*mcg\b|\b100\s*ug\b"], "max_resultados": 3},

    # === Salud mental ===
    {"nombre": "Sertralina 50mg", "patrones": ["sertralina", r"\b50\s*mg\b"], "max_resultados": 3},
    {"nombre": "Sertralina 100mg", "patrones": ["sertralina", r"\b100\s*mg\b"], "max_resultados": 2},
    {"nombre": "Escitalopram 10mg", "patrones": ["escitalopram", r"\b10\s*mg\b"], "max_resultados": 3},
    {"nombre": "Fluoxetina 20mg", "patrones": ["fluoxetina", r"\b20\s*mg\b"], "max_resultados": 2},

    # === Respiratorio ===
    {"nombre": "Salbutamol inhalador", "patrones": ["salbutamol", r"inhal|aerosol|\baer\b|\binh\b|spray"], "max_resultados": 2},
    {"nombre": "Budesonida inhalador", "patrones": ["budesonida", r"inhal|aerosol|\baer\b|\binh\b|polvo|spray|nebul"], "max_resultados": 2},

    # === Pediátrico (regex ampliado: SUS, INF, MAST además de jarabe/gotas/suspensión) ===
    {"nombre": "Paracetamol pediatrico",
     "patrones": ["paracetamol", r"jarabe|jbe|gotas|gts|susp|suspensi[oó]n|\bsus\b|\bped\b|\binf\b|infantil|baby|ni[ñn]os?|mast"],
     "max_resultados": 3},
    {"nombre": "Ibuprofeno pediatrico",
     "patrones": ["ibuprofeno", r"jarabe|jbe|gotas|gts|susp|suspensi[oó]n|\bsus\b|\bped\b|\binf\b|infantil|baby|ni[ñn]os?|mast"],
     "max_resultados": 3},
]

# ============================================================
#   PATRONES DE ALTO COSTO
# ============================================================
# Buscados en (descripción + laboratorio + acción terapéutica) en minúscula.
PATRONES_ALTO_COSTO = [
    # Oncología
    "oncolog", "antineoplas", "antitumor", "citotox", "quimioter",
    # Inmunología
    "inmunosupres", "inmunolog", "inmunomodul",
    # Biológicos
    "biologic", "monoclonal", "anticuerpo",
    # Anticoagulantes de nueva generación (DOACs)
    "apixaban", "rivaroxaban", "dabigatran", "edoxaban",
    # Antivirales hepatitis C nueva generación
    "sofosbuvir", "ledipasvir", "velpatasvir", "glecaprevir",
    # Esclerosis múltiple
    "esclerosis multiple", "interferon beta",
    # Otros biológicos comunes
    "tofacitinib", "adalimumab", "etanercept", "rituximab",
    "infliximab", "trastuzumab", "bevacizumab", "denosumab",
]


# ============================================================
#   CACHE DE HÉROES (se llena en cada corrida del sync)
# ============================================================
_HEROES = set()


def _cargar_overrides_manuales():
    """Lee data/heroes.json si existe. Acepta lista de strings, lista de dicts con 'sku', o dict."""
    if not os.path.exists(ARCHIVO_HEROES_OVERRIDE):
        return set()
    try:
        with open(ARCHIVO_HEROES_OVERRIDE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {
                str(x["sku"]) if isinstance(x, dict) and "sku" in x else str(x)
                for x in data
            }
        if isinstance(data, dict):
            return set(str(k) for k in data.keys())
        return set()
    except Exception:
        return set()


def buscar_candidato(productos_mediven, candidato):
    """
    Devuelve los productos seleccionados para un candidato a héroe.

    Aplica todos los patrones positivos + ningún excluir_patrones,
    ordena por precio asc, devuelve los max_resultados más baratos.
    """
    matches = []
    for prod in productos_mediven:
        descripcion = str(prod.get("Descripcion", "")).lower()
        if not all(re.search(p, descripcion, re.IGNORECASE) for p in candidato["patrones"]):
            continue
        excluir = candidato.get("excluir_patrones", [])
        if excluir and any(re.search(p, descripcion, re.IGNORECASE) for p in excluir):
            continue
        matches.append(prod)
    matches.sort(key=lambda x: float(x.get("Precio", 0) or 0))
    return matches[:candidato.get("max_resultados", 3)]


def precargar_heroes_desde_catalogo(productos_mediven):
    """
    Detecta héroes automáticamente desde el catálogo Mediven.

    Para cada categoría en CANDIDATOS_HEROES, llama a buscar_candidato()
    y agrega los SKUs encontrados. Luego suma overrides de data/heroes.json.
    """
    global _HEROES
    detectados = set()

    for cand in CANDIDATOS_HEROES:
        for prod in buscar_candidato(productos_mediven, cand):
            sku = str(prod.get("Codigo", "")).strip()
            if sku:
                detectados.add(sku)

    overrides = _cargar_overrides_manuales()
    _HEROES = detectados | overrides
    return len(_HEROES)


def cantidad_heroes_cargados():
    return len(_HEROES)


def listar_heroes_cargados():
    """Devuelve copia del set actual de SKUs héroes (útil para debug/diagnóstico)."""
    return set(_HEROES)


def es_heroe(sku):
    return str(sku) in _HEROES


def es_alto_costo(costo_neto, descripcion="", laboratorio="", accion_terapeutica=""):
    """Verdadero si el producto es alto costo por umbral o por categoría."""
    if costo_neto and costo_neto > UMBRAL_ALTO_COSTO:
        return True
    texto = " ".join([
        str(descripcion or ""),
        str(laboratorio or ""),
        str(accion_terapeutica or ""),
    ]).lower()
    return any(p in texto for p in PATRONES_ALTO_COSTO)


def clasificar_producto(sku, descripcion="", costo_neto=0, laboratorio="", accion_terapeutica=""):
    """
    Clasifica el producto en uno de tres segmentos.

    Returns:
        'heroe' | 'alto_costo' | 'regular'

    Héroes tienen prioridad sobre alto_costo.
    """
    if es_heroe(sku):
        return "heroe"
    if es_alto_costo(costo_neto, descripcion, laboratorio, accion_terapeutica):
        return "alto_costo"
    return "regular"
