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

El flujo automatizado es:
    1. sync.py descarga mediven_data
    2. sync.py llama precargar_heroes_desde_catalogo(mediven_data)
    3. clasificar_producto(...) ya tiene la cache cargada y devuelve el segmento

Sin intervención manual.
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
#   - max_resultados: cuántos SKUs aceptar por categoría (los de menor costo)
# La detección elige los más baratos para maximizar margen del loss leader.
CANDIDATOS_HEROES = [
    # === Analgésicos / Antiinflamatorios ===
    {"nombre": "Paracetamol 500mg comprimidos", "patrones": ["paracetamol", r"\b500\s*mg\b", r"\bcom\b|\bcomp"], "max_resultados": 3},
    {"nombre": "Ibuprofeno 400mg comprimidos", "patrones": ["ibuprofeno", r"\b400\s*mg\b"], "max_resultados": 3},
    {"nombre": "Diclofenaco sodico 50mg", "patrones": ["diclofenac", r"\b50\s*mg\b"], "max_resultados": 3},
    {"nombre": "Ketoprofeno 100mg", "patrones": ["ketoprofeno", r"\b100\s*mg\b"], "max_resultados": 2},
    {"nombre": "Naproxeno 550mg", "patrones": ["naproxeno", r"\b550\s*mg\b"], "max_resultados": 2},

    # === Gastrointestinal ===
    {"nombre": "Omeprazol 20mg", "patrones": ["omeprazol", r"\b20\s*mg\b"], "max_resultados": 3},
    {"nombre": "Lansoprazol 30mg", "patrones": ["lansoprazol", r"\b30\s*mg\b"], "max_resultados": 2},
    {"nombre": "Famotidina 20mg", "patrones": ["famotidina"], "max_resultados": 2},
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
    {"nombre": "Hidroclorotiazida 25mg", "patrones": ["hidroclorotiazida"], "max_resultados": 2},
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
    {"nombre": "Salbutamol inhalador", "patrones": ["salbutamol", "inhal|aerosol"], "max_resultados": 2},
    {"nombre": "Budesonida inhalador", "patrones": ["budesonida", "inhal|aerosol"], "max_resultados": 2},

    # === Pediátrico ===
    {"nombre": "Paracetamol pediatrico jarabe/gotas", "patrones": ["paracetamol", "jarabe|jbe|gotas|gts"], "max_resultados": 3},
    {"nombre": "Ibuprofeno pediatrico jarabe/gotas", "patrones": ["ibuprofeno", "jarabe|jbe|gotas|gts"], "max_resultados": 3},
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


def precargar_heroes_desde_catalogo(productos_mediven):
    """
    Detecta héroes automáticamente desde el catálogo Mediven.

    Para cada categoría en CANDIDATOS_HEROES, toma los `max_resultados` más
    baratos. Luego suma los overrides manuales de data/heroes.json (si existe).

    Args:
        productos_mediven: lista de dicts con al menos 'Codigo' y 'Descripcion'.

    Returns:
        Cantidad de SKUs cargados como héroes.
    """
    global _HEROES
    detectados = set()

    for cand in CANDIDATOS_HEROES:
        matches = []
        for prod in productos_mediven:
            descripcion = str(prod.get("Descripcion", "")).lower()
            if all(re.search(p, descripcion, re.IGNORECASE) for p in cand["patrones"]):
                matches.append(prod)
        # Los más baratos primero → loss leaders más rentables
        matches.sort(key=lambda x: float(x.get("Precio", 0) or 0))
        for prod in matches[:cand.get("max_resultados", 3)]:
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
    NOTA: para que la detección de héroes funcione, sync.py debe llamar
    primero a precargar_heroes_desde_catalogo(). Si no se llamó, ningún
    producto será héroe automáticamente (solo los overrides de heroes.json).
    """
    if es_heroe(sku):
        return "heroe"
    if es_alto_costo(costo_neto, descripcion, laboratorio, accion_terapeutica):
        return "alto_costo"
    return "regular"
