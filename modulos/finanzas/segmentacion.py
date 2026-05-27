"""
Segmentación de productos para fijación de precios.

Tres segmentos:
  - heroe: loss leaders, precio agresivo (-15% bajo el mínimo del mercado).
           Lista explícita en data/heroes.json.
  - alto_costo: productos donde no podemos competir por precio.
                Detectados por costo Mediven > UMBRAL_ALTO_COSTO o por
                categoría terapéutica (oncológicos, biológicos, etc.).
                Precio: mediana exacta del mercado.
  - regular: el resto del catálogo. Francotirador -3% bajo mediana.

La lista de héroes se carga al importar este módulo. Si cambia mid-runtime,
llamar a recargar_heroes().
"""

import os
import json

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ARCHIVO_HEROES = os.path.join(BASE_DIR, "data", "heroes.json")

UMBRAL_ALTO_COSTO = int(os.getenv("UMBRAL_ALTO_COSTO", "40000"))

# Patrones de texto que marcan categoría de alto costo.
# Se buscan en (descripción + laboratorio + acción terapéutica) en minúscula.
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


def _cargar_heroes():
    """Carga el set de SKUs marcados como héroes."""
    if not os.path.exists(ARCHIVO_HEROES):
        return set()
    try:
        with open(ARCHIVO_HEROES, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Formato aceptado:
        #   - lista de SKUs (strings)
        #   - lista de objetos con campo 'sku'
        #   - dict {sku: info}
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


# Cache cargada al importar
_HEROES = _cargar_heroes()


def recargar_heroes():
    """Recarga la lista de héroes desde disco. Útil tras editar heroes.json."""
    global _HEROES
    _HEROES = _cargar_heroes()


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

    Héroes tienen prioridad sobre alto_costo en caso de superposición.
    """
    if es_heroe(sku):
        return "heroe"
    if es_alto_costo(costo_neto, descripcion, laboratorio, accion_terapeutica):
        return "alto_costo"
    return "regular"


def total_heroes_cargados():
    """Útil para diagnóstico."""
    return len(_HEROES)
