"""
Motor financiero v3 — Segmentación por estrategia comercial.

Implementa tres segmentos de productos con lógicas distintas:

  REGULAR (la mayoría del catálogo):
      Francotirador -3% bajo la mediana del mercado.
      Factores piso: 1.35 / 1.30 / 1.22 según tramo de costo+IVA.

  HEROE (loss leaders, atractor de tráfico):
      -15% bajo el MÍNIMO del mercado (no la mediana).
      Factor piso: 1.10 (margen mínimo 10% sobre costo+IVA).
      Si no hay datos confiables, cae automáticamente a estrategia REGULAR.

  ALTO_COSTO (productos caros donde no podemos competir agresivamente):
      Precio = mediana exacta del mercado (multiplicador 1.00).
      Factor piso: 1.22.
      Estrategia: capturar la venta por conveniencia, no por precio.

Defensa en profundidad contra datos corruptos:
  - Si la mediana del mercado quedó bajo costo+IVA × 1.10, se ignoran los
    datos y se cae a Monopolio.
  - Cota superior por MARKUP_MAX: ningún precio supera costo+IVA × 3.5.

Todos los factores son configurables por variables de entorno sin tocar código.
"""

import os
import math

# ============================================================
#   COMISIONES (afectan a todos los segmentos)
# ============================================================
COMISION_MP = float(os.getenv("COMISION_MP", "0.045"))
COMISION_SHOPIFY = float(os.getenv("COMISION_SHOPIFY", "0.02"))
COMISION_TOTAL = COMISION_MP + COMISION_SHOPIFY

# ============================================================
#   ESTRATEGIA REGULAR (default)
# ============================================================
FACTOR_PISO_BAJO = float(os.getenv("FACTOR_PISO_BAJO", "1.35"))      # ≤ $3.000
FACTOR_PISO_MEDIO = float(os.getenv("FACTOR_PISO_MEDIO", "1.30"))    # $3.001 - $15.000
FACTOR_PISO_ALTO = float(os.getenv("FACTOR_PISO_ALTO", "1.22"))      # > $15.000

# Francotirador: -3% bajo mediana
AGRESIVIDAD_FRANCOTIRADOR = float(os.getenv("AGRESIVIDAD_FRANCOTIRADOR", "0.97"))

# ============================================================
#   ESTRATEGIA HÉROE (loss leader)
# ============================================================
FACTOR_PISO_HEROE = float(os.getenv("FACTOR_PISO_HEROE", "1.10"))
# -15% bajo el MÍNIMO del mercado
AGRESIVIDAD_HEROE = float(os.getenv("AGRESIVIDAD_HEROE", "0.85"))

# ============================================================
#   ESTRATEGIA ALTO COSTO (no competir agresivamente)
# ============================================================
FACTOR_PISO_ALTO_COSTO = float(os.getenv("FACTOR_PISO_ALTO_COSTO", "1.22"))
# Multiplicador 1.00 = mediana exacta. Subir a 1.01 si quieres dar 1% sobre, etc.
AGRESIVIDAD_ALTO_COSTO = float(os.getenv("AGRESIVIDAD_ALTO_COSTO", "1.00"))

# ============================================================
#   COMUNES
# ============================================================
# Factor cuando no hay datos del mercado (Monopolio)
FACTOR_MONOPOLIO = float(os.getenv("FACTOR_MONOPOLIO", "1.50"))

# Cota superior absoluta
MARKUP_MAX = float(os.getenv("MARKUP_MAX", "3.5"))

# Si mediana < costo+IVA × este factor, se rechaza como dato corrupto
MEDIANA_MIN_VS_COSTO = float(os.getenv("MEDIANA_MIN_VS_COSTO", "1.10"))


# ============================================================
#   HELPERS
# ============================================================
def redondear_precio_bonito(precio):
    """7180 → 7200, 1990 → 2000. Hacia la centena superior."""
    if precio <= 0:
        return 0
    return int(math.ceil(precio / 100.0) * 100)


def _factor_piso_regular(costo_con_iva):
    if costo_con_iva <= 3000:
        return FACTOR_PISO_BAJO
    if costo_con_iva <= 15000:
        return FACTOR_PISO_MEDIO
    return FACTOR_PISO_ALTO


def _precio_monopolio(costo_con_iva):
    return (costo_con_iva * FACTOR_MONOPOLIO) / (1 - COMISION_TOTAL)


def _aplicar_techo(precio, costo_con_iva):
    return min(precio, costo_con_iva * MARKUP_MAX)


def _extraer_mercado(datos_mercado_sku):
    """Devuelve (mediana, minimo) si los datos están disponibles, sino (None, None)."""
    if not datos_mercado_sku or not datos_mercado_sku.get("datos_mercado"):
        return None, None
    datos = datos_mercado_sku["datos_mercado"]
    return datos.get("mediana_competitiva"), datos.get("minimo")


def _mediana_es_corrupta(mediana, costo_con_iva):
    return mediana is not None and mediana < costo_con_iva * MEDIANA_MIN_VS_COSTO


# ============================================================
#   ESTRATEGIA: REGULAR
# ============================================================
def _calcular_regular(costo_con_iva, mediana):
    factor = _factor_piso_regular(costo_con_iva)
    precio_piso = (costo_con_iva * factor) / (1 - COMISION_TOTAL)

    if not mediana:
        return _aplicar_techo(_precio_monopolio(costo_con_iva), costo_con_iva), "Monopolio (Sin datos)"

    if _mediana_es_corrupta(mediana, costo_con_iva):
        return _aplicar_techo(_precio_monopolio(costo_con_iva), costo_con_iva), "Monopolio (Mediana corrupta)"

    if mediana <= precio_piso:
        return _aplicar_techo(precio_piso, costo_con_iva), "Muralla de Rescate"

    precio_estrategico = mediana * AGRESIVIDAD_FRANCOTIRADOR
    precio = max(precio_piso, precio_estrategico)
    return _aplicar_techo(precio, costo_con_iva), "Francotirador"


# ============================================================
#   ESTRATEGIA: HEROE (loss leader)
# ============================================================
def _calcular_heroe(costo_con_iva, mediana, minimo):
    """
    Loss leader: -15% bajo el MÍNIMO del mercado.
    Si no hay datos confiables, cae a estrategia REGULAR (no podemos ser
    loss leader sin saber el precio de la competencia).
    """
    if not mediana or not minimo:
        precio, estrategia = _calcular_regular(costo_con_iva, mediana)
        return precio, f"Héroe→Regular sin datos · {estrategia}"

    if _mediana_es_corrupta(mediana, costo_con_iva):
        precio, estrategia = _calcular_regular(costo_con_iva, mediana)
        return precio, f"Héroe→Regular datos corruptos · {estrategia}"

    precio_piso = (costo_con_iva * FACTOR_PISO_HEROE) / (1 - COMISION_TOTAL)
    precio_heroe = minimo * AGRESIVIDAD_HEROE
    precio = max(precio_piso, precio_heroe)
    return _aplicar_techo(precio, costo_con_iva), "Héroe (Loss Leader)"


# ============================================================
#   ESTRATEGIA: ALTO COSTO
# ============================================================
def _calcular_alto_costo(costo_con_iva, mediana):
    """No competir agresivamente. Vender a la mediana exacta del mercado."""
    precio_piso = (costo_con_iva * FACTOR_PISO_ALTO_COSTO) / (1 - COMISION_TOTAL)

    if not mediana:
        return _aplicar_techo(_precio_monopolio(costo_con_iva), costo_con_iva), "Alto Costo (Monopolio)"

    if _mediana_es_corrupta(mediana, costo_con_iva):
        return _aplicar_techo(_precio_monopolio(costo_con_iva), costo_con_iva), "Alto Costo (Mediana corrupta)"

    if mediana <= precio_piso:
        return _aplicar_techo(precio_piso, costo_con_iva), "Alto Costo (Muralla)"

    precio_estrategico = mediana * AGRESIVIDAD_ALTO_COSTO
    precio = max(precio_piso, precio_estrategico)
    return _aplicar_techo(precio, costo_con_iva), "Alto Costo (Mediana)"


# ============================================================
#   API PRINCIPAL
# ============================================================
def calcular_precio_final(costo_neto_mediven, datos_mercado_sku, segmento="regular"):
    """
    Calcula el precio final con segmentación.

    Args:
        costo_neto_mediven: costo neto del mayorista (sin IVA).
        datos_mercado_sku: dict con datos del espía o None.
        segmento: 'regular' | 'heroe' | 'alto_costo'. Default: 'regular'.

    Returns:
        (precio_venta_int, descripcion_estrategia)
    """
    if costo_neto_mediven is None or costo_neto_mediven <= 0:
        return 0, "Sin costo"

    costo_con_iva = costo_neto_mediven * 1.19
    mediana, minimo = _extraer_mercado(datos_mercado_sku)

    if segmento == "heroe":
        precio, estrategia = _calcular_heroe(costo_con_iva, mediana, minimo)
    elif segmento == "alto_costo":
        precio, estrategia = _calcular_alto_costo(costo_con_iva, mediana)
    else:
        precio, estrategia = _calcular_regular(costo_con_iva, mediana)

    return redondear_precio_bonito(precio), estrategia
