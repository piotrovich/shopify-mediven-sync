"""
Motor financiero v2.

Cambios estructurales:
  1. Estrategia "Atractivo": Francotirador a -3% de la mediana (antes -2%).
  2. Cota superior por MARKUP_MAX: ningún precio final puede superar
     `costo_iva * MARKUP_MAX`. Esto evita que datos sucios de mercado
     hagan vender un producto de costo $7.000 a $95.000.
  3. Rechazo de datos corruptos: si la mediana del mercado quedó bajo
     `costo_iva * 1.10`, se ignoran los datos y se cae a Monopolio.
     (El espía ya hace este check, pero acá tenemos defensa en profundidad.)
  4. Factores configurables via variables de entorno (no requiere tocar
     código para ajustar la estrategia comercial).
"""

import os
import math

# ============================================================
#   CONFIGURACIÓN (parametrizable por env vars)
# ============================================================
COMISION_MP = float(os.getenv("COMISION_MP", "0.045"))
COMISION_SHOPIFY = float(os.getenv("COMISION_SHOPIFY", "0.02"))
COMISION_TOTAL = COMISION_MP + COMISION_SHOPIFY

# Factores de margen mínimo según tramo de costo+IVA
FACTOR_PISO_BAJO = float(os.getenv("FACTOR_PISO_BAJO", "1.65"))     # ≤ $3.000
FACTOR_PISO_MEDIO = float(os.getenv("FACTOR_PISO_MEDIO", "1.52"))   # $3.001 - $15.000
FACTOR_PISO_ALTO = float(os.getenv("FACTOR_PISO_ALTO", "1.40"))     # > $15.000

# Estrategia comercial "Atractivo": Francotirador a -3% de la mediana
AGRESIVIDAD_FRANCOTIRADOR = float(os.getenv("AGRESIVIDAD_FRANCOTIRADOR", "0.97"))

# Factor para Monopolio (cuando no hay datos de mercado)
FACTOR_MONOPOLIO = float(os.getenv("FACTOR_MONOPOLIO", "1.65"))

# Cota superior absoluta: ningún producto se vende a más de N veces costo+IVA
MARKUP_MAX = float(os.getenv("MARKUP_MAX", "3.5"))

# Mediana del mercado mínima aceptable (como múltiplo del costo+IVA).
# Si la mediana queda bajo esto, asumimos datos corruptos y vamos a Monopolio.
MEDIANA_MIN_VS_COSTO = float(os.getenv("MEDIANA_MIN_VS_COSTO", "1.10"))


def redondear_precio_bonito(precio):
    """Redondea hacia la centena superior. 7180 → 7200, 1990 → 2000."""
    if precio <= 0:
        return 0
    return int(math.ceil(precio / 100.0) * 100)


def _factor_piso_para(costo_con_iva):
    if costo_con_iva <= 3000:
        return FACTOR_PISO_BAJO
    if costo_con_iva <= 15000:
        return FACTOR_PISO_MEDIO
    return FACTOR_PISO_ALTO


def _precio_monopolio(costo_con_iva):
    return (costo_con_iva * FACTOR_MONOPOLIO) / (1 - COMISION_TOTAL)


def calcular_precio_final(costo_neto_mediven, datos_mercado_sku):
    """
    Calcula el precio de venta final.

    Returns:
        (precio_venta, estrategia_utilizada)
    """
    if costo_neto_mediven is None or costo_neto_mediven <= 0:
        return 0, "Sin costo"

    costo_con_iva = costo_neto_mediven * 1.19
    factor_piso = _factor_piso_para(costo_con_iva)

    # Piso de margen: cubre comisiones (MP+Shopify) + markup mínimo
    precio_piso = (costo_con_iva * factor_piso) / (1 - COMISION_TOTAL)

    # Techo absoluto por sanidad económica
    precio_techo = costo_con_iva * MARKUP_MAX

    # === Caso A: sin datos de mercado → Monopolio ===
    if not datos_mercado_sku or not datos_mercado_sku.get("datos_mercado"):
        precio = _precio_monopolio(costo_con_iva)
        precio = min(precio, precio_techo)
        return redondear_precio_bonito(precio), "Monopolio (Sin datos)"

    datos = datos_mercado_sku["datos_mercado"]
    mediana = datos.get("mediana_competitiva")

    if not mediana:
        precio = _precio_monopolio(costo_con_iva)
        precio = min(precio, precio_techo)
        return redondear_precio_bonito(precio), "Monopolio (Datos vacíos)"

    # === Caso B: mediana corrupta (bajo el costo) → Monopolio ===
    if mediana < costo_con_iva * MEDIANA_MIN_VS_COSTO:
        precio = _precio_monopolio(costo_con_iva)
        precio = min(precio, precio_techo)
        return redondear_precio_bonito(precio), "Monopolio (Mediana corrupta)"

    # === Caso C: mediana válida pero bajo el piso → Muralla de Rescate ===
    if mediana <= precio_piso:
        precio = min(precio_piso, precio_techo)
        return redondear_precio_bonito(precio), "Muralla de Rescate"

    # === Caso D: mediana sobre el piso → Francotirador ===
    precio_estrategico = mediana * AGRESIVIDAD_FRANCOTIRADOR
    precio = max(precio_piso, precio_estrategico)
    precio = min(precio, precio_techo)
    return redondear_precio_bonito(precio), "Francotirador"
