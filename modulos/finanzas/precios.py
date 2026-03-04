import math

# ==========================================
# 💸 COMISIONES OCULTAS (El costo de vender)
# ==========================================
COMISION_MP = 0.045      # ~4.5% MercadoPago (Comisión + IVA)
COMISION_SHOPIFY = 0.02  # 2.0% Shopify (Comisión por usar pasarela externa en plan Basic)
COMISION_TOTAL = COMISION_MP + COMISION_SHOPIFY # Total: 6.5% 

def redondear_precio_bonito(precio):
    """
    Redondea el precio siempre hacia la centena superior.
    Ej: 7180 -> 7200 | 1990 -> 2000
    """
    if precio <= 0:
        return 0
    return int(math.ceil(precio / 100.0) * 100)

def calcular_precio_final(costo_neto_mediven, datos_mercado_sku):
    """
    El Cerebro Financiero.
    Retorna: (precio_venta, estrategia_utilizada)
    """
    # 1. Calcular el Costo Real con IVA
    costo_con_iva = costo_neto_mediven * 1.19
    
    # 2. LA MURALLA DE TITANIO (Márgenes ajustados para absorber envío)
    # 🚚 SUBSIDIO: Subimos los factores para recuperar la pérdida de $2.380 del Courier
    if costo_con_iva <= 3000:
        factor_piso = 1.65  # Antes 1.55 (+10% para recuperar más plata en productos baratos)
    elif costo_con_iva <= 15000:
        factor_piso = 1.52  # Antes 1.45 (+7% absorbe el golpe sin espantar al cliente)
    else:
        factor_piso = 1.40  # Antes 1.35 (+5% en productos caros suma muchos pesos reales)
        
    # El precio piso ahora te protege descontando el 6.5% de TODAS las pasarelas juntas
    precio_piso = (costo_con_iva * factor_piso) / (1 - COMISION_TOTAL)

    # 3. EVALUACIÓN DEL MERCADO
    if not datos_mercado_sku or not datos_mercado_sku.get("datos_mercado"):
        # Subimos el factor de monopolio de 1.60 a 1.65 para que coincida con la nueva estrategia
        precio_final = (costo_con_iva * 1.65) / (1 - COMISION_TOTAL)
        return redondear_precio_bonito(precio_final), "Monopolio (Sin datos)"
        
    # 🛡️ ESCUDO DEFENSIVO: Evitamos el KeyError si el JSON viene corrupto o vacío
    datos_mercado = datos_mercado_sku.get("datos_mercado", {})
    mercado_justo = datos_mercado.get("mediana_competitiva")

    # Si el espía falló y no existe el dato de la mediana, activamos el plan de contingencia
    if not mercado_justo:
        precio_final = (costo_con_iva * 1.65) / (1 - COMISION_TOTAL)
        return redondear_precio_bonito(precio_final), "Monopolio (Datos corruptos)"

    # 4. LA DECISIÓN
    if mercado_justo <= precio_piso:
        return redondear_precio_bonito(precio_piso), "Muralla de Rescate"
        
    # Nos ponemos 2% más baratos, pero siempre validando no romper el piso
    precio_estrategico = mercado_justo * 0.98
    precio_final = max(precio_piso, precio_estrategico)
    
    return redondear_precio_bonito(precio_final), "Francotirador"
