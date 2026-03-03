import math

# Comisión de Pasarela (Ej: MercadoPago 3.49% + IVA = ~4.15%. Usamos 4.5% por seguridad)
COMISION_MP = 0.045

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
    
    # 2. LA MURALLA DE TITANIO (Rentabilidad Mínima Exigida)
    if costo_con_iva <= 3000:
        factor_piso = 1.55
    elif costo_con_iva <= 15000:
        factor_piso = 1.45
    else:
        factor_piso = 1.35
        
    # El precio piso absorbe la comisión de MP para no restarla de tu ganancia
    precio_piso = (costo_con_iva * factor_piso) / (1 - COMISION_MP)

    # 3. EVALUACIÓN DEL MERCADO
    if not datos_mercado_sku or not datos_mercado_sku.get("datos_mercado"):
        precio_final = (costo_con_iva * 1.60) / (1 - COMISION_MP)
        return redondear_precio_bonito(precio_final), "Monopolio (Sin datos)"
        
    mercado_justo = datos_mercado_sku["datos_mercado"]["mediana_competitiva"]

    # 4. LA DECISIÓN
    if mercado_justo <= precio_piso:
        return redondear_precio_bonito(precio_piso), "Muralla de Rescate"
        
    # Nos ponemos 2% más baratos, pero siempre validando no romper el piso
    precio_estrategico = mercado_justo * 0.98
    precio_final = max(precio_piso, precio_estrategico)
    
    return redondear_precio_bonito(precio_final), "Francotirador"