from precios import calcular_precio_final

print("--- SIMULADOR FINANCIERO DE FARMACIAS LF ---")

# CASO 1: OPORTUNIDAD (Listerine)
# Costo neto: $3.000. Mercado lo vende a $7.190
costo = 3000
mercado = {"datos_mercado": {"mediana_competitiva": 7190}}
precio, estrategia = calcular_precio_final(costo, mercado)
print(f"CASO 1 (Listerine) -> Costo: ${costo} | Precio Venta: ${precio} | Estrategia: {estrategia}")

# CASO 2: ANOMALÍA TEGADERM (La competencia vende por unidad)
# Costo neto caja: $25.000. Mercado dice: $1.900
costo = 25000
mercado = {"datos_mercado": {"mediana_competitiva": 1900}}
precio, estrategia = calcular_precio_final(costo, mercado)
print(f"CASO 2 (Tegaderm Caja) -> Costo: ${costo} | Precio Venta: ${precio} | Estrategia: {estrategia}")

# CASO 3: MONOPOLIO (Nadie más lo tiene)
# Costo neto: $8.000. Mercado: Nada.
costo = 8000
mercado = {"datos_mercado": None}
precio, estrategia = calcular_precio_final(costo, mercado)
print(f"CASO 3 (Exclusivo) -> Costo: ${costo} | Precio Venta: ${precio} | Estrategia: {estrategia}")

# CASO 4: GUERRA DE PRECIOS (Competencia súper agresiva)
# Costo neto: $5.000. Mercado lo vende a $6.000 (casi al costo con IVA)
costo = 5000
mercado = {"datos_mercado": {"mediana_competitiva": 6000}}
precio, estrategia = calcular_precio_final(costo, mercado)
print(f"CASO 4 (Guerra) -> Costo: ${costo} | Precio Venta: ${precio} | Estrategia: {estrategia}")
