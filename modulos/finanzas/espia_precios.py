import os
import sys
import json
import time
import requests
import re
import statistics  # 🧠 Nueva librería para la inteligencia matemática
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv

# --- GPS DEL PROYECTO ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv(os.path.join(BASE_DIR, ".env"))
# ------------------------

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
ARCHIVO_MEDIVEN = os.path.join(BASE_DIR, "mediven_full.json")
ARCHIVO_MERCADO = os.path.join(BASE_DIR, "data", "precios_mercado.json")

# 📉 LÍMITE REDUCIDO A 10 PARA VER EL DETALLE DE LA LÓGICA
LIMITE_PRUEBA = 0

# 🚫 LISTA NEGRA: Marketplaces (Packs) y tu propia web (Inception)
DOMINIOS_IGNORADOS = [
    "farmaciaslf.cl", "mercadolibre.cl", "falabella.com", 
    "paris.cl", "ripley.cl", "linio.cl", "aliexpress.com"
]

FARMACIAS_CONOCIDAS = {
    "farmex.cl": "Farmex",
    "salcobrand.cl": "Salcobrand",
    "cruzverde.cl": "Cruz Verde",
    "ahumada.cl": "Ahumada",
    "fraccion.cl": "Fracción",
    "ecofarmacias.cl": "EcoFarmacias",
    "farmaciaelquimico.cl": "El Químico",
    "drsimi.cl": "Dr. Simi",
    "cofar.cl": "Cofar",
    "profar.cl": "Profar",
    "pharol.cl": "Pharol",
    "redfarma.cl": "Redfarma",
    "maicao.cl": "Maicao",
    "farmaciasknop.com": "Knop",
    "galenica.cl": "Galénica",
    "lider.cl": "Lider",
    "super.lider.cl": "Lider",
    "jumbo.cl": "Jumbo",
    "preunic.cl": "Preunic"
}

def buscar_precio_competencia(nombre_producto, laboratorio=""):
    url = "https://google.serper.dev/search"
    
    # 1. Quitamos lo que está entre paréntesis (suelen ser categorías genéricas)
    nombre_limpio = nombre_producto.split("(")[0].strip()
    
    # 2. Borramos la "x" aislada que usa Mediven (Ej: "Aceite x 30 ml" -> "Aceite 30 ml")
    nombre_limpio = re.sub(r'\b(X|x)\b', '', nombre_limpio)
    
    # 3. Limpiamos espacios dobles sobrantes
    nombre_limpio = " ".join(nombre_limpio.split())
    
    # 4. 🎯 LÓGICA DE PRECISIÓN: Sumar Laboratorio si no está en el nombre
    # Si el producto se llama "Loreal Delineador", y el lab es "L'Oreal", evitamos redundancia.
    # Pero si se llama "Aceite De Oregano 30 ml", y el lab es "Green Medical", lo sumamos.
    if laboratorio and laboratorio.lower() not in nombre_limpio.lower():
        query = f'{nombre_limpio} {laboratorio} precio chile'
    else:
        query = f'{nombre_limpio} precio chile'
    
    payload = json.dumps({"q": query, "gl": "cl", "hl": "es"})
    headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        if response.status_code == 200:
            data = response.json()
            precios_encontrados = []
            dominios_vistos = set()

            for organic in data.get("organic", []):
                link = organic.get("link", "")
                snippet = organic.get("snippet", "") + " " + organic.get("title", "")
                
                dominio_crudo = urlparse(link).netloc.replace('www.', '').lower()
                
                # 🛡️ FILTRO 1: Ignorar la Lista Negra
                if any(ignorado in dominio_crudo for ignorado in DOMINIOS_IGNORADOS):
                    continue
                
                if dominio_crudo in dominios_vistos:
                    continue
                    
                farmacia_nombre = FARMACIAS_CONOCIDAS.get(dominio_crudo, dominio_crudo.capitalize())
                
                matches = re.findall(r'\$\s?(\d{1,3}(?:\.\d{3})*)', snippet)
                
                precios_link = []
                for m in matches:
                    precio_int = int(m.replace('.', ''))
                    if 500 < precio_int < 200000: 
                        precios_link.append(precio_int)
                        
                if precios_link:
                    mejor_precio = min(precios_link)
                    precios_encontrados.append({
                        "farmacia": farmacia_nombre,
                        "precio": mejor_precio
                    })
                    dominios_vistos.add(dominio_crudo)

            if precios_encontrados:
                # Ordenamos de menor a mayor
                precios_encontrados = sorted(precios_encontrados, key=lambda x: x["precio"])
                todos_los_precios = [p["precio"] for p in precios_encontrados]
                
                # 🧠 INTELIGENCIA ESTADÍSTICA
                mediana_cruda = statistics.median(todos_los_precios)
                
                # Definimos los límites (Ignoramos < 50% de la mediana o > 200% de la mediana)
                LIMITE_INFERIOR = mediana_cruda * 0.5
                LIMITE_SUPERIOR = mediana_cruda * 2.0
                
                detalle_completo = []
                precios_validos = []
                
                for p in precios_encontrados:
                    if LIMITE_INFERIOR <= p["precio"] <= LIMITE_SUPERIOR:
                        precios_validos.append(p["precio"])
                        detalle_completo.append({"farmacia": p["farmacia"], "precio": p["precio"], "estado": "🟢 Válido"})
                    else:
                        motivo = "🔴 Descartado (Sachet/Gancho)" if p["precio"] < LIMITE_INFERIOR else "🔴 Descartado (Pack/Caro)"
                        detalle_completo.append({"farmacia": p["farmacia"], "precio": p["precio"], "estado": motivo})
                
                # Por si todos los precios se descartaron (muy raro, pero hay que prevenir)
                if not precios_validos:
                    precios_validos = todos_los_precios
                
                minimo_real = min(precios_validos)
                mediana_competitiva = int(statistics.median(precios_validos))
                
                return {
                    "detalle": detalle_completo,
                    "minimo": minimo_real,
                    "mediana_competitiva": mediana_competitiva
                }
    except Exception as e:
        pass
    
    return None

def main():
    if not SERPER_API_KEY:
        print("❌ Error: No se encontró SERPER_API_KEY en el .env")
        return

    print("🕵️‍♂️ Iniciando Espía de Precios (Modo: Full-Text + Laboratorio 🎯)...")
    
    if not os.path.exists(ARCHIVO_MEDIVEN):
        print("❌ Falta mediven_full.json.")
        return
        
    with open(ARCHIVO_MEDIVEN, "r", encoding="utf-8") as f:
        productos_mediven = json.load(f)
        
    precios_mercado = {}
    if os.path.exists(ARCHIVO_MERCADO):
        with open(ARCHIVO_MERCADO, "r", encoding="utf-8") as f:
            precios_mercado = json.load(f)

    pendientes = [p for p in productos_mediven if str(p.get("Codigo", "")) not in precios_mercado]
    print(f"📦 Total productos Mediven: {len(productos_mediven)}")
    print(f"✅ Ya espiados: {len(precios_mercado)}")
    print(f"⏳ Pendientes: {len(pendientes)}")

    if not pendientes:
        print("✨ Todos los precios de mercado ya están documentados.")
        return

    procesados_hoy = 0
    
    try:
        for p in pendientes:
            if LIMITE_PRUEBA > 0 and procesados_hoy >= LIMITE_PRUEBA:
                print(f"\n🛑 Límite de prueba ({LIMITE_PRUEBA}) alcanzado.")
                break
                
            sku = str(p.get("Codigo", ""))
            nombre = p.get("Descripcion", "")
            # 🎯 Extraemos el laboratorio del JSON de Mediven
            laboratorio = p.get("Laboratorio", "")
            
            print(f"\n🔍 [{procesados_hoy+1}] Espiando: {nombre[:50]} [{laboratorio[:15]}]...")
            
            datos_mercado = buscar_precio_competencia(nombre, laboratorio)
            
            if datos_mercado:
                for d in datos_mercado["detalle"]:
                    precio_fmt = f"${d['precio']:,}".replace(',', '.')
                    print(f"   {d['estado']} -> {d['farmacia']}: {precio_fmt}")
                
                minimo_fmt = f"${datos_mercado['minimo']:,}".replace(',', '.')
                mediana_fmt = f"${datos_mercado['mediana_competitiva']:,}".replace(',', '.')
                print(f"   🎯 DECISIÓN FINAL -> Mínimo Real: {minimo_fmt} | Mercado Justo: {mediana_fmt}")
                
                precios_mercado[sku] = {
                    "datos_mercado": datos_mercado,
                    "fecha": datetime.now().strftime("%Y-%m-%d")
                }
            else:
                print("   ❌ No encontrado en la competencia")
                precios_mercado[sku] = {
                    "datos_mercado": None,
                    "fecha": datetime.now().strftime("%Y-%m-%d")
                }
                
            procesados_hoy += 1
            
            if procesados_hoy % 5 == 0:
                with open(ARCHIVO_MERCADO, "w", encoding="utf-8") as f:
                    json.dump(precios_mercado, f, indent=2)

    except KeyboardInterrupt:
        print("\n⚠️ Proceso interrumpido por el usuario.")
    finally:
        os.makedirs(os.path.dirname(ARCHIVO_MERCADO), exist_ok=True)
        with open(ARCHIVO_MERCADO, "w", encoding="utf-8") as f:
            json.dump(precios_mercado, f, indent=2)
        print("💾 Progreso guardado en data/precios_mercado.json")

if __name__ == "__main__":
    main()
