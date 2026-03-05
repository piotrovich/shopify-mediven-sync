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
    
    # 1. Separar los componentes pegados con "+" o "/" (Google prefiere espacios)
    nombre_limpio = nombre_producto.replace('+', ' ').replace('/', ' ')
    
    # 2. Quitamos lo que está entre paréntesis (suelen ser categorías genéricas)
    nombre_limpio = nombre_limpio.split("(")[0].strip()
    
    # 3. 🧠 TRADUCTOR CLÍNICO MÁXIMO -> COMERCIAL
    traducciones = {
        r'\bCOM\b': 'comprimidos',
        r'\bCOMP\b': 'comprimidos',
        r'\bCAP\b': 'capsulas',
        r'\bJBE\b': 'jarabe',
        r'\bINY\b': 'inyectable',
        r'\bFCO\b': 'frasco',
        r'\bAMP\b': 'ampollas',
        r'\bCRE\b': 'crema',
        r'\bAER\b': 'aerosol',
        r'\bUDS\b': 'unidades',
        r'\bUND\b': 'unidades',
        r'\bSAB\b': 'sabor',
        r'\bPVO\b': 'polvo',
        r'\bSBR\b': 'sobres',
        r'\bLOC\b': 'locion',
        r'\bGTS\b': 'gotas',
        r'\bUNG\b': 'unguento',
        r'\bSUP\b': 'supositorios',
        r'\bSOL\b': 'solucion',
        r'\bSUSP\b': 'suspension',
        r'\bACO\b': 'acondicionador',  
        r'\bSH\b': 'shampoo',          
        r'\bSHA\b': 'shampoo',
        r'\bMATIF\b': 'matificante',   
        r'\bSPY\b': 'spray',
        r'\bCEP\b': 'cepillo',
        r'\bDEN\b': 'dental',
        r'\bDENT\b': 'dental',
        r'\bTOA\b': 'toalla',
        r'\bUF\b': 'ultra fina',
        r'\bC/A\b': 'con alas',
        r'\bS/A\b': 'sin alas',
        r'\bJAB\b': 'jabon',
        r'\bOFT\b': 'oftalmica',
        r'\bPED\b': 'pediatrico',
        r'\bOSC\b': 'oscuro'
    }
    for patron, palabra_real in traducciones.items():
        nombre_limpio = re.sub(patron, palabra_real, nombre_limpio, flags=re.IGNORECASE)
        
    # 4. Borramos la "X" aislada y las palabras "PARA EL" o "DE" que alargan mucho
    basura_conectora = r'\b(X|x|PARA|EL|LA|LOS|LAS|DE|CON)\b'
    nombre_limpio = re.sub(basura_conectora, '', nombre_limpio, flags=re.IGNORECASE)
    
    # 5. Limpiamos espacios dobles sobrantes
    nombre_limpio = " ".join(nombre_limpio.split())
    
    # 6. 🔪 CORTA-NOMBRES INTELIGENTE: Si el nombre es absurdamente largo (> 6 palabras), 
    # nos quedamos solo con las primeras 6 para no marear a Google.
    palabras = nombre_limpio.split()
    if len(palabras) > 6:
        nombre_limpio = " ".join(palabras[:6])
    
    # 7. 🎯 LÓGICA DE PRECISIÓN
    if laboratorio and laboratorio.lower() not in nombre_limpio.lower():
        query = f'{nombre_limpio} {laboratorio} precio'
    else:
        query = f'{nombre_limpio} precio'
    
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
                precios_encontrados = sorted(precios_encontrados, key=lambda x: x["precio"])
                todos_los_precios = [p["precio"] for p in precios_encontrados]
                
                mediana_cruda = statistics.median(todos_los_precios)
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
