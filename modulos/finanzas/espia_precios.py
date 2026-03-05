import os
import sys
import json
import time
import requests
import re
import statistics
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv(os.path.join(BASE_DIR, ".env"))

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
ARCHIVO_MEDIVEN = os.path.join(BASE_DIR, "mediven_full.json")
ARCHIVO_MERCADO = os.path.join(BASE_DIR, "data", "precios_mercado.json")

LIMITE_PRUEBA = 0

DOMINIOS_IGNORADOS = [
    "farmaciaslf.cl", "mercadolibre.cl", "falabella.com", 
    "paris.cl", "ripley.cl", "linio.cl", "aliexpress.com"
]

FARMACIAS_CONOCIDAS = {
    "farmex.cl": "Farmex", "salcobrand.cl": "Salcobrand", "cruzverde.cl": "Cruz Verde",
    "ahumada.cl": "Ahumada", "fraccion.cl": "Fracción", "ecofarmacias.cl": "EcoFarmacias",
    "farmaciaelquimico.cl": "El Químico", "drsimi.cl": "Dr. Simi", "cofar.cl": "Cofar",
    "profar.cl": "Profar", "pharol.cl": "Pharol", "redfarma.cl": "Redfarma",
    "maicao.cl": "Maicao", "farmaciasknop.com": "Knop", "galenica.cl": "Galénica",
    "lider.cl": "Lider", "super.lider.cl": "Lider", "jumbo.cl": "Jumbo", "preunic.cl": "Preunic"
}

def buscar_precio_competencia(nombre_producto, laboratorio=""):
    url = "https://google.serper.dev/search"
    
    nombre_limpio = nombre_producto.replace('+', ' ').replace('/', ' ')
    nombre_limpio = nombre_limpio.split("(")[0].strip()
    
    traducciones = {
        r'\bCOM\b': 'comprimidos', r'\bCAP\b': 'capsulas', r'\bJBE\b': 'jarabe',
        r'\bINY\b': 'inyectable', r'\bFCO\b': 'frasco', r'\bAMP\b': 'ampollas',
        r'\bCRE\b': 'crema', r'\bAER\b': 'aerosol', r'\bUDS\b': 'unidades',
        r'\bUND\b': 'unidades', r'\bSAB\b': 'sabor', r'\bPVO\b': 'polvo',
        r'\bSBR\b': 'sobres', r'\bLOC\b': 'locion', r'\bGTS\b': 'gotas',
        r'\bUNG\b': 'unguento', r'\bSUP\b': 'supositorios', r'\bSOL\b': 'solucion',
        r'\bSUSP\b': 'suspension', r'\bACO\b': 'acondicionador', r'\bSH\b': 'shampoo',         
        r'\bMATIF\b': 'matificante', r'\bSPY\b': 'spray', r'\bCOMP\b': 'comprimidos',
        r'\bSHA\b': 'shampoo', r'\bCEP\b': 'cepillo', r'\bDEN\b': 'dental',
        r'\bDENT\b': 'dental', r'\bTOA\b': 'toalla', r'\bUF\b': 'ultra fina',
        r'\bC/A\b': 'con alas', r'\bS/A\b': 'sin alas', r'\bJAB\b': 'jabon',
        r'\bOFT\b': 'oftalmica', r'\bPED\b': 'pediatrico', r'\bOSC\b': 'oscuro'           
    }
    for patron, palabra_real in traducciones.items():
        nombre_limpio = re.sub(patron, palabra_real, nombre_limpio, flags=re.IGNORECASE)
        
    basura_conectora = r'\b(X|x|PARA|EL|LA|LOS|LAS|DE|CON)\b'
    nombre_limpio = re.sub(basura_conectora, '', nombre_limpio, flags=re.IGNORECASE)
    nombre_limpio = " ".join(nombre_limpio.split())
    
    palabras = nombre_limpio.split()
    if len(palabras) > 6:
        nombre_limpio = " ".join(palabras[:6])
    
    lab_limpio = laboratorio.upper()
    holdings_basura = [
        "BEIERSDORF", "GSK", "PERFUMERIA", "DURANDIN", "PROCTER & GAMBL", 
        "PROCTER", "GAMBL", "LOREAL VICHY LA", "LOREAL", "VICHY", 
        "CMPC", "TISSUE", "CONSUMO", "OTC", "LASTRADE", "JOHNSON", "DENTAID"
    ]
    
    for holding in holdings_basura:
        lab_limpio = lab_limpio.replace(holding, "").strip()
    
    if lab_limpio and lab_limpio.lower() not in nombre_limpio.lower():
        query = f'{nombre_limpio} {lab_limpio} precio'
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
                        "farmacia": farmacia_nombre, "precio": mejor_precio
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
