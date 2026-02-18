#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import time
from dotenv import load_dotenv

from google import genai
from google.genai import types
from google.genai.errors import APIError

# ==========================================
# CONFIGURACIÓN E INICIALIZACIÓN
# ==========================================
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    print("❌ Falta GEMINI_API_KEY en el archivo .env")
    exit(1)

client = genai.Client(api_key=GEMINI_API_KEY)

ARCHIVO_ENTRADA = "mediven_full.json"
ARCHIVO_DICCIONARIO = "data/diccionario_ia.json"

# MODO PRUEBA: Cambia este número a None o 0 cuando quieras procesar TODO el archivo.
LIMITE_PRUEBA = None 

# ==========================================
# FUNCIÓN DE LLAMADA A LA IA CON ESQUEMA ESTRICTO
# ==========================================
def generar_explicacion_ia(datos_producto, reintentos_max=3):
    prompt = f"""
    Eres un experto redactor médico y de e-commerce para la "Farmacias LF" en Chile.
    A continuación tienes los datos de un producto:
    
    - Nombre Original: {datos_producto.get('Descripcion')}
    - Laboratorio: {datos_producto.get('Laboratorio')}
    - Acción Terapéutica: {datos_producto.get('AccionTerapeutica')}
    - Equivalente (Categoría/Principio): {datos_producto.get('Equivalente')}

    Tu tarea es generar la información para la página del producto.
    
    1. "titulo_normalizado": Limpia el nombre original. Expande abreviaturas (ej. COM=Comprimidos, UND=Unidades, JBE=Jarabe, DM=Dispositivo Médico). Mantenlo profesional.
    2. "descripcion_amable": Tono cercano, persuasivo, ágil (2 párrafos). Explica sus beneficios y para qué sirve. USO OBLIGATORIO de emojis acordes al producto. No uses tecnicismos duros aquí.
    3. "ficha_tecnica": Tono médico formal, 100% clínico y serio. SIN emojis. Usa formato HTML (listas <ul> y <li>) para detallar composición, uso, presentación y laboratorio.
    
    Reglas Críticas:
    - Si "Acción Terapéutica" o "Equivalente" están vacíos, deduce el uso basándote en el Nombre y Laboratorio.
    - COHERENCIA DE PÚBLICO: Analiza bien el producto y laboratorio. Si es de uso VETERINARIO, la descripción amable DEBE estar dirigida a dueños de mascotas (perros/gatos). JAMÁS lo recomiendes a humanos.
    - JAMÁS inventes indicaciones médicas que no correspondan a la naturaleza real del producto.
    """

    for intento in range(reintentos_max):
        try:
            response = client.models.generate_content(
                model='gemini-2.0-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema={
                        "type": "OBJECT",
                        "properties": {
                            "titulo_normalizado": {"type": "STRING"},
                            "descripcion_amable": {"type": "STRING"},
                            "ficha_tecnica": {"type": "STRING"}
                        },
                        "required": ["titulo_normalizado", "descripcion_amable", "ficha_tecnica"]
                    },
                    temperature=0.3,
                )
            )
            
            # Limpieza por si Gemini intenta poner markdown (```json ... ```)
            texto = response.text.strip()
            if texto.startswith("```json"):
                texto = texto.replace("```json", "").replace("```", "").strip()
            elif texto.startswith("```"):
                texto = texto.replace("```", "").strip()

            resultado = json.loads(texto)
            return resultado
            
        except APIError as e:
            error_texto = str(e).lower()
            if e.code == 429:
                if "perday" in error_texto or "quota" in error_texto:
                    print(f"\n❌ LÍMITE DIARIO AGOTADO EN ESTA API KEY.")
                    return "LIMITE_DIARIO"
                else:
                    tiempo_espera = 35 
                    print(f"\n   ⏳ Límite de velocidad. Descansando {tiempo_espera}s (Intento {intento+1}/{reintentos_max})...", end="", flush=True)
                    time.sleep(tiempo_espera)
            else:
                print(f"\n❌ Error API de Gemini: {e}")
                return None
        except json.JSONDecodeError as e:
            print(f"\n❌ Error formateando JSON: {e}")
            return None
        except Exception as e:
            print(f"\n❌ Error inesperado: {e}")
            return None
            
    return None

# ==========================================
# FUNCIÓN PRINCIPAL
# ==========================================
def main():
    print("==================================================")
    print("🧠 INICIANDO MOTOR DE CONTENIDO IA (VERSIÓN PRODUCTOS) 🧠")
    print("==================================================\n")

    os.makedirs("data", exist_ok=True)

    if not os.path.exists(ARCHIVO_ENTRADA):
        print(f"❌ No se encontró {ARCHIVO_ENTRADA}. Ejecuta sync.py primero.")
        return

    with open(ARCHIVO_ENTRADA, "r", encoding="utf-8") as f:
        productos = json.load(f)

    # Cargar el diccionario existente con Red de Seguridad anti-archivos vacíos
    diccionario = {}
    if os.path.exists(ARCHIVO_DICCIONARIO):
        try:
            with open(ARCHIVO_DICCIONARIO, "r", encoding="utf-8") as f:
                diccionario = json.load(f)
            print(f"📚 Memoria IA cargada: {len(diccionario)} productos ya documentados.")
        except json.JSONDecodeError:
            print("⚠️ El archivo diccionario_ia.json estaba vacío o corrupto. Se iniciará desde cero.")
            diccionario = {}

    # Filtramos los productos que aún no están en el diccionario (usando el 'Codigo' como llave)
    faltantes = []
    for p in productos:
        codigo = str(p.get("Codigo", "")).strip()
        if codigo and codigo not in diccionario:
            faltantes.append(p)

    print(f"🚀 Productos NUEVOS por procesar: {len(faltantes)}\n")

    if not faltantes:
        print("🎉 ¡Tu diccionario está 100% actualizado! No hay nada nuevo que generar hoy.")
        return

    # Aplicar Límite de Prueba
    if LIMITE_PRUEBA and LIMITE_PRUEBA > 0:
        faltantes = faltantes[:LIMITE_PRUEBA]
        print(f"⚠️ MODO PRUEBA ACTIVADO: Solo se procesarán los primeros {LIMITE_PRUEBA} productos.\n")

    nuevos_generados = 0

    for idx, producto in enumerate(faltantes, 1):
        codigo = str(producto.get("Codigo", "")).strip()
        nombre_original = producto.get("Descripcion", "")
        nombre_corto = nombre_original[:40] + "..." if len(nombre_original) > 40 else nombre_original
        
        print(f"[{idx}/{len(faltantes)}] 🤖 Generando: {nombre_corto}", end=" ", flush=True)

        resultado = generar_explicacion_ia(producto)

        if resultado == "LIMITE_DIARIO":
            print("\n🛑 Proceso detenido por límite.")
            break
            
        if isinstance(resultado, dict):
            # Convertimos todas las llaves a minúsculas por si acaso
            res_lower = {k.lower(): v for k, v in resultado.items()}
            
            if "titulo_normalizado" in res_lower and "descripcion_amable" in res_lower and "ficha_tecnica" in res_lower:
                diccionario[codigo] = {
                    "titulo_normalizado": res_lower["titulo_normalizado"],
                    "descripcion_amable": res_lower["descripcion_amable"],
                    "ficha_tecnica": res_lower["ficha_tecnica"]
                }
                nuevos_generados += 1
                print("✅ OK")

                # Guardado en cada iteración para no perder datos si se corta
                with open(ARCHIVO_DICCIONARIO, "w", encoding="utf-8") as f:
                    json.dump(diccionario, f, ensure_ascii=False, indent=2)
            else:
                print(f"❌ Formato inválido devuelto por la IA. Llaves: {list(res_lower.keys())}")
        else:
            print(f"❌ Respuesta no válida.")

        # Pausa para no saturar el servidor de Gemini
        time.sleep(3) 

    print("\n==================================================")
    print(f"✨ Proceso terminado. Se agregaron {nuevos_generados} nuevos productos.")
    print(f"📁 Archivo actualizado: {ARCHIVO_DICCIONARIO}")
    print("==================================================")

if __name__ == "__main__":
    main()
