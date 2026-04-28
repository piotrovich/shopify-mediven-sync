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

# Timeout explícito de 60s para no colgar el servidor si Google no responde.
client = genai.Client(
    api_key=GEMINI_API_KEY,
    http_options=types.HttpOptions(timeout=60000)
)

ARCHIVO_ENTRADA = "mediven_full.json"
ARCHIVO_DICCIONARIO = "data/diccionario_ia.json"

LIMITE_PRUEBA = None

# 🔧 FIX 3: Lista centralizada de patrones que indican LÍMITE DIARIO en los
# mensajes de error 429 de Gemini. Antes solo se buscaba "perday"/"quota",
# pero Google ha cambiado los textos y ahora los errores diarios pueden
# llegar como "exhausted", "RequestsPerDay", "GenerateRequestsPerDay", etc.
INDICADORES_LIMITE_DIARIO = [
    "perday",
    "per day",
    "perdayperproject",
    "perdaypermodel",
    "requestsperday",
    "generaterequestsperday",
    "quota",
    "exhausted",
    "exceeded",
    "rpd",
]

# ==========================================
# FUNCIÓN DE LLAMADA A LA IA CON ESQUEMA ESTRICTO
# ==========================================
def generar_explicacion_ia(datos_producto, reintentos_max=3):
    # Aquí va tu Súper Prompt exacto
    prompt = f"""
    Eres un experto Químico Farmacéutico y redactor de e-commerce para "Farmacias LF" en Chile.
    A continuación tienes los datos de un producto:
    - Nombre Original: {datos_producto.get('Descripcion')}
    - Laboratorio: {datos_producto.get('Laboratorio')}
    - Acción Terapéutica: {datos_producto.get('AccionTerapeutica')}
    - Equivalente (Categoría/Principio): {datos_producto.get('Equivalente')}

    Tu tarea es generar un JSON con la información estructurada para la web.

    1. "titulo_normalizado": Limpia el nombre original. Expande abreviaturas (ej. COM=Comprimidos, UND=Unidades, JBE=Jarabe, DM=Dispositivo Médico). Mantenlo profesional e impecable.

    2. "descripcion_amable": Tono cercano, persuasivo, empático y ágil (máximo 3 a 4 líneas). Explica sus beneficios y para qué sirve de forma fácil de entender para el paciente. USO OBLIGATORIO de 1 o 2 emojis acordes al producto. No uses tecnicismos duros aquí.

    3. "ficha_tecnica": Tono médico formal, 100% clínico y serio (como un prospecto médico). SIN emojis.
    Debe estar estructurado ESTRICTAMENTE en estas 4 partes, separadas por saltos de línea (\\n):
    - [Párrafo de introducción]: Qué es el producto, su principio activo y mecanismo de acción básico.
    - Para qué sirve: (Usa viñetas con el símbolo •) Explica las indicaciones principales.
    - Consideraciones importantes: (Usa viñetas con el símbolo •) Advertencias, dosis, contraindicaciones o interacciones médicas.
    - En resumen: Una oración final de conclusión médica.

    IMPORTANTE PARA FICHA TÉCNICA: NO uses HTML (ni <ul> ni <li>) y NO uses formatos Markdown como asteriscos (**) o hashtags (#). Usa estrictamente texto plano y el símbolo de viñeta (•).

    Reglas Críticas:
    - Si "Acción Terapéutica" o "Equivalente" están vacíos, deduce el uso basándote en el Nombre y Laboratorio.
    - COHERENCIA DE PÚBLICO: Analiza bien el producto y laboratorio. Si es de uso VETERINARIO, la descripción amable DEBE estar dirigida a dueños de mascotas. JAMÁS lo recomiendes a humanos.
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
                # 🔧 FIX 3: Detección mejorada de límite diario.
                # Si CUALQUIER patrón conocido de cuota diaria aparece en el
                # error, cortamos sin gastar más reintentos.
                if any(ind in error_texto for ind in INDICADORES_LIMITE_DIARIO):
                    print(f"\n❌ LÍMITE DIARIO AGOTADO EN ESTA API KEY.")
                    print(f"   (Detectado en error: {error_texto[:200]})")
                    return "LIMITE_DIARIO"
                else:
                    # 🔧 FIX 1: La ventana RPM de Gemini se renueva cada 60s.
                    # Antes esperábamos 35s -> caíamos en la MISMA ventana
                    # saturada y los 3 reintentos fallaban en cadena.
                    # Con 65s garantizamos que la ventana se haya reseteado.
                    tiempo_espera = 65
                    print(f"   ⏳ Límite de velocidad. Descansando {tiempo_espera}s (Intento {intento+1}/{reintentos_max})...", flush=True)
                    time.sleep(tiempo_espera)
            else:
                print(f"   ⚠️ Error API de Gemini (Intento {intento+1}): {e}")
                time.sleep(5)

        except json.JSONDecodeError as e:
            print(f"   ❌ Error formateando JSON: {e}")
            return None

        except Exception as e:
            # Atrapamos TimeoutError y desconexiones de red.
            print(f"   ⚠️ Error de red/timeout (Intento {intento+1}): Se cortó la conexión. Reintentando...")
            time.sleep(10)

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

    diccionario = {}
    if os.path.exists(ARCHIVO_DICCIONARIO):
        try:
            with open(ARCHIVO_DICCIONARIO, "r", encoding="utf-8") as f:
                diccionario = json.load(f)
            print(f"📚 Memoria IA cargada: {len(diccionario)} productos ya documentados.")
        except json.JSONDecodeError:
            print("⚠️ El archivo diccionario_ia.json estaba vacío o corrupto. Se iniciará desde cero.")
            diccionario = {}

    faltantes = []
    for p in productos:
        codigo = str(p.get("Codigo", "")).strip()
        if codigo and codigo not in diccionario:
            faltantes.append(p)

    print(f"🚀 Productos NUEVOS por procesar: {len(faltantes)}\n")

    if not faltantes:
        print("🎉 ¡Tu diccionario está 100% actualizado! No hay nada nuevo que generar hoy.")
        return

    if LIMITE_PRUEBA and LIMITE_PRUEBA > 0:
        faltantes = faltantes[:LIMITE_PRUEBA]
        print(f"⚠️ MODO PRUEBA ACTIVADO: Solo se procesarán los primeros {LIMITE_PRUEBA} productos.\n")

    nuevos_generados = 0

    for idx, producto in enumerate(faltantes, 1):
        codigo = str(producto.get("Codigo", "")).strip()
        nombre_original = producto.get("Descripcion", "")
        nombre_corto = nombre_original[:40] + "..." if len(nombre_original) > 40 else nombre_original

        print(f"[{idx}/{len(faltantes)}] 🤖 Generando: {nombre_corto} ...", flush=True)

        resultado = generar_explicacion_ia(producto)

        if resultado == "LIMITE_DIARIO":
            print("🛑 Proceso detenido por límite.")
            break

        if isinstance(resultado, dict):
            res_lower = {k.lower(): v for k, v in resultado.items()}

            if "titulo_normalizado" in res_lower and "descripcion_amable" in res_lower and "ficha_tecnica" in res_lower:
                diccionario[codigo] = {
                    "titulo_normalizado": res_lower["titulo_normalizado"],
                    "descripcion_amable": res_lower["descripcion_amable"],
                    "ficha_tecnica": res_lower["ficha_tecnica"]
                }
                nuevos_generados += 1
                print(f"   ✅ OK - Guardado.")

                with open(ARCHIVO_DICCIONARIO, "w", encoding="utf-8") as f:
                    json.dump(diccionario, f, ensure_ascii=False, indent=2)
            else:
                print(f"   ❌ Formato inválido devuelto por la IA. Llaves: {list(res_lower.keys())}")
        else:
            print(f"   ❌ Respuesta no válida.")

        # 🔧 FIX 2: Espacio mínimo entre llamadas exitosas.
        # Free tier de gemini-2.0-flash = ~15 RPM (1 request cada ~4s).
        # Antes esperaba 3s -> se saturaba el RPM al 5to-6to producto.
        # Con 5s nos mantenemos cómodamente bajo el límite.
        time.sleep(5)

    print("\n==================================================")
    print(f"✨ Proceso terminado. Se agregaron {nuevos_generados} nuevos productos.")
    print(f"📁 Archivo actualizado: {ARCHIVO_DICCIONARIO}")
    print("==================================================")


if __name__ == "__main__":
    main()
