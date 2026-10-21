#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Orquestador de imágenes automáticas (versión Claude).

Decide qué productos buscar en cada corrida, llama al motor de imágenes con
Claude (modulos/multimedia/motor_imagenes_claude.py), sube la foto a Shopify y
mantiene la memoria data/registro_imagenes.json:

  registro[sku] = "https://...jpg"                     → foto real encontrada
  registro[sku] = "<URL_GENERICA>|<timestamp>|<intentos>" → en cuarentena con foto genérica

La cuarentena crece con cada intento fallido (30, 60, 90 ... hasta 180 días)
para no gastar créditos buscando una y otra vez productos que no tienen foto
en internet. Las entradas antiguas "<URL_GENERICA>|<timestamp>" (sin intentos)
siguen siendo válidas y cuentan como 1 intento.

Variables de entorno:
  IMAGENES_LIMITE_NUEVOS     productos sin registro por corrida (default 15)
  IMAGENES_LIMITE_REPESCA    productos saliendo de cuarentena por corrida (default 5)
  IMAGENES_TIEMPO_MAX_SEG    presupuesto de tiempo del módulo por corrida (default 900)
"""

import os
import json
import random
import time
import base64
import requests

from modulos.nucleo.sync_diagnostico import shopify_graphql
from modulos.multimedia import motor_imagenes_claude as motor

DEFAULT_IMAGE_URL = (os.getenv("SHOPIFY_DEFAULT_IMAGE_URL") or "").strip()
ARCHIVO_REGISTRO = "data/registro_imagenes.json"

SHOP_DOMAIN = os.getenv("SHOP_DOMAIN")
SHOPIFY_ADMIN_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-10")

LIMITE_NUEVOS = int(os.getenv("IMAGENES_LIMITE_NUEVOS", "15"))
LIMITE_REPESCA = int(os.getenv("IMAGENES_LIMITE_REPESCA", "5"))
TIEMPO_MAX_SEG = int(os.getenv("IMAGENES_TIEMPO_MAX_SEG", "900"))

DIAS_CUARENTENA_BASE = 30
DIAS_CUARENTENA_MAX = 180
MAX_ERRORES_SEGUIDOS = 3


# ==========================================
# MEMORIA (registro_imagenes.json)
# ==========================================
def cargar_registro(ruta=None):
    ruta = ruta or ARCHIVO_REGISTRO
    if not os.path.exists(ruta):
        return {}
    try:
        with open(ruta, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        print("   ⚠️ registro_imagenes.json ilegible; se parte con memoria vacía.")
        return {}


def guardar_registro(registro, ruta=None):
    ruta = ruta or ARCHIVO_REGISTRO
    os.makedirs(os.path.dirname(ruta) or ".", exist_ok=True)
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(registro, f, indent=2)


def parsear_estado(estado):
    """
    Interpreta una entrada del registro.
    Devuelve (tipo, timestamp, intentos) con tipo en {"ok", "cuarentena"}.
    """
    texto = str(estado or "")
    if "|" in texto:
        partes = texto.split("|")
        try:
            ts = int(partes[1])
        except (ValueError, IndexError):
            ts = 0
        try:
            intentos = max(1, int(partes[2]))
        except (ValueError, IndexError):
            intentos = 1
        return "cuarentena", ts, intentos
    if DEFAULT_IMAGE_URL and texto == DEFAULT_IMAGE_URL:
        # Formato antiguo sin fecha: sale a repesca de inmediato.
        return "cuarentena", 0, 1
    return "ok", None, 0


def dias_cuarentena(intentos):
    return min(DIAS_CUARENTENA_BASE * max(1, intentos), DIAS_CUARENTENA_MAX)


def marcar_cuarentena(registro, sku, intentos_previos):
    intentos = intentos_previos + 1
    registro[sku] = f"{DEFAULT_IMAGE_URL}|{int(time.time())}|{intentos}"
    return intentos


# ==========================================
# SUBIDA A SHOPIFY (CON BORRADO PREVIO)
# ==========================================
def _borrar_media_actual(product_gid):
    query = f"""
    query {{ product(id: "{product_gid}") {{ media(first: 10) {{ edges {{ node {{ id }} }} }} }} }}
    """
    res = shopify_graphql(query, contexto="get_media")
    media_ids = []
    if res and res.get("data", {}).get("product"):
        media_ids = [e["node"]["id"] for e in res["data"]["product"].get("media", {}).get("edges", [])]
    if media_ids:
        mut_del = """mutation productDeleteMedia($productId: ID!, $mediaIds: [ID!]!) { productDeleteMedia(productId: $productId, mediaIds: $mediaIds) { userErrors { message } } }"""
        shopify_graphql(mut_del, {"productId": product_gid, "mediaIds": media_ids}, contexto="del_media")
        time.sleep(0.5)


def subir_imagen_generica(product_gid):
    """Asigna la imagen genérica (URL pública en Shopify Files) vía GraphQL."""
    if not DEFAULT_IMAGE_URL:
        return False
    _borrar_media_actual(product_gid)
    mut_cre = """mutation productCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) { productCreateMedia(productId: $productId, media: $media) { userErrors { message } } }"""
    res = shopify_graphql(
        mut_cre,
        {"productId": product_gid, "media": [{"originalSource": DEFAULT_IMAGE_URL, "mediaContentType": "IMAGE"}]},
        contexto="create_media",
    )
    return bool(res and not res.get("data", {}).get("productCreateMedia", {}).get("userErrors"))


def subir_imagen_jpeg(product_gid, jpeg_bytes, sku="", alt=""):
    """Sube la foto normalizada (bytes JPEG) vía REST Admin API con attachment base64."""
    _borrar_media_actual(product_gid)
    product_id_num = product_gid.split("/")[-1]
    url_rest = f"https://{SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/products/{product_id_num}/images.json"
    payload = {"image": {
        "attachment": base64.b64encode(jpeg_bytes).decode("utf-8"),
        "filename": f"producto_{sku or product_id_num}.jpg",
        "alt": (alt or "")[:512],
    }}
    headers = {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"}
    try:
        r = requests.post(url_rest, json=payload, headers=headers, timeout=60)
    except requests.RequestException as e:
        print(f"❌ Error de red subiendo a Shopify: {e}")
        return False
    if r.status_code in (200, 201):
        return True
    print(f"❌ Shopify rechazó la imagen (HTTP {r.status_code}): {r.text[:200]}")
    return False


def reemplazar_imagen_shopify(product_gid, url_nueva=None, jpeg_bytes=None, sku="", alt=""):
    """
    Compatibilidad con el flujo antiguo: si vienen bytes, sube la foto real;
    si la URL es la genérica, la asigna por GraphQL; si es otra URL, la descarga,
    normaliza y sube.
    """
    if jpeg_bytes:
        return subir_imagen_jpeg(product_gid, jpeg_bytes, sku=sku, alt=alt)
    if url_nueva and url_nueva == DEFAULT_IMAGE_URL:
        return subir_imagen_generica(product_gid)
    if url_nueva:
        jpeg = motor.descargar_y_estandarizar_imagen(url_nueva)
        return bool(jpeg) and subir_imagen_jpeg(product_gid, jpeg, sku=sku, alt=alt)
    return False


# ==========================================
# PROCESAR UN PRODUCTO
# ==========================================
def procesar_producto(producto, registro, info_extra=None):
    """
    Busca y sube la foto de un producto. Devuelve uno de:
      "ok"          foto real subida y registrada
      "cuarentena"  no hubo foto aprobada: genérica asignada y cuarentena registrada
      "error"       fallo transitorio (API/Shopify): el registro NO se toca
    Propaga motor.MotorNoDisponible para que el orquestador se detenga.
    """
    sku = str(producto.get("sku", "")).strip()
    titulo = str(producto.get("product_title", "")).strip()
    product_gid = f"gid://shopify/Product/{producto['product_id']}"
    extra = (info_extra or {}).get(sku, {}) if info_extra else {}

    _, _, intentos_previos = parsear_estado(registro.get(sku))

    print(f"      🔍 {titulo[:45]:45} ", end="", flush=True)
    try:
        resultado = motor.encontrar_imagen_producto(
            titulo,
            laboratorio=extra.get("laboratorio", ""),
            descripcion_original=extra.get("descripcion_original", ""),
            log=lambda m: print(f"\n         · {m}", end="", flush=True),
        )
    except motor.ErrorTransitorio as e:
        print(f"\n         ⚠️ Error transitorio, se reintentará en la próxima corrida: {e}")
        return "error"

    if resultado:
        if subir_imagen_jpeg(product_gid, resultado["jpeg"], sku=sku, alt=titulo):
            registro[sku] = resultado["url_imagen"]
            print(f"\n         ✅ Subida 800x800 desde {resultado['tienda'] or resultado['url_imagen'][:50]}")
            return "ok"
        print("\n         ❌ No se pudo subir a Shopify; se reintentará en la próxima corrida.")
        return "error"

    if subir_imagen_generica(product_gid):
        intentos = marcar_cuarentena(registro, sku, intentos_previos)
        print(f"\n         🛡️ Sin foto confiable → genérica y cuarentena {dias_cuarentena(intentos)} días (intento {intentos})")
        return "cuarentena"

    print("\n         ❌ Error Fatal Shopify al asignar la genérica.")
    return "error"


# ==========================================
# ORQUESTADOR DE REPESCA (CON CUARENTENA PROGRESIVA)
# ==========================================
def ejecutar_repesca_imagenes(df_shop, skus_forzados=None, info_extra=None):
    """
    df_shop:       DataFrame normalizado de Shopify (product_id, product_title, sku...).
    skus_forzados: SKUs activos que hoy no tienen foto en Shopify (se tratan como nuevos).
    info_extra:    {sku: {"laboratorio": str, "descripcion_original": str}} para afinar la búsqueda.
    Devuelve un dict con los contadores de la corrida.
    """
    skus_forzados = set(skus_forzados or [])
    resumen = {"ok": 0, "cuarentena": 0, "error": 0, "procesados": 0, "detenido": None}

    if not motor.motor_disponible():
        print("⚠️ ANTHROPIC_API_KEY no detectada. Saltando módulo de imágenes.")
        resumen["detenido"] = "sin_api_key"
        return resumen
    if not DEFAULT_IMAGE_URL:
        print("⚠️ SHOPIFY_DEFAULT_IMAGE_URL no definida. Saltando módulo de imágenes.")
        resumen["detenido"] = "sin_imagen_generica"
        return resumen

    registro = cargar_registro()
    productos = df_shop.drop_duplicates(subset=["product_id"]).to_dict("records")

    nuevos, repesca = [], []
    cuarentena_count = ok_count = 0
    ahora = time.time()

    for p in productos:
        sku = str(p.get("sku", "")).strip()
        if not sku or sku == "nan":
            continue
        if sku in skus_forzados:
            nuevos.append(p)  # el usuario borró la foto a mano o nunca se subió
            continue
        estado = registro.get(sku)
        if not estado:
            nuevos.append(p)
            continue
        tipo, ts, intentos = parsear_estado(estado)
        if tipo == "ok":
            ok_count += 1
        elif (ahora - ts) / 86400 >= dias_cuarentena(intentos):
            repesca.append(p)
        else:
            cuarentena_count += 1

    print("\n   📊 [RESUMEN DE IMÁGENES]")
    print(f"      ✅ Fotos OK: {ok_count}")
    print(f"      🛡️ En Cuarentena: {cuarentena_count}")
    print(f"      🆕 Faltan por analizar: {len(nuevos)}")
    print(f"      🔄 Listos para Repesca: {len(repesca)}")
    print(f"      🤖 Motor: Claude ({motor.MODELO}) · visión {'ON' if motor.VERIFICAR_VISION else 'OFF'}")
    print("   ------------------------------------------------")

    random.shuffle(nuevos)
    random.shuffle(repesca)
    lote = nuevos[:LIMITE_NUEVOS] + repesca[:LIMITE_REPESCA]

    if not lote:
        print("   🖼️ Nada que buscar hoy. Ahorrando créditos de Claude 💸.")
        return resumen

    print(f"   🖼️ Procesando {len(lote)} imágenes en esta pasada (tope {TIEMPO_MAX_SEG}s)...")
    inicio = time.time()
    errores_seguidos = 0

    for p in lote:
        if time.time() - inicio > TIEMPO_MAX_SEG:
            print(f"   ⏱️ Presupuesto de tiempo agotado; el resto queda para la próxima corrida.")
            resumen["detenido"] = "tiempo"
            break
        try:
            estado = procesar_producto(p, registro, info_extra)
        except motor.MotorNoDisponible as e:
            print(f"\n   🛑 Motor de imágenes no disponible: {e}")
            print("      Se detiene el módulo sin castigar a los productos pendientes.")
            resumen["detenido"] = "motor_no_disponible"
            break

        resumen["procesados"] += 1
        resumen[estado] += 1
        if estado == "error":
            errores_seguidos += 1
            if errores_seguidos >= MAX_ERRORES_SEGUIDOS:
                print(f"\n   🛑 {MAX_ERRORES_SEGUIDOS} errores seguidos; se detiene el módulo por esta corrida.")
                resumen["detenido"] = "errores_seguidos"
                break
        else:
            errores_seguidos = 0
            guardar_registro(registro)  # persistir de inmediato: la corrida puede ser cancelada

    guardar_registro(registro)
    print(f"\n   📈 Resultado: {resumen['ok']} fotos reales · {resumen['cuarentena']} a cuarentena · {resumen['error']} errores")
    print(f"   💳 Consumo Claude: {motor.resumen_uso()}")
    return resumen
