#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import random
import requests
import time
import io
import base64
from PIL import Image
from modulos.nucleo.sync_diagnostico import shopify_graphql

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
DEFAULT_IMAGE_URL = os.getenv("SHOPIFY_DEFAULT_IMAGE_URL")
ARCHIVO_REGISTRO = "data/registro_imagenes.json"

SHOP_DOMAIN = os.getenv("SHOP_DOMAIN")
SHOPIFY_ADMIN_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-10")

# ==========================================
# BUSCADOR SERPER
# ==========================================
def buscar_imagen_serper(query):
    url = "https://google.serper.dev/images"
    busqueda_exacta = f"{query} medicamento caja farmacia chile -site:farmex.cl -farmex"
    payload = json.dumps({"q": busqueda_exacta, "num": 5})
    headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}
    try:
        r = requests.post(url, headers=headers, data=payload, timeout=10)
        if r.status_code == 200:
            datos = r.json()
            for img in datos.get("images", []):
                img_url = img.get("imageUrl", "")
                if "farmex" not in img_url.lower():
                    return img_url
    except:
        pass
    return None

# ==========================================
# PROCESAMIENTO DE IMAGEN (PILLOW 800x800)
# ==========================================
def descargar_y_estandarizar_imagen(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200: return None
            
        img = Image.open(io.BytesIO(r.content))
        
        if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
            img = img.convert("RGBA")
            fondo_temp = Image.new("RGBA", img.size, (255, 255, 255, 255))
            fondo_temp.paste(img, (0, 0), img)
            img = fondo_temp.convert("RGB")
        else:
            img = img.convert("RGB")
            
        TAMANO_LIENZO = 800
        img.thumbnail((TAMANO_LIENZO, TAMANO_LIENZO), Image.Resampling.LANCZOS)
        
        lienzo_final = Image.new("RGB", (TAMANO_LIENZO, TAMANO_LIENZO), (255, 255, 255))
        x = (TAMANO_LIENZO - img.width) // 2
        y = (TAMANO_LIENZO - img.height) // 2
        lienzo_final.paste(img, (x, y))
            
        buffer = io.BytesIO()
        lienzo_final.save(buffer, format="JPEG", quality=90)
        return base64.b64encode(buffer.getvalue()).decode('utf-8')
    except:
        return None

# ==========================================
# SUBIDA A SHOPIFY (CON BORRADO PREVIO)
# ==========================================
def reemplazar_imagen_shopify(product_gid, url_nueva):
    # 1. Borrar anteriores
    query = f"""
    query {{ product(id: "{product_gid}") {{ media(first: 10) {{ edges {{ node {{ id }} }} }} }} }}
    """
    res = shopify_graphql(query, contexto="get_media")
    media_ids = [edge["node"]["id"] for edge in res.get("data", {}).get("product", {}).get("media", {}).get("edges", [])] if res and "data" in res and res["data"].get("product") else []
            
    if media_ids:
        mut_del = """mutation productDeleteMedia($productId: ID!, $mediaIds: [ID!]!) { productDeleteMedia(productId: $productId, mediaIds: $mediaIds) { userErrors { message } } }"""
        shopify_graphql(mut_del, {"productId": product_gid, "mediaIds": media_ids}, contexto="del_media")
        time.sleep(0.5)
        
    # 2. Si la URL es la genérica, la subimos normal (sin Pillow para no gastar memoria)
    if url_nueva == DEFAULT_IMAGE_URL:
        mut_cre = """mutation productCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) { productCreateMedia(productId: $productId, media: $media) { userErrors { message } } }"""
        res_cre = shopify_graphql(mut_cre, {"productId": product_gid, "media": [{"originalSource": url_nueva, "mediaContentType": "IMAGE"}]}, contexto="create_media")
        return bool(res_cre and not res_cre.get("data", {}).get("productCreateMedia", {}).get("userErrors"))
        
    # 3. Si es de Google, la pasamos por Pillow y subimos por REST API
    imagen_base64 = descargar_y_estandarizar_imagen(url_nueva)
    if not imagen_base64: return False
        
    product_id_num = product_gid.split("/")[-1]
    url_rest = f"https://{SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/products/{product_id_num}/images.json"
    payload = {"image": {"attachment": imagen_base64, "filename": "producto_optimizado.jpg"}}
    headers_rest = {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"}
    
    r = requests.post(url_rest, json=payload, headers=headers_rest)
    return r.status_code in (200, 201)

# ==========================================
# ORQUESTADOR DE REPESCA (CON CUARENTENA IA)
# ==========================================
def ejecutar_repesca_imagenes(df_shop, skus_forzados=None): 
    if skus_forzados is None:
        skus_forzados = []
        
    if not SERPER_API_KEY:
        print("⚠️ SERPER_API_KEY no detectada. Saltando módulo de imágenes.")
        return

    registro = {}
    if os.path.exists(ARCHIVO_REGISTRO):
        with open(ARCHIVO_REGISTRO, "r", encoding="utf-8") as f:
            registro = json.load(f)

    productos = df_shop.drop_duplicates(subset=['product_id']).to_dict('records')
    
    # Contadores para el tablero
    nuevos = []
    repesca = []
    cuarentena_count = 0
    ok_count = 0
    
    for p in productos:
        sku = str(p.get("sku", "")).strip()
        if not sku or sku == "nan": continue
        
        # 🔥 SI EL USUARIO BORRÓ LA FOTO A MANO, LO FORZAMOS COMO NUEVO
        if sku in skus_forzados:
            nuevos.append(p)
            continue
            
        estado = registro.get(sku)
        
        if not estado: 
            nuevos.append(p)
        elif str(estado).startswith(str(DEFAULT_IMAGE_URL)): 
            # 🛡️ LÓGICA DE CUARENTENA (30 DÍAS)
            partes = str(estado).split("|")
            if len(partes) > 1:
                ultimo_intento = int(partes[1])
                dias_transcurridos = (time.time() - ultimo_intento) / 86400 # 86400 segundos = 1 día
                if dias_transcurridos >= 30:
                    repesca.append(p) # Ya pasaron 30 días, lo sacamos de cuarentena
                else:
                    cuarentena_count += 1
            else:
                # Formato viejo (sin fecha), a repesca para asignarle su fecha hoy
                repesca.append(p)
        else:
            ok_count += 1
            
    # --- 📊 TABLERO DE CONTROL DE IMÁGENES ---
    print("\n   📊 [RESUMEN DE IMÁGENES]")
    print(f"      ✅ Fotos OK: {ok_count}")
    print(f"      🛡️ En Cuarentena (30 días): {cuarentena_count}")
    print(f"      🆕 Faltan por analizar: {len(nuevos)}")
    print(f"      🔄 Listos para Repesca: {len(repesca)}")
    print("   ------------------------------------------------")

    random.shuffle(nuevos)
    random.shuffle(repesca)
    
    # 🛑 LIMITES DIARIOS SEGUROS PARA NO SATURAR (15 nuevos, 5 repesca)
    LIMITE_NUEVOS = 15
    LIMITE_REPESCA = 5

    lote = nuevos[:LIMITE_NUEVOS] + repesca[:LIMITE_REPESCA]
    
    if not lote:
        print("   🖼️ Nada que buscar hoy. Ahorrando créditos de Serper API 💸.")
        return
        
    print(f"   🖼️ Procesando {len(lote)} imágenes en esta pasada...")
    
    for p in lote:
        sku, titulo = p["sku"], p["product_title"]
        product_gid = f"gid://shopify/Product/{p['product_id']}"
        
        print(f"      🔍 Buscando: {titulo[:35]}...", end=" ")
        url_encontrada = buscar_imagen_serper(titulo)
        
        if url_encontrada and reemplazar_imagen_shopify(product_gid, url_encontrada):
            registro[sku] = url_encontrada
            print("✅ Subida (800x800)")
        else:
            if reemplazar_imagen_shopify(product_gid, DEFAULT_IMAGE_URL):
                # 🛑 ENVIAR A CUARENTENA: Guardamos la URL genérica unida a la marca de tiempo (Timestamp)
                registro[sku] = f"{DEFAULT_IMAGE_URL}|{int(time.time())}"
                print("🛡️ A Cuarentena (30 días)")
            else:
                print("❌ Error Fatal Shopify")
            
    with open(ARCHIVO_REGISTRO, "w", encoding="utf-8") as f:
        json.dump(registro, f, indent=2)


