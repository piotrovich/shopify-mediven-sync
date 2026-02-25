import os
import json
import random
import requests
import time
import io
import base64
from PIL import Image
from sync_diagnostico import shopify_graphql

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
# ORQUESTADOR DE REPESCA
# ==========================================
def ejecutar_repesca_imagenes(df_shop):
    if not SERPER_API_KEY:
        print("⚠️ SERPER_API_KEY no detectada. Saltando módulo de imágenes.")
        return

    registro = {}
    if os.path.exists(ARCHIVO_REGISTRO):
        with open(ARCHIVO_REGISTRO, "r", encoding="utf-8") as f:
            registro = json.load(f)

    productos = df_shop.drop_duplicates(subset=['product_id']).to_dict('records')
    nuevos, repesca = [], []
    
    for p in productos:
        sku = str(p.get("sku", "")).strip()
        if not sku or sku == "nan": continue
        
        estado = registro.get(sku)
        if not estado: nuevos.append(p)
        elif estado == DEFAULT_IMAGE_URL: repesca.append(p)
            
    random.shuffle(nuevos)
    random.shuffle(repesca)
    lote = nuevos[:15] + repesca[:5]
    
    if not lote:
        print("   🖼️ Todas las imágenes están sincronizadas. No hay repesca pendiente.")
        return
        
    print(f"   🖼️ Procesando {len(lote)} imágenes (Nuevos: {len(nuevos[:15])} | Repesca: {len(repesca[:5])})")
    
    for p in lote:
        sku, titulo = p["sku"], p["product_title"]
        product_gid = f"gid://shopify/Product/{p['product_id']}"
        
        print(f"      🔍 Buscando: {titulo[:35]}...", end=" ")
        url_encontrada = buscar_imagen_serper(titulo)
        usando_generica = not url_encontrada
        url_final = url_encontrada if url_encontrada else DEFAULT_IMAGE_URL
        
        if reemplazar_imagen_shopify(product_gid, url_final):
            registro[sku] = url_final
            print("⚠️ Genérica" if usando_generica else "✅ Subida (800x800)")
        else:
            print("❌ Error")
            
    with open(ARCHIVO_REGISTRO, "w", encoding="utf-8") as f:
        json.dump(registro, f, indent=2)
