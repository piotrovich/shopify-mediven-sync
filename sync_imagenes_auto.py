import os
import json
import random
import requests
import time
from sync_diagnostico import shopify_graphql

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
DEFAULT_IMAGE_URL = os.getenv("SHOPIFY_DEFAULT_IMAGE_URL")
ARCHIVO_REGISTRO = "data/registro_imagenes.json"

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

def reemplazar_imagen_shopify(product_gid, url_nueva):
    # 1. Obtener el ID de la imagen actual en Shopify
    query = f"""
    query {{
      product(id: "{product_gid}") {{
        media(first: 10) {{ edges {{ node {{ id }} }} }}
      }}
    }}
    """
    res = shopify_graphql(query, contexto="get_media")
    media_ids = []
    if res and "data" in res and res["data"]["product"]:
        for edge in res["data"]["product"]["media"]["edges"]:
            media_ids.append(edge["node"]["id"])
            
    # 2. Borrar imagen anterior (La genérica)
    if media_ids:
        mut_del = """
        mutation productDeleteMedia($productId: ID!, $mediaIds: [ID!]!) {
          productDeleteMedia(productId: $productId, mediaIds: $mediaIds) {
            userErrors { message }
          }
        }
        """
        shopify_graphql(mut_del, {"productId": product_gid, "mediaIds": media_ids}, contexto="del_media")
        time.sleep(1) # Pausa segura
        
    # 3. Crear nueva imagen
    mut_cre = """
    mutation productCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) {
      productCreateMedia(productId: $productId, media: $media) {
        userErrors { message }
      }
    }
    """
    vars_cre = {
        "productId": product_gid,
        "media": [{"originalSource": url_nueva, "mediaContentType": "IMAGE"}]
    }
    res_cre = shopify_graphql(mut_cre, vars_cre, contexto="create_media")
    
    if res_cre and not res_cre.get("data", {}).get("productCreateMedia", {}).get("userErrors"):
        return True
    return False

def ejecutar_repesca_imagenes(df_shop):
    if not SERPER_API_KEY:
        print("⚠️ SERPER_API_KEY no detectada. Saltando módulo de imágenes.")
        return

    registro = {}
    if os.path.exists(ARCHIVO_REGISTRO):
        with open(ARCHIVO_REGISTRO, "r", encoding="utf-8") as f:
            registro = json.load(f)

    # Buscar candidatos en la memoria de la tienda
    productos = df_shop.drop_duplicates(subset=['product_id']).to_dict('records')
    nuevos = []
    repesca = []
    
    for p in productos:
        sku = str(p.get("sku", "")).strip()
        if not sku or sku == "nan": continue
        
        estado = registro.get(sku)
        if not estado:
            nuevos.append(p)
        elif estado == DEFAULT_IMAGE_URL:
            repesca.append(p)
            
    # Lotes pequeños: Máximo 15 nuevos y 5 repescas por ejecución (20 en total)
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
            print("⚠️ Genérica" if usando_generica else "✅ Subida")
        else:
            print("❌ Error")
            
    # Guardar memoria
    with open(ARCHIVO_REGISTRO, "w", encoding="utf-8") as f:
        json.dump(registro, f, indent=2)