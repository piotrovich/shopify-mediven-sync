import os
import json
import time
import requests
import io
import base64
from PIL import Image
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

SHOP_DOMAIN = os.getenv("SHOP_DOMAIN")
SHOPIFY_ADMIN_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-10")
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
URL_GENERICA = os.getenv("SHOPIFY_DEFAULT_IMAGE_URL")

ARCHIVO_REGISTRO = "data/registro_imagenes.json"
os.makedirs("data", exist_ok=True)

# ==========================================
# HELPER: GRAPHQL SHOPIFY
# ==========================================
def shopify_graphql(query, variables=None, contexto="graphql"):
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN,
    }
    for _ in range(5):
        try:
            r = requests.post(
                f"https://{SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/graphql.json",
                json={"query": query, "variables": variables or {}},
                headers=headers,
                timeout=30
            )
            if r.status_code == 429:
                time.sleep(2)
                continue
            return r.json()
        except Exception:
            time.sleep(2)
    return None

# ==========================================
# BUSCADOR SERPER
# ==========================================
def buscar_imagen_serper(query):
    url = "https://google.serper.dev/images"
    busqueda_exacta = f"{query} medicamento caja farmacia chile -site:farmex.cl -farmex"
    payload = json.dumps({"q": busqueda_exacta, "num": 5})
    headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}
    
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        if response.status_code == 200:
            datos = response.json()
            if "images" in datos and len(datos["images"]) > 0:
                for img in datos["images"]:
                    img_url = img.get("imageUrl", "")
                    if "farmex" not in img_url.lower():
                        return img_url
    except Exception:
        pass
    return None

# ==========================================
# PROCESAMIENTO DE IMAGEN (PILLOW)
# ==========================================
def descargar_y_estandarizar_imagen(url):
    """Descarga, limpia (fondo blanco), convierte a JPEG y codifica en Base64."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return None
            
        # Abrimos la imagen
        img = Image.open(io.BytesIO(r.content))
        
        # Si tiene fondo transparente, le ponemos fondo blanco
        if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
            img = img.convert("RGBA")
            fondo = Image.new("RGBA", img.size, (255, 255, 255, 255))
            fondo.paste(img, (0, 0), img)
            img = fondo.convert("RGB")
        else:
            img = img.convert("RGB")
            
        # Guardamos como JPEG en memoria
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG", quality=90)
        
        # Convertimos a Base64
        return base64.b64encode(buffer.getvalue()).decode('utf-8')
    except Exception as e:
        return None

# ==========================================
# SUBIDA A SHOPIFY
# ==========================================
def reemplazar_imagen_shopify(product_gid, url_nueva):
    # 1. Obtener y borrar las imágenes anteriores (GraphQL)
    query = f"""
    query {{
      product(id: "{product_gid}") {{
        media(first: 10) {{ edges {{ node {{ id }} }} }}
      }}
    }}
    """
    res = shopify_graphql(query, contexto="get_media")
    media_ids = []
    if res and "data" in res and res.get("data", {}).get("product"):
        for edge in res["data"]["product"]["media"]["edges"]:
            media_ids.append(edge["node"]["id"])
            
    if media_ids:
        mut_del = """
        mutation productDeleteMedia($productId: ID!, $mediaIds: [ID!]!) {
          productDeleteMedia(productId: $productId, mediaIds: $mediaIds) {
            userErrors { message }
          }
        }
        """
        shopify_graphql(mut_del, {"productId": product_gid, "mediaIds": media_ids}, contexto="del_media")
        time.sleep(0.5)
        
    # 2. Lavado de imagen: Descargar y convertir a JPEG estandarizado
    imagen_base64 = descargar_y_estandarizar_imagen(url_nueva)
    if not imagen_base64:
        return False
        
    # 3. Subir la imagen perfecta vía REST API
    product_id_num = product_gid.split("/")[-1]
    url_rest = f"https://{SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/products/{product_id_num}/images.json"
    
    payload = {
        "image": {
            "attachment": imagen_base64,
            "filename": "producto_optimizado.jpg" # Nombre forzado
        }
    }
    
    headers_rest = {
        "X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN,
        "Content-Type": "application/json"
    }
    
    r = requests.post(url_rest, json=payload, headers=headers_rest)
    return r.status_code in (200, 201)

# ==========================================
# FUNCIÓN PRINCIPAL
# ==========================================
def main():
    print("==================================================")
    print("🚀 INICIANDO BUSCADOR DE IMÁGENES (CON PILLOW)")
    print("==================================================")
    print(f"🔗 Conectado a Shopify: {SHOP_DOMAIN}")
    
    registro = {}
    if os.path.exists(ARCHIVO_REGISTRO):
        with open(ARCHIVO_REGISTRO, "r", encoding="utf-8") as f:
            try:
                registro = json.load(f)
            except:
                registro = {}
    
    print("📦 Escaneando el catálogo de Shopify...")
    query_scan = """
    query($cursor: String) {
      products(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            title
            variants(first: 1) { edges { node { sku } } }
          }
        }
      }
    }
    """
    
    cursor = None
    has_next = True
    pagina = 1
    productos_a_procesar = []
    
    while has_next:
        data = shopify_graphql(query_scan, {"cursor": cursor}, "scan_products")
        if not data or "data" not in data or not data["data"].get("products"):
            print(f"\n⚠️ Error en GraphQL al escanear: {data.get('errors') if data else 'Sin respuesta'}")
            break
            
        for edge in data["data"]["products"]["edges"]:
            node = edge["node"]
            sku = node["variants"]["edges"][0]["node"]["sku"] if node["variants"]["edges"] else None
            
            # MAGIA: Confiamos 100% en nuestra memoria JSON
            if sku and sku not in registro:
                productos_a_procesar.append({
                    "gid": node["id"],
                    "title": node["title"],
                    "sku": sku
                })
                
        print(f"   → Escaneando página {pagina}... (Encontrados: {len(productos_a_procesar)})", end="\r")
        
        page_info = data["data"]["products"]["pageInfo"]
        has_next = page_info["hasNextPage"]
        cursor = page_info["endCursor"]
        pagina += 1
        
    print(f"\n🔍 Se encontraron {len(productos_a_procesar)} productos en la fila.\n")
    
    if not productos_a_procesar:
        print("🎉 No hay productos pendientes. ¡Todo está perfecto!")
        return
        
    reales = 0
    genericas = 0
    total = len(productos_a_procesar)
    
    for idx, p in enumerate(productos_a_procesar, 1):
        sku = p["sku"]
        titulo = p["title"]
        gid = p["gid"]
        
        print(f"[{idx}/{total}] 🖼️ {titulo[:35]}... ", end="", flush=True)
        
        url_encontrada = buscar_imagen_serper(titulo)
        usando_generica = not url_encontrada
        url_final = url_encontrada if url_encontrada else URL_GENERICA
        
        # Esto automáticamente borra la foto FAILED del intento anterior!
        if reemplazar_imagen_shopify(gid, url_final):
            registro[sku] = url_final
            if usando_generica:
                print("⚠️ Uso Genérica")
                genericas += 1
            else:
                print("✅ Encontrada y Subida")
                reales += 1
        else:
            print("❌ Error de procesamiento")
            
        # Guardar en disco cada 10 para no perder el progreso
        if idx % 10 == 0:
            with open(ARCHIVO_REGISTRO, "w", encoding="utf-8") as f:
                json.dump(registro, f, indent=2)
                
    # Guardado final
    with open(ARCHIVO_REGISTRO, "w", encoding="utf-8") as f:
        json.dump(registro, f, indent=2)
        
    print(f"\n🎉 FINALIZADO | Reales: {reales} | Genéricas: {genericas}")

if __name__ == "__main__":
    main()
