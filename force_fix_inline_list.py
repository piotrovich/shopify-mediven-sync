import requests
import os
import time
import random
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from ddgs import DDGS 

# ==========================================
# 📝 LISTA DE PRODUCTOS A CORREGIR
# ==========================================
LISTA_PRODUCTOS_MALOS = [
    "ACIFIN COM MAST X 10",
    "ACCU-CHEK FASTCLIK 24 LANCETS (DM)",
    "ACECNOU SBR 3 GR X 1", 
    "ACECNOU SBR 3 GR X 2",
    "ACEITE DE ALMENDRAS X 50 ML QUIMNATURA",
    "ACIDO CITRICO X 80 GR QUIMNATURA",
    "ACIDO SALICILICO SBR X 5 GR (CAJ X 25) QUIMNATURA",
    "ACOTOL COM X 28",
    "ACTIOVAL CAPS X 30",
    "ADRISIN COM SUB X 50",
    "AERO-GASTROL CAP X 20",
    "AGUA TERMAL X 150 ML LA ROCHE POSAY",
    "AGUA TERMAL X 150 ML VICHY",
    "AGUJA HIPODERMICA 19 G X 1 1/2 CAJA X 100 CRANBERRY (DM)",
    "AILICEC SOL OFT X 5 ML",
    "AJO NEGRO X 60 CAP",
    "ALCOHOL GEL C/VALVULA 1 LT",
    "ALCOHOL SPRAY 70% X 130 ML ELITE",
    "ALERPHYT X 90 CAP AURAVITALIS",
    "ALIZON COM X 28",
    "ALONDRA COM X 30",
    "ALWAYS PROTECTORES DIARIOS LARGOS SCENTED X 50 UND",
    "ALWAYS TOALLA INFINITY FLEX FOAM ULTRA ABSORBENTE X 16 UND",
    "ALWAYS TOALLA ULTRA FINA NOCTURNA MALLA F/ABUN X 16 UND",
    "ALWAYS TOALLA ULTRA FINA NOCTURNA SUAVE X 14 UND",
    "ALWAYS TOALLA ULTRA FINA NOCTURNA SUAVE X 8 UND",
    "ALWAYS TOALLA ULTRA FINA SUAVE FLUJO ABUNDANTE C/A X 24 UND",
    "AMAZING CARE ELECTROLITOS EN POLVO NARANJA X 30 UND",
    "AMBROXOL ADL JBE 30 MG X 100 ML OPKO",
    "AGUA MICELAR EN ACEITE FACE X 400 ML GARNIER",
    "AGUA MICELAR PIEL SENSIBLE HIDRA TOTAL5 X 200 ML"
]
# ==========================================

load_dotenv()

SHOP_DOMAIN = os.getenv("SHOP_DOMAIN")
SHOPIFY_ADMIN_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-10")
GRAPHQL_ENDPOINT = f"https://{SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

MAX_WORKERS = 3

SESSION = requests.Session()
SESSION.headers.update({
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN,
})

print_lock = threading.Lock()

def safe_print(msg):
    with print_lock:
        print(msg, flush=True)

def limpiar_nombre_producto(nombre):
    nombre = re.sub(r'\s*\(.*?\)', '', nombre)
    nombre = re.sub(r'\s+X\s+\d+.*$', '', nombre)
    traducciones = {
        " PVO ": " Polvo ", " CRE ": " Crema ", " DES ": " Desodorante ", 
        " SOL ": " Solucion ", " COMP ": " Comprimidos ", " JAB ": " Jabon "
    }
    for k, v in traducciones.items():
        if k in nombre:
            nombre = nombre.replace(k, v)
    return nombre.strip()

def buscar_imagen_quirurgica(nombre_original):
    nombre_limpio = limpiar_nombre_producto(nombre_original)
    
    # Búsqueda forzada: "medicamento", "caja", "farmacia"
    variantes = [
        f"{nombre_limpio} medicamento caja farmacia chile", 
        f"{nombre_limpio} envase producto chile",
        f"{nombre_limpio} farmaceutico",
        f"{nombre_limpio} chile",
    ]
    
    BLACKLIST = [
        "tripadvisor", "wikipedia", "pinterest", "stock", "alamy", "freepik", 
        "paisaje", "turismo", "mapa", "ubicacion", "farmex", "farmazon"
    ]

    for query in variantes:
        try:
            time.sleep(random.uniform(1.0, 1.5))
            with DDGS(timeout=20) as ddgs:
                results = list(ddgs.images(query, region="cl-es", max_results=5, safesearch='off'))
                for res in results:
                    url = res['image']
                    if url.startswith("data:"): continue
                    if any(bad in url.lower() for bad in BLACKLIST): continue
                    if "logo" in url.lower(): continue
                    return url, query
        except Exception:
            continue
    return None, nombre_limpio

def gql(query, variables=None):
    for _ in range(3):
        try:
            r = SESSION.post(GRAPHQL_ENDPOINT, json={"query": query, "variables": variables or {}}, timeout=20)
            if r.status_code == 200:
                return r.json()
            time.sleep(1)
        except:
            time.sleep(1)
    return None

def get_product_id_by_title(title_fragment):
    query = """
    query($query: String!) {
      products(first: 5, query: $query) {
        edges {
          node {
            id
            title
            media(first: 10) { edges { node { id } } }
          }
        }
      }
    }
    """
    data = gql(query, {"query": f"title:{title_fragment}"})
    if data and "data" in data and data["data"]["products"]["edges"]:
        return data["data"]["products"]["edges"][0]["node"]
    return None

def delete_all_media(product_id, media_ids):
    if not media_ids: return
    mutation = """
    mutation productDeleteMedia($productId: ID!, $mediaIds: [ID!]!) {
      productDeleteMedia(productId: $productId, mediaIds: $mediaIds) {
        deletedMediaIds
      }
    }
    """
    gql(mutation, {"productId": product_id, "mediaIds": media_ids})

def upload_media(product_id, url):
    mutation = """
    mutation productCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) {
      productCreateMedia(productId: $productId, media: $media) {
        media { id }
        mediaUserErrors { message }
      }
    }
    """
    variables = {"productId": product_id, "media": [{"originalSource": url, "mediaContentType": "IMAGE"}]}
    data = gql(mutation, variables)
    if data and "data" in data:
        errs = data["data"]["productCreateMedia"].get("mediaUserErrors", [])
        if not errs:
            return True
    return False

def process_single_product(nombre_producto):
    clean_name = nombre_producto.strip()
    if not clean_name: return
    
    safe_print(f"🔍 Buscando en Shopify: '{clean_name}'...")
    
    # 1. Buscar en Shopify
    prod_data = get_product_id_by_title(clean_name)
    
    if not prod_data:
        # Intento de búsqueda relajada (primeras 3 palabras)
        palabras = clean_name.split()
        if len(palabras) > 2:
            short_name = " ".join(palabras[:3])
            prod_data = get_product_id_by_title(short_name)
    
    if not prod_data:
        safe_print(f"❌ NO ENCONTRADO EN SHOPIFY: {clean_name}")
        return
    
    p_id = prod_data['id']
    real_title = prod_data['title']
    old_media_ids = [m["node"]["id"] for m in prod_data["media"]["edges"]]
    
    # 2. Borrar lo viejo (Si existe)
    if old_media_ids:
        delete_all_media(p_id, old_media_ids)
        safe_print(f"   🗑️  Imágenes borradas para: {real_title}")
    else:
        safe_print(f"   ℹ️  El producto no tenía imágenes. Buscando una nueva...")

    # 3. Buscar y subir lo nuevo (Quirúrgico)
    new_url, busqueda_usada = buscar_imagen_quirurgica(real_title)
    
    if new_url:
        success = upload_media(p_id, new_url)
        if success:
            safe_print(f"✅ FOTO OK: {real_title[:20]}... [Busq: '{busqueda_usada}']")
        else:
            safe_print(f"⚠️ Error subiendo imagen para: {real_title}")
    else:
        safe_print(f"❌ No se encontró imagen decente para: {real_title}")

def main():
    print("=== 🚑 CORRECTOR V2 (Ejecución de Lista Manual) ===")
    
    lista_limpia = [p for p in LISTA_PRODUCTOS_MALOS if p.strip()]
    total = len(lista_limpia)
    
    if total == 0:
        print("La lista está vacía.")
        return

    print(f"🚀 Procesando {total} productos específicos...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_single_product, prod) for prod in lista_limpia]
        for future in as_completed(futures):
            future.result()

    print("\n✅ Proceso terminado.")

if __name__ == "__main__":
    main()