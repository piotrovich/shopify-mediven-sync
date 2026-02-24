import shopify
import json
import os
import time
from dotenv import load_dotenv

load_dotenv()

# Configuración
raw_shop_url = os.getenv("SHOP_DOMAIN", "").replace("https://", "").strip("/")
SHOP_URL = raw_shop_url
PASSWORD = os.getenv("SHOPIFY_ADMIN_TOKEN") 
API_VERSION = '2024-01'
ARCHIVO_DICCIONARIO = 'data/diccionario_ia.json'

def conectar_shopify():
    session = shopify.Session(SHOP_URL, API_VERSION, PASSWORD)
    shopify.ShopifyResource.activate_session(session)
    print(f"🔗 Conectado exitosamente a {SHOP_URL}")

def actualizar_producto(sku, datos_ia):
    try:
        query = f"""{{ productVariants(first: 1, query: "sku:{sku}") {{ edges {{ node {{ product {{ id handle }} }} }} }} }}"""
        result = shopify.GraphQL().execute(query)
        data = json.loads(result)
        
        if not data.get('data', {}).get('productVariants', {}).get('edges'):
            return "NO_ENCONTRADO"
            
        product_gid = data['data']['productVariants']['edges'][0]['node']['product']['id']
        pure_id = product_gid.split('/')[-1]

        ficha_texto = datos_ia.get("ficha_tecnica", "")
        ficha_html = ficha_texto.replace('\n', '<br>')

        metafield = shopify.Metafield({
            'owner_resource': 'product',
            'owner_id': pure_id,
            'namespace': 'custom',
            'key': 'ficha_tecnica',
            'value': ficha_html,
            'type': 'multi_line_text_field'
        })
        metafield.save()

        desc_amable = datos_ia.get("descripcion_amable", "")
        prod = shopify.Product.find(pure_id)
        prod.body_html = desc_amable 
        prod.save()

        return "OK"

    except Exception as e:
        print(f"\n❌ Error con SKU {sku}: {e}")
        return "ERROR"

def main():
    print("==================================================")
    print("🚀 INICIANDO SINCRONIZACIÓN IA -> SHOPIFY (OPTIMIZADO)")
    print("==================================================")
    
    conectar_shopify()

    if not os.path.exists(ARCHIVO_DICCIONARIO):
        print(f"❌ No se encontró el archivo {ARCHIVO_DICCIONARIO}")
        return

    with open(ARCHIVO_DICCIONARIO, 'r', encoding='utf-8') as f:
        diccionario = json.load(f)

    # 🔥 FILTRO MÁGICO: Solo toma los que NO han sido subidos
    pendientes = {sku: datos for sku, datos in diccionario.items() if not datos.get("subido_shopify", False)}
    
    total = len(pendientes)
    if total == 0:
        print("🎉 Todos los productos ya están actualizados en Shopify. Nada que subir.")
        return

    print(f"📦 Se encontraron {total} productos NUEVOS listos para subir.\n")

    exitos = 0
    errores = 0

    for i, (sku, datos) in enumerate(pendientes.items(), 1):
        print(f"[{i}/{total}] Actualizando SKU {sku}...", end=" ", flush=True)
        
        resultado = actualizar_producto(sku, datos)
        
        if resultado == "OK" or resultado == "NO_ENCONTRADO":
            if resultado == "OK":
                exitos += 1
                print("✅ Listo")
            else:
                print("⚠️ SKU no hallado en tienda")
            
            # Marcamos como subido para que no lo intente procesar nunca más
            diccionario[sku]["subido_shopify"] = True
            
            # Guardamos el archivo por cada éxito (así si se corta, no perdemos el progreso)
            with open(ARCHIVO_DICCIONARIO, 'w', encoding='utf-8') as f:
                json.dump(diccionario, f, ensure_ascii=False, indent=2)
                
        else:
            errores += 1
            print("❌ Falló")

        time.sleep(0.6)

    print("\n==================================================")
    print("🎉 RESUMEN DE SINCRONIZACIÓN")
    print(f"✅ Actualizados/Procesados: {exitos}")
    print(f"❌ Errores: {errores}")
    print("==================================================")

if __name__ == "__main__":
    main()
