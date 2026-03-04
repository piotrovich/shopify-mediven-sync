import shopify
import time
import os
from dotenv import load_dotenv

load_dotenv()

# Configuración
SHOP_URL = os.getenv("SHOP_DOMAIN", "").replace("https://", "").strip("/")
PASSWORD = os.getenv("SHOPIFY_ADMIN_TOKEN") 
API_VERSION = '2024-01'

def conectar_shopify():
    session = shopify.Session(SHOP_URL, API_VERSION, PASSWORD)
    shopify.ShopifyResource.activate_session(session)
    print(f"🔗 Conectado exitosamente a {SHOP_URL}")

def main():
    print("==================================================")
    print("🌪️ INICIANDO LA GRAN PURGA DE IMÁGENES 🌪️")
    print("==================================================")
    conectar_shopify()

    has_next_page = True
    cursor = None
    productos_procesados = 0
    productos_limpiados = 0

    while has_next_page:
        # Construimos la query con paginación
        cursor_str = f', after: "{cursor}"' if cursor else ""
        query = f"""
        {{
          products(first: 50{cursor_str}) {{
            pageInfo {{
              hasNextPage
              endCursor
            }}
            edges {{
              node {{
                id
                title
                media(first: 20) {{
                  edges {{
                    node {{
                      id
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
        """
        
        try:
            result = shopify.GraphQL().execute(query)
            import json
            data = json.loads(result)
            
            products = data['data']['products']['edges']
            page_info = data['data']['products']['pageInfo']
            
            for edge in products:
                product = edge['node']
                product_id = product['id']
                title = product['title']
                media_edges = product['media']['edges']
                
                productos_procesados += 1
                
                if not media_edges:
                    print(f"[{productos_procesados}] ⏩ {title[:40]}... (Ya está sin fotos)")
                    continue
                
                # Extraer los IDs de las imágenes/media a borrar
                media_ids = [m['node']['id'] for m in media_edges]
                
                # Mutación para borrar la media
                delete_mutation = f"""
                mutation {{
                  productDeleteMedia(productId: "{product_id}", mediaIds: {json.dumps(media_ids)}) {{
                    deletedMediaIds
                    userErrors {{
                      field
                      message
                    }}
                  }}
                }}
                """
                
                del_result = shopify.GraphQL().execute(delete_mutation)
                productos_limpiados += 1
                print(f"[{productos_procesados}] 🗑️ {title[:40]}... ({len(media_ids)} fotos eliminadas)")
                
                # Pausa para no saturar la API
                time.sleep(0.5)

            has_next_page = page_info['hasNextPage']
            cursor = page_info['endCursor']

        except Exception as e:
            print(f"\n❌ Error en la paginación o ejecución: {e}")
            print("⏳ Esperando 10 segundos antes de reintentar...")
            time.sleep(10)

    print("\n==================================================")
    print("🎉 PURGA FINALIZADA")
    print(f"📦 Productos revisados: {productos_procesados}")
    print(f"✨ Productos limpiados: {productos_limpiados}")
    print("==================================================")

if __name__ == "__main__":
    main()
