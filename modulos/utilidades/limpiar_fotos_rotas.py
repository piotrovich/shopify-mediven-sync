import os
import json
import time
from modulos.nucleo.sync_diagnostico import shopify_graphql

ARCHIVO_REGISTRO = "data/registro_imagenes.json"

def main():
    print("=========================================")
    print(" 🧹 LIMPIADOR DE FOTOS ROTAS (FAILED)")
    print("=========================================")
    
    # Cargar la memoria actual
    registro = {}
    if os.path.exists(ARCHIVO_REGISTRO):
        with open(ARCHIVO_REGISTRO, "r", encoding="utf-8") as f:
            registro = json.load(f)

    # Query que busca explícitamente el "status" de la imagen
    query = """
    query($cursor: String) {
      products(first: 50, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            title
            variants(first:1) { edges { node { sku } } }
            media(first: 5) {
              edges {
                node {
                  id
                  status
                }
              }
            }
          }
        }
      }
    }
    """
    
    cursor = None
    has_next = True
    pagina = 1
    rotas_encontradas = 0
    
    while has_next:
        print(f"🔍 Escaneando página {pagina} de Shopify buscando errores...", end="\r")
        data = shopify_graphql(query, {"cursor": cursor}, "get_media_status")
        
        if not data or "data" not in data: 
            break
            
        for edge in data["data"]["products"]["edges"]:
            node = edge["node"]
            product_gid = node["id"]
            
            # Sacar el SKU (si no tiene, saltamos)
            sku = None
            if node["variants"]["edges"]:
                sku = node["variants"]["edges"][0]["node"]["sku"]
            
            # Buscar fotos que digan "FAILED"
            media_to_delete = []
            for m_edge in node["media"]["edges"]:
                if m_edge["node"]["status"] == "FAILED":
                    media_to_delete.append(m_edge["node"]["id"])
                    
            if media_to_delete:
                print(f"\n   ⚠️ Foto rota detectada en: {node['title'][:40]} (SKU: {sku})")
                
                # 1. Borrar la foto mala de Shopify
                mut_del = """
                mutation productDeleteMedia($productId: ID!, $mediaIds: [ID!]!) {
                  productDeleteMedia(productId: $productId, mediaIds: $mediaIds) {
                    userErrors { message }
                  }
                }
                """
                shopify_graphql(mut_del, {"productId": product_gid, "mediaIds": media_to_delete}, "del_media")
                
                # 2. Borrar de la memoria local para obligar al script a buscar de nuevo
                if sku and sku in registro:
                    del registro[sku]
                    
                rotas_encontradas += 1
                time.sleep(0.5)
                
        page_info = data["data"]["products"]["pageInfo"]
        has_next = page_info["hasNextPage"]
        cursor = page_info["endCursor"]
        pagina += 1
        
    # Guardar la memoria corregida
    with open(ARCHIVO_REGISTRO, "w", encoding="utf-8") as f:
        json.dump(registro, f, indent=2)
        
    print(f"\n\n✅ Limpieza completada. Se detectaron y borraron {rotas_encontradas} fotos con error.")
    print("👉 PRÓXIMO PASO: Ejecuta 'python sincronizar_imagenes.py' para rellenar estos huecos.")

if __name__ == "__main__":
    main()
