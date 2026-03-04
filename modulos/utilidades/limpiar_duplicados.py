import os
import time
from dotenv import load_dotenv
import pandas as pd

# GPS para encontrar tu .env si estás en la carpeta utils/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

import sys
sys.path.append(BASE_DIR)

from modulos.nucleo.sync_diagnostico import get_shopify_products, shopify_graphql

def main():
    print("🕵️‍♂️ Buscando clones en Shopify...")
    productos = get_shopify_products()
    
    skus_vistos = set()
    gids_a_borrar = []

    for p in productos:
        # Asumimos que la primera variante tiene el SKU
        if p["variants"]:
            sku = p["variants"][0]["sku"]
            if not sku:
                continue
                
            if sku in skus_vistos:
                # ¡Es un clon!
                gids_a_borrar.append(f"gid://shopify/Product/{p['id']}")
            else:
                skus_vistos.add(sku)

    print(f"\n🗑️ Se encontraron {len(gids_a_borrar)} productos duplicados.")
    
    if not gids_a_borrar:
        print("✨ Tu tienda está limpia.")
        return

    confirmar = input("¿Deseas eliminarlos definitivamente de Shopify? (s/n): ")
    if confirmar.lower() != 's':
        return

    # Borrado masivo
    BATCH = 50
    ok = 0
    err = 0
    for i in range(0, len(gids_a_borrar), BATCH):
        batch = gids_a_borrar[i:i+BATCH]
        q = "mutation { " + "\n".join([f'd{idx}: productDelete(input: {{ id: "{gid}" }}) {{ userErrors {{ message }} }}' for idx, gid in enumerate(batch)]) + " }"
        
        res = shopify_graphql(q, contexto="borrar_clones")
        if res:
            ok += len(batch)
        else:
            err += len(batch)
            
        print(f"   → Borrados: {ok}/{len(gids_a_borrar)}...", end="\r")

    print(f"\n\n✅ Limpieza terminada. OK: {ok} | Errores: {err}")

if __name__ == "__main__":
    main()
