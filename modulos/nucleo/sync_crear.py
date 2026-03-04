#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import math
import concurrent.futures
import random

# 🔌 Importamos las herramientas clave desde el diagnóstico
from modulos.nucleo.sync_diagnostico import (
    shopify_graphql, 
    SHOPIFY_LOCATION_GID, 
    ONLINE_STORE_PUBLICATION_ID, 
    DEFAULT_IMAGE_URL
)

# ============================
# GRAPHQL: SETEAR STOCK=100 (MANTENIDO ORIGINAL)
# ============================
def set_stock_100_for_inventory_items(inventory_item_ids, contexto="set_stock"):
    if not inventory_item_ids:
        return

    quantities = [
        {
            "inventoryItemId": inv_id,
            "locationId": SHOPIFY_LOCATION_GID,
            "quantity": 100,
        }
        for inv_id in inventory_item_ids
    ]

    query = """
    mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
      inventorySetQuantities(input: $input) {
        inventoryAdjustmentGroup {
          reason
        }
        userErrors {
          code
          field
          message
        }
      }
    }
    """

    variables = {
        "input": {
            "name": "available",
            "reason": "correction",
            "ignoreCompareQuantity": True,
            "referenceDocumentUri": "gid://farmaciaslf/SyncJob/Mediven",
            "quantities": quantities,
        }
    }

    data = shopify_graphql(query, variables, contexto=contexto)

    if not data or "data" not in data:
        print(f"\r⚠️ No se pudo ejecutar inventorySetQuantities ({contexto}).", end="", flush=True)
        return

    payload = data.get("data", {}).get("inventorySetQuantities")
    if not payload:
        print(f"\r⚠️ Respuesta vacía en inventorySetQuantities ({contexto}).", end="", flush=True)
        return

    user_errors = payload.get("userErrors", [])
    if user_errors:
        print(f"\r⚠️ Errores inventorySetQuantities {contexto}: {len(user_errors)}       ", end="", flush=True)


# ============================
# PUBLICAR PRODUCTO (MANTENIDO ORIGINAL)
# ============================
def publish_product_online_store(product_gid):
    if not ONLINE_STORE_PUBLICATION_ID:
        return

    mutation = """
    mutation publishToOnlineStore($id: ID!, $pubId: ID!) {
      publishablePublish(
        id: $id,
        input: { publicationId: $pubId }
      ) {
        publishable {
          ... on Product {
            id
          }
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    variables = {
        "id": product_gid,
        "pubId": ONLINE_STORE_PUBLICATION_ID,
    }

    shopify_graphql(
        mutation,
        variables,
        contexto="publishablePublish_online_store",
    )


# ============================
# WORKER: CREAR 1 PRODUCTO (MANTENIDO ORIGINAL)
# ============================
def crear_producto_worker(p):
    sku = p["SKU"]
    titulo = p["Descripcion"]
    precio = p["Precio"]

    mutation_product_create = """
    mutation productCreate($product: ProductCreateInput!) {
      productCreate(product: $product) {
        product {
          id
          title
          variants(first: 1) {
            nodes {
              id
              inventoryItem {
                id
              }
            }
          }
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    variables_create = {
        "product": {
            "title": titulo,
            "status": "ACTIVE",
        }
    }

    data_create = shopify_graphql(
        mutation_product_create,
        variables_create,
        contexto="productCreate_turbo",
    )

    if (
        not data_create
        or "data" not in data_create
        or not data_create["data"].get("productCreate")
    ):
        print(f"❌ ERROR productCreate → SKU={sku}")
        return 0, 1

    pc = data_create["data"]["productCreate"]
    user_errors = pc.get("userErrors", []) or []
    if user_errors:
        print(f"❌ userErrors en productCreate → {user_errors}")
        return 0, 1

    product = pc.get("product")
    if not product:
        print(f"❌ productCreate sin product → SKU={sku}")
        return 0, 1

    product_gid = product["id"]
    variants = product["variants"]["nodes"]
    default_variant = variants[0]
    variant_gid = default_variant["id"]

    mutation_pv_bulk = """
    mutation productVariantsBulkUpdate(
      $productId: ID!,
      $variants: [ProductVariantsBulkInput!]!
    ) {
      productVariantsBulkUpdate(
        productId: $productId,
        variants: $variants,
        allowPartialUpdates: true
      ) {
        productVariants {
          id
          inventoryItem {
            id
          }
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    variants_input = [
        {
            "id": variant_gid,
            "price": str(precio),
            "inventoryItem": {
                "sku": sku,
                "tracked": True,
                "requiresShipping": True,
            },
        }
    ]

    variables_pv = {
        "productId": product_gid,
        "variants": variants_input,
    }

    data_pv = shopify_graphql(
        mutation_pv_bulk,
        variables_pv,
        contexto="productVariantsBulkUpdate_create_turbo",
    )

    if not data_pv or "data" not in data_pv:
        print(f"❌ ERROR pvBulk (sin data) → SKU={sku}")
        return 0, 1

    pv_result = data_pv["data"]["productVariantsBulkUpdate"]

    user_errors_pv = pv_result.get("userErrors", []) or []
    if user_errors_pv:
        print(f"❌ userErrors en pvBulk → SKU={sku} → {user_errors_pv}")
        return 0, 1

    new_inventory_item = pv_result["productVariants"][0]["inventoryItem"]["id"]
    set_stock_100_for_inventory_items(
        [new_inventory_item],
        contexto="set_stock_create_turbo",
    )

    if DEFAULT_IMAGE_URL:
        mutation_product_create_media = """
        mutation productCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) {
          productCreateMedia(productId: $productId, media: $media) {
            media {
              id
            }
            mediaUserErrors {
              field
              message
            }
          }
        }
        """

        variables_media = {
            "productId": product_gid,
            "media": [
                {
                    "originalSource": DEFAULT_IMAGE_URL,
                    "mediaContentType": "IMAGE",
                }
            ],
        }

        shopify_graphql(
            mutation_product_create_media,
            variables_media,
            contexto="productCreateMedia_turbo",
        )

    publish_product_online_store(product_gid)
    return 1, 0


# ============================
# CREAR PRODUCTOS TURBO (MANTENIDO ORIGINAL)
# ============================
def crear_productos_graphql_turbo(productos, batch_size=20):
    import concurrent.futures
    import random

    total = len(productos)
    if total == 0:
        print("No hay productos para crear.")
        return {"ok": 0, "errores": 0}

    print(f"🆕 Creando productos nuevos (TURBO optimizado): {total} productos...")

    workers = 4  
    min_workers = 2  
    max_workers = 6  

    total_ok = 0
    total_err = 0
    procesados = 0

    base_sleep = 0.6
    num_batches = math.ceil(total / batch_size)

    for b in range(num_batches):
        inicio = b * batch_size
        fin = min((b + 1) * batch_size, total)
        batch = productos[inicio:fin]

        print(f"\n🚧 Batch {b+1}/{num_batches} — procesando {len(batch)} productos...")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(crear_producto_worker, p): p
                for p in batch
            }

            for future in concurrent.futures.as_completed(future_map):
                p = future_map[future]
                try:
                    ok, err = future.result()
                except Exception as e:
                    print(f"❌ Excepción inesperada en SKU {p['SKU']}: {e}")
                    ok, err = 0, 1

                total_ok += ok
                total_err += err
                procesados += 1

                print(
                    f"\r→ Progreso global: {procesados}/{total} "
                    f"(OK={total_ok}, ERR={total_err}) "
                    f"| Workers={workers}",
                    end="",
                    flush=True
                )

                if err > 0:
                    workers = max(min_workers, workers - 1)
                else:
                    if random.random() < 0.2:
                        workers = min(max_workers, workers + 1)

        time.sleep(base_sleep)

    print("\n📦 Creación finalizada!")
    print(f"✔ OK: {total_ok}")
    print(f"❌ ERRORES: {total_err}")

    return {"ok": total_ok, "errores": total_err}
