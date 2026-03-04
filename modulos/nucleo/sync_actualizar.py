#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 🔌 Importamos la conexión centralizada desde el diagnóstico
from modulos.nucleo.sync_diagnostico import shopify_graphql

# ============================
# ACTUALIZACIÓN MASIVA DE BÁSICOS (TÍTULO Y REACTIVACIÓN)
# ============================
def bulk_update_product_basics(productos_a_actualizar):
    if not productos_a_actualizar:
        return
    print(f"📝 Actualizando nombres/estado de {len(productos_a_actualizar)} productos...")
    BATCH = 50
    for i in range(0, len(productos_a_actualizar), BATCH):
        batch = productos_a_actualizar[i:i+BATCH]
        alias_bodies = []
        for idx, p in enumerate(batch):
            gid = f"gid://shopify/Product/{p['product_id']}"
            titulo = p["Descripcion"].replace('"', '\\"')
            # MAGIA 2: Forzamos status: ACTIVE para resucitarlo si estaba archivado
            alias_bodies.append(
                f'p{idx}: productUpdate(input: {{ id: "{gid}", title: "{titulo}", status: ACTIVE }}) {{ '
                f'product {{ id }} userErrors {{ message }} }}'
            )
        
        mutation = "mutation { " + "\n".join(alias_bodies) + " }"
        shopify_graphql(mutation, contexto="bulk_update_basics")
        print(f"   → {min(i+BATCH, len(productos_a_actualizar))} procesados...", end="\r")
    print("\n✅ Títulos y estados actualizados.")


# ============================
# GRAPHQL BULK (MANTENIDO ORIGINAL)
# ============================
def graphql_bulk_update_variants(variantes):
    print("=== INICIO (ACTUALIZAR PRECIOS) ===")

    variantes = [v for v in variantes if "Nuevo_Precio" in v]

    if not variantes:
        print("⚠️ No hay variantes para actualizar.")
        return {"ok": 0, "errores": 0}

    productos = {}
    for v in variantes:
        pid = v["product_id"]
        if pid not in productos:
            productos[pid] = []
        productos[pid].append(v)

    total_batches = len(productos)
    print(f"🔁 Ejecutando {total_batches} actualizaciones por producto...")

    ok_global = 0
    err_global = 0

    for idx, (pid, group) in enumerate(productos.items(), 1):
        product_gid = f"gid://shopify/Product/{pid}"

        variants_payload = [
            {
                "id": f"gid://shopify/ProductVariant/{v['variant_id']}",
                "price": str(v["Nuevo_Precio"]),
            }
            for v in group
        ]

        mutation = """
        mutation updateProductVariants($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(
            productId: $productId,
            variants: $variants,
            allowPartialUpdates: true
          ) {
            productVariants {
              id
            }
            userErrors {
              field
              message
            }
          }
        }
        """

        variables = {
            "productId": product_gid,
            "variants": variants_payload
        }

        r = shopify_graphql(mutation, variables, contexto="bulk_variant_update")

        if not r or "data" not in r:
            err_global += len(group)
        else:
            result = r["data"]["productVariantsBulkUpdate"]
            if result["userErrors"]:
                err_global += len(group)
            else:
                ok_global += len(group)

        porcentaje = round((idx / total_batches) * 100, 1)
        print(f"\r📦 Producto {idx}/{total_batches} — {porcentaje}%", end="", flush=True)

    print("\n\n=== RESULTADO FINAL ===")
    print(f"✔ Variantes actualizadas correctamente: {ok_global}")
    print(f"❌ Variantes con error: {err_global}")

    return {"ok": ok_global, "errores": err_global}


# ============================
# QUITAR IMPUESTOS MASIVAMENTE
# ============================
def quitar_impuestos_graphql(variantes_malas):
    if not variantes_malas: return 0, 0
    productos = {}
    for v in variantes_malas:
        pid = v["product_id"]
        if pid not in productos: productos[pid] = []
        productos[pid].append(v)

    ok_g, err_g = 0, 0
    for pid, group in productos.items():
        product_gid = f"gid://shopify/Product/{pid}"
        variants_payload = [{"id": f"gid://shopify/ProductVariant/{v['variant_id']}", "taxable": False} for v in group]
        
        mutation = """
        mutation updateProductVariants($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants, allowPartialUpdates: true) {
            productVariants { id } userErrors { message }
          }
        }
        """
        r = shopify_graphql(mutation, {"productId": product_gid, "variants": variants_payload}, contexto="remove_tax")
        if not r or "data" not in r or r["data"]["productVariantsBulkUpdate"]["userErrors"]:
            err_g += len(group)
        else:
            ok_g += len(group)
    return ok_g, err_g
