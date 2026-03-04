#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 🔌 Importamos la conexión centralizada desde el diagnóstico para no repetir código
from modulos.nucleo.sync_diagnostico import shopify_graphql

# ============================
# ARCHIVAR PRODUCTOS (ESCUDO SEO)
# ============================
def archive_products_graphql(archivar):
    if not archivar:
        return 0, 0

    # NUEVO: Filtramos para ignorar los que ya están en estado 'archived'
    nuevos_por_archivar = [p for p in archivar if p.get("status_actual") != "archived"]
    
    if not nuevos_por_archivar:
        print("✅ No hay productos nuevos para archivar. Todo está al día.")
        return 0, 0

    # Cambiamos 'archivar' por 'nuevos_por_archivar' en la lista de GIDs
    product_gids = [
        f"gid://shopify/Product/{p['product_id']}"
        for p in nuevos_por_archivar
        if p.get("product_id")
    ]
    
    total = len(product_gids)
    # ... resto del código igual ...
    if total == 0:
        return 0, 0

    print(f"📦 Archivando {total} productos (Protección SEO) con GraphQL...")

    BATCH_UPDATE = 50
    ok_total = 0
    err_total = 0
    procesadas = 0
    total_batches = (total + BATCH_UPDATE - 1) // BATCH_UPDATE

    for batch_index in range(0, total, BATCH_UPDATE):
        batch_num = batch_index // BATCH_UPDATE + 1
        batch_gids = product_gids[batch_index : batch_index + BATCH_UPDATE]

        alias_bodies = []
        for idx, gid in enumerate(batch_gids):
            alias_name = f"a{idx}"
            # AQUÍ ESTÁ LA MAGIA: Usamos productUpdate cambiando el status a ARCHIVED
            alias_bodies.append(
                f'''
      {alias_name}: productUpdate(input: {{ id: "{gid}", status: ARCHIVED }}) {{
        product {{ id status }}
        userErrors {{
          field
          message
        }}
      }}'''
            )

        mutation_body = "\n".join(alias_bodies)
        mutation = f"mutation productArchiveBulk {{\n{mutation_body}\n}}"

        data = shopify_graphql(
            mutation,
            None,
            contexto="productArchive_bulk_aliases",
        )

        if not data or "data" not in data:
            batch_count = len(batch_gids)
            err_total += batch_count
            procesadas += batch_count
            print(f"\r   → 📦 Batch {batch_num}/{total_batches}: {procesadas}/{total} productos (OK={ok_total}, errores={err_total})", end="", flush=True)
            continue

        data_block = data["data"]

        for idx, gid in enumerate(batch_gids):
            alias_name = f"a{idx}"
            result = data_block.get(alias_name)

            if not result:
                err_total += 1
                continue

            user_errors = result.get("userErrors", []) or []
            if user_errors:
                err_total += 1
            else:
                ok_total += 1

        procesadas += len(batch_gids)
        print(f"\r   → 📦 Batch {batch_num}/{total_batches}: {procesadas}/{total} productos (OK={ok_total}, errores={err_total})", end="", flush=True)

    print()
    print(f"✅ Archivado completado. OK={ok_total}, errores={err_total}")
    return ok_total, err_total
