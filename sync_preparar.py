#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
sync_preparar.py

Paso 1 de la sincronización:
- Descarga inventario desde Mediven.
- Descarga productos desde Shopify (solo lectura, GraphQL).
- Detecta productos veterinarios.
- Calcula las listas de CREAR / ACTUALIZAR / ARCHIVAR.
- Guarda el estado en data/sync_state.json y genera un Excel de diagnóstico.

Se ejecuta con:
    python sync_preparar.py
    python sync_preparar.py --modo produccion
    python sync_preparar.py --modo diagnostico

El modo sólo afecta el texto de log; este script NO toca Shopify.
"""

import os
import json
from datetime import datetime

import pandas as pd

from sync_diagnostico import (
    SIMULATE,
    PRICE_FIELD,
    get_mediven_inventory,
    get_shopify_products,
    normalize_shopify_products,
    calcular_precio,
    generar_excel,
)

STATE_DIR = "data"
STATE_PATH = os.path.join(STATE_DIR, "sync_state.json")


def preparar_sync(modo: str = "produccion") -> None:
    """Realiza el diagnóstico completo y guarda el estado en disco."""

    os.makedirs(STATE_DIR, exist_ok=True)

    print(f"🔧 Modo actual (PREPARAR): {modo.upper()}")
    if SIMULATE:
        print("🧪 SIMULATE=true → solo diagnóstico (no aplica cambios en otros scripts).")
    print("=== INICIO (PREPARAR) ===")

    # ---------------------------------------------------------------
    # 1) Datos origen: Mediven + Shopify
    # ---------------------------------------------------------------
    mediven_data = get_mediven_inventory()
    print("Descargando productos de Shopify (GraphQL, solo lectura)...")
    shopify_products = get_shopify_products()
    print(f"✅ Shopify (GraphQL): {len(shopify_products)} productos cargados.")

    df_med = pd.DataFrame(mediven_data)
    df_shop = pd.DataFrame(normalize_shopify_products(shopify_products))

    # Normalizar columnas clave
    if not df_shop.empty:
        df_shop["sku"] = df_shop["sku"].astype(str).str.strip()
    else:
        df_shop["sku"] = pd.Series(dtype=str)

    df_med["Codigo"] = df_med["Codigo"].astype(str).str.strip()

    skus_med = set(df_med["Codigo"])

    # ---------------------------------------------------------------
    # 2) Mapa SKU → fila Shopify
    # ---------------------------------------------------------------
    shop_by_sku = {}
    for _, row in df_shop.iterrows():
        sku = row["sku"]
        if sku and sku not in shop_by_sku:
            shop_by_sku[sku] = row

    # ---------------------------------------------------------------
    # 3) Detectar veterinarios
    # ---------------------------------------------------------------
    palabras_vet = [
        "perro",
        "perros",
        "gato",
        "gatos",
        "mascota",
        "veterinaria",
        "mundo animal"
    ]

    def es_vet(row):
        texto = " ".join(
            [
                str(row.get(c, "")).lower()
                for c in ["Descripcion", "Laboratorio", "AccionTerapeutica"]
            ]
        )
        return any(p in texto for p in palabras_vet)

    df_med["EsVet"] = df_med.apply(es_vet, axis=1)
    vet_skus = set(df_med[df_med["EsVet"] == True]["Codigo"])

    print(
        f"🐾 Productos veterinarios detectados: "
        f"{len(vet_skus)} (excluidos de la sync)."
    )

    # ---------------------------------------------------------------
    # 4) Construir listas CREAR / ACTUALIZAR / ARCHIVAR
    # ---------------------------------------------------------------
    crear = []
    actualizar = []
    archivar = []

    # CREAR / ACTUALIZAR
    for _, row in df_med.iterrows():
        sku = row["Codigo"]

        if sku in vet_skus:
            continue

        nuevo_precio = calcular_precio(row.get(PRICE_FIELD, 0))

        if sku in shop_by_sku:
            shop_row = shop_by_sku[sku]
            precio_actual = float(shop_row["price"] or 0)

            if abs(precio_actual - nuevo_precio) >= 1:
                actualizar.append(
                    {
                        "SKU": sku,
                        "Descripcion": row.get("Descripcion", ""),
                        "Precio_Shopify": precio_actual,
                        "Nuevo_Precio": nuevo_precio,
                        "variant_id": shop_row["variant_id"],
                        "product_id": shop_row["product_id"],
                    }
                )
        else:
            crear.append(
                {
                    "SKU": sku,
                    "Descripcion": row.get("Descripcion", ""),
                    "Precio": nuevo_precio,
                    "Stock": 100,
                }
            )

    # ARCHIVAR
    for _, row in df_shop.iterrows():
        sku = row["sku"]
        if not sku:
            continue
        if sku not in skus_med and sku not in vet_skus:
            archivar.append(
                {
                    "SKU": sku,
                    "product_id": row["product_id"],
                    "Descripcion": row.get("product_title", ""),
                }
            )

    print(
        f"📊 Resumen cambios: "
        f"CREAR={len(crear)} | ACTUALIZAR={len(actualizar)} | ARCHIVAR={len(archivar)}"
    )

    # ---------------------------------------------------------------
    # 5) Guardar estado en disco
    # ---------------------------------------------------------------
    estado = {
        "timestamp": datetime.now().isoformat(),
        "modo": modo,
        "mediven_count": len(df_med),
        "shopify_count": len(df_shop),
        "crear": crear,
        "actualizar": actualizar,
        "archivar": archivar,
    }

    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(estado, f, ensure_ascii=False, indent=2)

    print(f"💾 Estado de sincronización guardado en {STATE_PATH}")

    # ---------------------------------------------------------------
    # 6) Excel de diagnóstico (mismo formato que antes)
    # ---------------------------------------------------------------
    try:
        generar_excel(crear, actualizar, archivar, mediven_data)
    except Exception as e:
        print(f"⚠️ Error al generar Excel de diagnóstico: {e}")

    print("=== FIN (PREPARAR) ===")


def main():
    import sys

    modo = "diagnostico"
    args = [a.lower() for a in sys.argv[1:]]

    if "--modo" in args:
        idx = args.index("--modo")
        if idx + 1 < len(args) and args[idx + 1] == "produccion":
            modo = "produccion"

    preparar_sync(modo=modo)


if __name__ == "__main__":
    main()
