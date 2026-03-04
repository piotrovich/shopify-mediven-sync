#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
sync_eliminar.py

Paso 2 de la sincronización:
- Lee el archivo data/sync_state.json generado por sync_preparar.py.
- Elimina definitivamente en Shopify los productos listados en "archivar".

Se ejecuta con:
    python sync_eliminar.py
"""

import os
import json

from modulos.nucleo.sync_diagnostico import archive_products_graphql

STATE_PATH = os.path.join("data", "sync_state.json")


def cargar_estado():
    if not os.path.exists(STATE_PATH):
        raise FileNotFoundError(
            f"No se encontró {STATE_PATH}. Ejecuta primero sync_preparar.py."
        )
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    print("=== INICIO (ELIMINAR) ===")

    estado = cargar_estado()
    archivar = estado.get("archivar", [])

    print(f"🗑️ Productos a eliminar (ARCHIVAR): {len(archivar)}")

    if not archivar:
        print("ℹ️ No hay productos para eliminar. Nada que hacer.")
        print("=== FIN (ELIMINAR) ===")
        return

    ok, err = archive_products_graphql(archivar)

    print(f"✅ Eliminación definitiva completada. OK={ok}, errores={err}")
    print("=== FIN (ELIMINAR) ===")


if __name__ == "__main__":
    main()
