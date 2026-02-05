#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
sync_actualizar.py

Paso 3 de la sincronización:
- Lee el archivo data/sync_state.json generado por sync_preparar.py.
- Actualiza los precios de las variantes listadas en "actualizar"
  usando GraphQL (productVariantsBulkUpdate).

Se ejecuta con:
    python sync_actualizar.py
"""

import os
import json

from sync_diagnostico import graphql_bulk_update_variants

STATE_PATH = os.path.join("data", "sync_state.json")


def cargar_estado():
    if not os.path.exists(STATE_PATH):
        raise FileNotFoundError(
            f"No se encontró {STATE_PATH}. Ejecuta primero sync_preparar.py."
        )
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    estado = cargar_estado()
    actualizar = estado.get("actualizar", [])

    # Llamamos a la función que ya contiene los logs bonitos
    ok, err = graphql_bulk_update_variants(actualizar)

    # Solo mensaje final (opcional)
    print(f"\nProceso finalizado: OK={ok}, errores={err}")


if __name__ == "__main__":
    main()
