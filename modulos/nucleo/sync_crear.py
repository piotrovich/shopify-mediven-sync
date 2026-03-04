#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
sync_crear.py

Paso 4 de la sincronización:
- Lee el archivo data/sync_state.json generado por sync_preparar.py.
- Crea en Shopify todos los productos listados en "crear"
  usando la función crear_productos_graphql_turbo del módulo principal.

Se ejecuta con:
    python sync_crear.py
"""

import os
import json

from modulos.nucleo.sync_diagnostico import crear_productos_graphql_turbo

STATE_PATH = os.path.join("data", "sync_state.json")


def cargar_estado():
    if not os.path.exists(STATE_PATH):
        raise FileNotFoundError(
            f"No se encontró {STATE_PATH}. Ejecuta primero sync_preparar.py."
        )
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    print("=== INICIO (CREAR PRODUCTOS) ===")

    estado = cargar_estado()
    crear = estado.get("crear", [])

    print(f"🆕 Productos a crear: {len(crear)}")

    if not crear:
        print("ℹ️ No hay productos para crear. Nada que hacer.")
        print("=== FIN (CREAR PRODUCTOS) ===")
        return

    resultado = crear_productos_graphql_turbo(crear)

    ok = resultado.get("ok", 0)
    err = resultado.get("errores", 0)

    print(f"✅ Creación de productos completada. OK={ok}, errores={err}")
    print("=== FIN (CREAR PRODUCTOS) ===")


if __name__ == "__main__":
    main()
