#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Buscador MANUAL de imágenes (lote grande), versión Claude.

Recorre el catálogo de Shopify, toma los productos que NO están en
data/registro_imagenes.json y les busca foto con el mismo motor que usa el
robot automático (modulos/multimedia/motor_imagenes_claude.py).

Uso:
    python -m modulos.utilidades.sincronizar_imagenes            # todos los pendientes
    python -m modulos.utilidades.sincronizar_imagenes --limite 50
    python -m modulos.utilidades.sincronizar_imagenes --solo-listar

Requiere en .env: SHOP_DOMAIN, SHOPIFY_ADMIN_TOKEN, SHOPIFY_DEFAULT_IMAGE_URL,
ANTHROPIC_API_KEY (y opcionalmente CLAUDE_MODEL_IMAGENES).
"""

import argparse
from dotenv import load_dotenv

load_dotenv()

from modulos.nucleo.sync_diagnostico import shopify_graphql  # noqa: E402
from modulos.multimedia import sync_imagenes_auto as auto  # noqa: E402
from modulos.multimedia import motor_imagenes_claude as motor  # noqa: E402


def escanear_pendientes(registro):
    """Devuelve [{product_id, product_title, sku}] de productos sin entrada en el registro."""
    query_scan = """
    query($cursor: String) {
      products(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            title
            status
            variants(first: 1) { edges { node { sku } } }
          }
        }
      }
    }
    """
    cursor, has_next, pagina = None, True, 1
    pendientes = []
    while has_next:
        data = shopify_graphql(query_scan, {"cursor": cursor}, "scan_products")
        if not data or "data" not in data or not data["data"].get("products"):
            print(f"\n⚠️ Error en GraphQL al escanear: {data.get('errors') if data else 'Sin respuesta'}")
            break
        for edge in data["data"]["products"]["edges"]:
            node = edge["node"]
            if str(node.get("status", "")).lower() != "active":
                continue
            variantes = node["variants"]["edges"]
            sku = variantes[0]["node"]["sku"] if variantes else None
            if sku and sku not in registro:
                pendientes.append({
                    "product_id": node["id"].split("/")[-1],
                    "product_title": node["title"],
                    "sku": sku,
                })
        print(f"   → Escaneando página {pagina}... (pendientes: {len(pendientes)})", end="\r")
        page_info = data["data"]["products"]["pageInfo"]
        has_next, cursor = page_info["hasNextPage"], page_info["endCursor"]
        pagina += 1
    print()
    return pendientes


def main():
    parser = argparse.ArgumentParser(description="Buscador manual de imágenes con Claude")
    parser.add_argument("--limite", type=int, default=0, help="Máximo de productos a procesar (0 = todos)")
    parser.add_argument("--solo-listar", action="store_true", help="Solo muestra los pendientes, sin llamar a Claude")
    args = parser.parse_args()

    print("==================================================")
    print("🚀 BUSCADOR MANUAL DE IMÁGENES (CLAUDE)")
    print(f"   Modelo: {motor.MODELO} · visión {'ON' if motor.VERIFICAR_VISION else 'OFF'}")
    print("==================================================")

    if not motor.motor_disponible():
        print("❌ Falta ANTHROPIC_API_KEY en el .env")
        return

    registro = auto.cargar_registro()
    print("📦 Escaneando el catálogo de Shopify...")
    pendientes = escanear_pendientes(registro)
    print(f"🔍 Productos activos sin registro de imagen: {len(pendientes)}")

    if args.solo_listar or not pendientes:
        for p in pendientes[:50]:
            print(f"   · {p['sku']} — {p['product_title'][:60]}")
        if len(pendientes) > 50:
            print(f"   ... y {len(pendientes) - 50} más")
        return

    if args.limite > 0:
        pendientes = pendientes[:args.limite]

    resumen = {"ok": 0, "cuarentena": 0, "error": 0}
    for idx, p in enumerate(pendientes, 1):
        print(f"[{idx}/{len(pendientes)}]", end="")
        try:
            estado = auto.procesar_producto(p, registro)
        except motor.MotorNoDisponible as e:
            print(f"\n🛑 Motor no disponible: {e}")
            break
        resumen[estado] += 1
        if estado != "error":
            auto.guardar_registro(registro)

    auto.guardar_registro(registro)
    print(f"\n🎉 FINALIZADO | Reales: {resumen['ok']} | Genéricas: {resumen['cuarentena']} | Errores: {resumen['error']}")
    print(f"💳 Consumo Claude: {motor.resumen_uso()}")


if __name__ == "__main__":
    main()
