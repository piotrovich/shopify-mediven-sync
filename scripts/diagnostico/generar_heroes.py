"""
scripts/diagnostico/generar_heroes.py

Diagnóstico: muestra qué SKUs serían detectados como HÉROES (loss leaders)
en la próxima corrida del sync.

NO modifica nada. Solo lee mediven_full.json y aplica la lógica de detección
que vive en modulos/finanzas/segmentacion.py (fuente única de verdad).

Uso:
    python scripts/diagnostico/generar_heroes.py

REQUIERE que exista mediven_full.json.
"""

import os
import sys
import json

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, BASE_DIR)

from modulos.finanzas.segmentacion import (
    CANDIDATOS_HEROES,
    buscar_candidato,
    precargar_heroes_desde_catalogo,
)

MEDIVEN_FULL = os.path.join(BASE_DIR, "mediven_full.json")


def main():
    if not os.path.exists(MEDIVEN_FULL):
        print(f"❌ Falta {MEDIVEN_FULL}.")
        print("   Corre primero: python sync.py --dry-run")
        print("   O directamente:")
        print('   python -c "from modulos.nucleo.sync_diagnostico import get_mediven_inventory; get_mediven_inventory()"')
        return

    with open(MEDIVEN_FULL, "r", encoding="utf-8") as f:
        productos = json.load(f)

    print(f"📋 Analizando {len(productos)} productos Mediven con {len(CANDIDATOS_HEROES)} categorías de héroes...")
    print("=" * 70)

    total_matches = 0
    sin_matches = []

    for cand in CANDIDATOS_HEROES:
        seleccionados = buscar_candidato(productos, cand)

        if not seleccionados:
            sin_matches.append(cand["nombre"])
            continue

        print(f"\n✓ {cand['nombre']}: {len(seleccionados)} seleccionado(s)")
        for m in seleccionados:
            sku = str(m.get("Codigo", ""))
            desc = m.get("Descripcion", "")
            lab = m.get("Laboratorio", "")
            precio = float(m.get("Precio", 0) or 0)
            print(f"    [{sku}] {desc[:55]} · {lab[:20]} · costo: ${precio:,.0f}")
            total_matches += 1

    # Consolidamos (incluye overrides manuales de heroes.json)
    total_real = precargar_heroes_desde_catalogo(productos)
    overrides_manuales = total_real - total_matches

    print()
    print("=" * 70)
    print(f"📊 RESUMEN")
    print(f"   Detectados automáticamente: {total_matches}")
    print(f"   Overrides manuales (heroes.json): {max(0, overrides_manuales)}")
    print(f"   TOTAL héroes activos: {total_real}")
    print(f"   Categorías sin matches: {len(sin_matches)}")
    if sin_matches:
        print(f"   ⚠️  Sin matches (Mediven no tiene este producto o cambió la descripción):")
        for n in sin_matches:
            print(f"        · {n}")

    print()
    print("💡 Para agregar manualmente algún SKU que no calce con los patrones,")
    print("   edita data/heroes.json con formato: [\"SKU_1\", \"SKU_2\", ...]")


if __name__ == "__main__":
    main()
