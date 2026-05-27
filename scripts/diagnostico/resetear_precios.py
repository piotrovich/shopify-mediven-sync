"""
scripts/diagnostico/resetear_precios.py

Script CONSOLIDADO que:
  1. Introspecciona el estado actual de data/precios_mercado.json y
     data/memoria_precios.json (cuántas entradas, cuántas con datos válidos,
     antigüedad de los estudios).
  2. Hace backup con timestamp en data/backups/.
  3. Vacía los archivos (los deja como {}) para forzar reanálisis total
     con el nuevo motor.
  4. Reporta resumen.

Uso:
    python scripts/diagnostico/resetear_precios.py            # reset real
    python scripts/diagnostico/resetear_precios.py --dry-run  # solo reporta

⚠️ Hacer commit del estado actual ANTES de correr esto. El backup local se
   genera igual, pero un backup en git es más seguro.
"""

import os
import sys
import json
import shutil
from datetime import datetime
from collections import Counter

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DIR_DATA = os.path.join(BASE_DIR, "data")
DIR_BACKUPS = os.path.join(DIR_DATA, "backups")

ARCHIVO_MERCADO = os.path.join(DIR_DATA, "precios_mercado.json")
ARCHIVO_MEMORIA = os.path.join(DIR_DATA, "memoria_precios.json")

DRY_RUN = "--dry-run" in sys.argv


def cargar_json(ruta):
    if not os.path.exists(ruta):
        return {}
    with open(ruta, "r", encoding="utf-8") as f:
        return json.load(f)


def introspeccionar():
    """Reporta el estado actual ANTES de hacer cualquier cambio."""
    mercado = cargar_json(ARCHIVO_MERCADO)
    memoria = cargar_json(ARCHIVO_MEMORIA)

    total_mercado = len(mercado)
    con_datos = sum(1 for v in mercado.values() if (v or {}).get("datos_mercado"))
    sin_datos = total_mercado - con_datos

    fechas = Counter()
    for v in mercado.values():
        fechas[(v or {}).get("fecha", "sin_fecha")] += 1

    fechas_ord = sorted(fechas.items())
    primera = fechas_ord[0][0] if fechas_ord else "—"
    ultima = fechas_ord[-1][0] if fechas_ord else "—"

    total_memoria = len(memoria)
    precios = [v for v in memoria.values() if isinstance(v, (int, float)) and v > 0]
    promedio = sum(precios) / len(precios) if precios else 0

    print("=" * 60)
    print("📊 ESTADO ACTUAL")
    print("=" * 60)
    print(f"precios_mercado.json:")
    print(f"  • Total entradas:           {total_mercado:>6}")
    print(f"  • Con datos confiables:     {con_datos:>6}")
    print(f"  • Sin datos (Monopolio):    {sin_datos:>6}")
    print(f"  • Rango de fechas:          {primera}  →  {ultima}")
    print()
    print(f"memoria_precios.json:")
    print(f"  • Total entradas:           {total_memoria:>6}")
    print(f"  • Precio promedio:          ${promedio:>10,.0f}")
    print("=" * 60)
    return mercado, memoria


def hacer_backup():
    if not os.path.exists(ARCHIVO_MERCADO) and not os.path.exists(ARCHIVO_MEMORIA):
        print("⚠️  No hay archivos para respaldar.")
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dir_backup = os.path.join(DIR_BACKUPS, f"reset_{timestamp}")
    os.makedirs(dir_backup, exist_ok=True)

    for archivo in (ARCHIVO_MERCADO, ARCHIVO_MEMORIA):
        if os.path.exists(archivo):
            shutil.copy2(archivo, dir_backup)
            print(f"  ✔ Backup: {os.path.basename(archivo)}")

    return dir_backup


def vaciar_archivos():
    os.makedirs(DIR_DATA, exist_ok=True)
    for archivo in (ARCHIVO_MERCADO, ARCHIVO_MEMORIA):
        with open(archivo, "w", encoding="utf-8") as f:
            json.dump({}, f)
        print(f"  ✔ Vaciado: {os.path.basename(archivo)}")


def main():
    print()
    print("🔄 RESETEO DE PRECIOS — Farmacias LF")
    if DRY_RUN:
        print("   (modo DRY-RUN, no se hacen cambios)")
    print()

    introspeccionar()

    if DRY_RUN:
        print("\n💡 Modo dry-run. Para ejecutar el reset real, vuelve a correr SIN --dry-run.")
        return

    print("\n📦 Generando backup...")
    dir_backup = hacer_backup()
    if dir_backup:
        print(f"   📁 Backup guardado en: {dir_backup}")

    print("\n🗑️  Vaciando archivos...")
    vaciar_archivos()

    print("\n✅ Reset completo. Próximos pasos:")
    print("   1. Verifica que SERPER_API_KEY tenga cuota suficiente (~10K búsquedas).")
    print("   2. Corre 'python sync.py --dry-run' primero para revisar el plan de precios.")
    print("   3. Si todo se ve bien, corre 'python sync.py' para aplicar a Shopify.")
    print()


if __name__ == "__main__":
    main()
