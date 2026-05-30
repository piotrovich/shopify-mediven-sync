#!/usr/bin/env python
"""
scripts/diagnostico/reespiar_premium.py

Re-espía SOLO los productos de alto costo (premium) con el espía corregido
(v7), para recapturar sus precios de mercado SIN el sesgo del piso antiguo.

Contexto: hasta v5, el espía descartaba precios de mercado bajo
`costo_iva × 1.10`. En productos premium donde las grandes cadenas compran
más barato que Mediven, eso eliminaba los precios competitivos y dejaba
medianas infladas (o Monopolio). El v7 corrige esto con un piso de sanidad
laxo. Este script vuelve a espiar la franja premium para limpiar esos datos.

Qué hace:
  - Filtra productos con costo c/IVA > UMBRAL_PREMIUM (default $40.000).
  - Re-espía cada uno con buscar_precio_competencia (espía v7).
  - Actualiza data/precios_mercado.json (con backup previo).
  - Reporta antes/después de cada mediana para que se vea el efecto.

Lo que NO hace:
  - NO toca Shopify (solo actualiza el JSON de datos de mercado).
  - NO cambia precios. Eso es decisión posterior (ajuste del motor).

Costo aproximado: ~1 a 2 créditos Serper por producto (~221 productos →
~250-450 créditos). Trivial frente a los 28.000+ disponibles.

Uso:
    python scripts/diagnostico/reespiar_premium.py --dry-run   (lista qué re-espiaría, sin gastar créditos)
    python scripts/diagnostico/reespiar_premium.py             (re-espía y guarda)
"""

import os
import sys
import json
import time
import shutil
import argparse
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, BASE_DIR)

from modulos.finanzas.espia_precios import buscar_precio_competencia

ARCHIVO_MEDIVEN = os.path.join(BASE_DIR, "mediven_full.json")
ARCHIVO_MERCADO = os.path.join(BASE_DIR, "data", "precios_mercado.json")
DIR_BACKUPS = os.path.join(BASE_DIR, "data", "backups")

# Umbral premium: costo c/IVA > $40.000  →  costo neto > ~33.613
UMBRAL_PREMIUM_COSTO_IVA = int(os.getenv("UMBRAL_PREMIUM", "40000"))


def cargar_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="Re-espía la franja premium con el espía v7")
    parser.add_argument("--dry-run", action="store_true",
                        help="Lista los productos que re-espiaría, sin llamar a Serper ni escribir")
    args = parser.parse_args()

    if not os.path.exists(ARCHIVO_MEDIVEN):
        print(f"❌ No existe {ARCHIVO_MEDIVEN}.")
        sys.exit(1)

    productos = cargar_json(ARCHIVO_MEDIVEN)
    mercado = cargar_json(ARCHIVO_MERCADO) if os.path.exists(ARCHIVO_MERCADO) else {}

    # Filtrar premium por costo c/IVA
    premium = []
    for p in productos:
        sku = str(p.get("Codigo", "")).strip()
        if not sku:
            continue
        try:
            costo_neto = float(p.get("Precio", 0) or 0)
        except (TypeError, ValueError):
            costo_neto = 0
        costo_iva = costo_neto * 1.19
        if costo_iva > UMBRAL_PREMIUM_COSTO_IVA:
            premium.append((sku, p, costo_neto))

    print("=" * 72)
    print("RE-ESPÍA DE PRODUCTOS PREMIUM (espía v7, sin sesgo de piso)")
    print("=" * 72)
    print(f"Umbral premium: costo c/IVA > ${UMBRAL_PREMIUM_COSTO_IVA:,}")
    print(f"Productos premium a re-espiar: {len(premium)}")
    print()

    if args.dry_run:
        print("🔍 DRY-RUN: estos son los productos que se re-espiarían (no se gastó nada):\n")
        for sku, p, costo_neto in premium[:40]:
            estado = "Monopolio" if not (mercado.get(sku) or {}).get("datos_mercado") else \
                     f"mediana ${mercado[sku]['datos_mercado'].get('mediana_competitiva'):,}"
            print(f"  {sku:<14} c/IVA ${costo_neto*1.19:>10,.0f}  [{estado}]  {str(p.get('Descripcion',''))[:40]}")
        if len(premium) > 40:
            print(f"  ... y {len(premium) - 40} más")
        print(f"\nTotal: {len(premium)} productos. Quita --dry-run para re-espiar.")
        return

    # Backup antes de tocar nada
    os.makedirs(DIR_BACKUPS, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = os.path.join(DIR_BACKUPS, f"precios_mercado_pre_reespiado_premium_{ts}.json")
    if os.path.exists(ARCHIVO_MERCADO):
        shutil.copy2(ARCHIVO_MERCADO, backup)
        print(f"💾 Backup: {backup}\n")

    fecha_hoy = datetime.now().strftime("%Y-%m-%d")
    bajaron = subieron = nuevos_con_datos = siguen_monopolio = 0

    for i, (sku, p, costo_neto) in enumerate(premium, 1):
        nombre = p.get("Descripcion", "")
        lab = p.get("Laboratorio", "")
        mediana_antes = (mercado.get(sku) or {}).get("datos_mercado", {})
        mediana_antes = mediana_antes.get("mediana_competitiva") if mediana_antes else None

        try:
            estudio = buscar_precio_competencia(nombre, lab, costo_neto)
        except Exception as e:
            print(f"  [{i}/{len(premium)}] ⚠️  {sku} error: {e}")
            continue

        mercado[sku] = {"datos_mercado": estudio, "fecha": fecha_hoy}

        if estudio:
            med_ahora = estudio["mediana_competitiva"]
            if mediana_antes is None:
                nuevos_con_datos += 1
                cambio = "🆕 salió de Monopolio"
            elif med_ahora < mediana_antes:
                bajaron += 1
                cambio = f"↓ ${mediana_antes:,} → ${med_ahora:,}"
            elif med_ahora > mediana_antes:
                subieron += 1
                cambio = f"↑ ${mediana_antes:,} → ${med_ahora:,}"
            else:
                cambio = "= sin cambio"
            print(f"  [{i}/{len(premium)}] {sku} {cambio} ({estudio['fuentes_validas']} fuentes)")
        else:
            if mediana_antes is None:
                siguen_monopolio += 1
            print(f"  [{i}/{len(premium)}] {sku} → Monopolio (sin datos confiables)")

        # Guardado incremental cada 25 y pausa suave para no saturar Serper
        if i % 25 == 0:
            with open(ARCHIVO_MERCADO, "w", encoding="utf-8") as f:
                json.dump(mercado, f, indent=2, ensure_ascii=False)
        time.sleep(0.3)

    # Guardado final
    with open(ARCHIVO_MERCADO, "w", encoding="utf-8") as f:
        json.dump(mercado, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 72)
    print("📊 RESULTADO DEL RE-ESPÍA PREMIUM")
    print("=" * 72)
    print(f"Medianas que BAJARON (estaban infladas por el sesgo): {bajaron}")
    print(f"Medianas que SUBIERON:                                {subieron}")
    print(f"Salieron de Monopolio (ahora con datos):              {nuevos_con_datos}")
    print(f"Siguen en Monopolio (sin presencia online):           {siguen_monopolio}")
    print(f"\n✅ data/precios_mercado.json actualizado.")
    print("\nSiguiente paso: simular escenarios de margen sobre estos datos limpios.")


if __name__ == "__main__":
    main()
