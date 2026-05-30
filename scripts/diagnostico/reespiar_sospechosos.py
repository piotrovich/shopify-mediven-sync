#!/usr/bin/env python
"""
scripts/diagnostico/reespiar_sospechosos.py

Re-espía productos cuya mediana de mercado es sospechosamente alta respecto
al costo (probable captura de otra presentación / pack), usando el espía
v7.1 que ahora incluye el filtro anti-pack (cluster bajo coherente).

Contexto: el espía a veces captura precios de packs (caja de 6-12) en lugar
del producto individual, inflando la mediana. Ej: un desodorante que cuesta
$4.420 con "mediana" $35.021 (2 fuentes = un pack). El filtro anti-pack v7.1
descarta esos precios cuando hay fuentes individuales en la mezcla; este
script vuelve a espiar los sospechosos para que el filtro actúe y, de paso,
intente traer más fuentes individuales.

OJO: productos cuyas TODAS las fuentes son pack (no existe oferta individual
listada) no se corrigen automáticamente y quedarán marcados para revisión.

Qué hace:
  - Filtra productos con mediana > RATIO_SOSPECHOSO × costo c/IVA.
  - Re-espía cada uno con buscar_precio_competencia (espía v7.1 anti-pack).
  - Actualiza data/precios_mercado.json (con backup previo).
  - Reporta antes/después y marca los que SIGUEN sospechosos (irrecuperables).

Lo que NO hace:
  - NO toca Shopify ni cambia precios.

Uso:
    python scripts/diagnostico/reespiar_sospechosos.py --dry-run
    python scripts/diagnostico/reespiar_sospechosos.py
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

# Mediana > este múltiplo del costo c/IVA = sospechosa de pack/otra presentación
RATIO_SOSPECHOSO = float(os.getenv("RATIO_SOSPECHOSO", "3.0"))


def cargar_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="Re-espía productos con mediana sospechosa de pack")
    parser.add_argument("--dry-run", action="store_true",
                        help="Lista los sospechosos sin llamar a Serper ni escribir")
    args = parser.parse_args()

    productos = cargar_json(ARCHIVO_MEDIVEN)
    mercado = cargar_json(ARCHIVO_MERCADO) if os.path.exists(ARCHIVO_MERCADO) else {}

    costo_por_sku = {}
    desc_por_sku = {}
    lab_por_sku = {}
    for p in productos:
        sku = str(p.get("Codigo", "")).strip()
        if not sku:
            continue
        try:
            costo_por_sku[sku] = float(p.get("Precio", 0) or 0)
        except (TypeError, ValueError):
            costo_por_sku[sku] = 0
        desc_por_sku[sku] = p.get("Descripcion", "")
        lab_por_sku[sku] = p.get("Laboratorio", "")

    # Detectar sospechosos: mediana > RATIO × costo c/IVA
    sospechosos = []
    for sku, reg in mercado.items():
        dm = reg.get("datos_mercado")
        costo = costo_por_sku.get(sku, 0)
        if not dm or costo <= 0:
            continue
        costo_iva = costo * 1.19
        med = dm.get("mediana_competitiva", 0)
        if med > costo_iva * RATIO_SOSPECHOSO:
            ratio = med / costo_iva
            sospechosos.append((sku, costo, med, ratio, dm.get("fuentes_validas", 0)))

    sospechosos.sort(key=lambda x: -x[3])  # peor ratio primero

    print("=" * 74)
    print("RE-ESPÍA DE SOSPECHOSOS DE PACK (espía v7.1 anti-pack)")
    print("=" * 74)
    print(f"Criterio: mediana > {RATIO_SOSPECHOSO}× costo c/IVA")
    print(f"Productos sospechosos: {len(sospechosos)}\n")

    if args.dry_run:
        print("🔍 DRY-RUN (no se gastó nada). Peores 40 por ratio:\n")
        for sku, costo, med, ratio, f in sospechosos[:40]:
            print(f"  {sku:<14} {ratio:>4.1f}× [{f}f] costoIVA ${costo*1.19:>9,.0f} med ${med:>10,} {str(desc_por_sku.get(sku,''))[:34]}")
        if len(sospechosos) > 40:
            print(f"  ... y {len(sospechosos) - 40} más")
        print(f"\nTotal: {len(sospechosos)}. Quita --dry-run para re-espiar.")
        return

    os.makedirs(DIR_BACKUPS, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = os.path.join(DIR_BACKUPS, f"precios_mercado_pre_reespiado_sospechosos_{ts}.json")
    if os.path.exists(ARCHIVO_MERCADO):
        shutil.copy2(ARCHIVO_MERCADO, backup)
        print(f"💾 Backup: {backup}\n")

    fecha_hoy = datetime.now().strftime("%Y-%m-%d")
    corregidos = 0          # bajaron por debajo del umbral sospechoso
    siguen_sospechosos = [] # siguen con ratio alto tras re-espía (irrecuperables)
    a_monopolio = 0

    for i, (sku, costo, med_antes, ratio_antes, f_antes) in enumerate(sospechosos, 1):
        nombre = desc_por_sku.get(sku, "")
        lab = lab_por_sku.get(sku, "")
        costo_iva = costo * 1.19

        try:
            estudio = buscar_precio_competencia(nombre, lab, costo)
        except Exception as e:
            print(f"  [{i}/{len(sospechosos)}] ⚠️  {sku} error: {e}")
            continue

        mercado[sku] = {"datos_mercado": estudio, "fecha": fecha_hoy}

        if estudio:
            med_ahora = estudio["mediana_competitiva"]
            ratio_ahora = med_ahora / costo_iva
            if ratio_ahora <= RATIO_SOSPECHOSO:
                corregidos += 1
                print(f"  [{i}/{len(sospechosos)}] ✅ {sku} CORREGIDO {ratio_antes:.1f}×→{ratio_ahora:.1f}× (${med_antes:,}→${med_ahora:,})")
            else:
                siguen_sospechosos.append((sku, ratio_ahora, med_ahora, estudio["fuentes_validas"], nombre))
                print(f"  [{i}/{len(sospechosos)}] ⚠️  {sku} sigue alto {ratio_ahora:.1f}× (${med_ahora:,}, {estudio['fuentes_validas']}f)")
        else:
            a_monopolio += 1
            print(f"  [{i}/{len(sospechosos)}] → {sku} Monopolio (sin datos confiables)")

        if i % 25 == 0:
            with open(ARCHIVO_MERCADO, "w", encoding="utf-8") as fp:
                json.dump(mercado, fp, indent=2, ensure_ascii=False)
        time.sleep(0.3)

    with open(ARCHIVO_MERCADO, "w", encoding="utf-8") as fp:
        json.dump(mercado, fp, indent=2, ensure_ascii=False)

    print("\n" + "=" * 74)
    print("📊 RESULTADO")
    print("=" * 74)
    print(f"Corregidos (ratio bajó del umbral):       {corregidos}")
    print(f"Pasaron a Monopolio:                      {a_monopolio}")
    print(f"SIGUEN sospechosos (revisar manual):      {len(siguen_sospechosos)}")

    if siguen_sospechosos:
        print(f"\n⚠️  LISTA PARA REVISIÓN MANUAL (todas sus fuentes parecen pack):")
        print(f"{'SKU':<14}{'Ratio':>7}{'Mediana':>11}{'Fuentes':>8}  Descripción")
        for sku, ratio, med, f, nombre in sorted(siguen_sospechosos, key=lambda x: -x[1]):
            print(f"{sku:<14}{ratio:>6.1f}×{med:>11,}{f:>8}  {str(nombre)[:32]}")
        # Guardar la lista en un archivo para referencia
        out_lista = os.path.join(BASE_DIR, "data", f"sospechosos_irrecuperables_{ts}.json")
        with open(out_lista, "w", encoding="utf-8") as fp:
            json.dump([{"sku": s, "ratio": round(r, 2), "mediana": m, "fuentes": f, "desc": n}
                       for s, r, m, f, n in siguen_sospechosos], fp, indent=2, ensure_ascii=False)
        print(f"\n💾 Lista guardada en: {out_lista}")

    print(f"\n✅ data/precios_mercado.json actualizado.")
    print("Siguiente paso: recalibrar (dispersión) y volver a simular precios.")


if __name__ == "__main__":
    main()
