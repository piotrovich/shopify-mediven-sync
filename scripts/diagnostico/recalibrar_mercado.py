#!/usr/bin/env python
"""
scripts/diagnostico/recalibrar_mercado.py

Recalibra data/precios_mercado.json aplicando filtros de calidad MÁS
estrictos SIN volver a llamar a Serper (gratis, instantáneo).

Qué corrige (sobre los datos ya recolectados):
  1. Elimina fuentes NO chilenas (.com.ar, .com.co, .es, etc.) que
     contaminan las medianas con precios de otros mercados.
  2. Elimina fuentes que NO son comercio (Instagram, Facebook, TikTok,
     Scribd, UberEats, etc.).
  3. Re-aplica el filtro de outliers (IQR + mediana) sobre las fuentes
     chilenas limpias.
  4. Regla de dispersión: si quedan solo 2 fuentes y difieren más de
     RATIO_DISPERSION_MAX (2.5x), el estudio se considera poco confiable
     y el SKU pasa a Monopolio (precio conservador en el motor).
  5. Si quedan menos de MIN_FUENTES_VALIDAS (2) fuentes chilenas, el SKU
     pasa a Monopolio.
  6. Re-aplica el sanity check final contra el costo Mediven (mediana
     debe ser >= costo_iva * FACTOR_PISO).

Trabaja sobre el DETALLE COMPLETO de cada estudio (fuentes válidas +
descartadas por IQR en la corrida original), así que puede recuperar
fuentes chilenas que el IQR había descartado por culpa de outliers
internacionales.

Hace BACKUP del archivo original antes de sobrescribir.

Uso:
    python scripts/diagnostico/recalibrar_mercado.py
    python scripts/diagnostico/recalibrar_mercado.py --dry-run   (no escribe, solo reporta)
"""

import os
import sys
import json
import re
import shutil
import argparse
import statistics
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, BASE_DIR)

ARCHIVO_MERCADO = os.path.join(BASE_DIR, "data", "precios_mercado.json")
ARCHIVO_MEDIVEN = os.path.join(BASE_DIR, "mediven_full.json")
DIR_BACKUPS = os.path.join(BASE_DIR, "data", "backups")

# ============================================================
#   PARÁMETROS DE CALIBRACIÓN
# ============================================================
MIN_FUENTES_VALIDAS = 2
RATIO_DISPERSION_MAX = 2.5   # con 2 fuentes, si max/min supera esto → Monopolio
FACTOR_PISO_VS_COSTO = 1.10  # mediana debe ser >= costo_iva * esto

# ============================================================
#   FILTRO DE FUENTES CHILENAS
# ============================================================
# Dominios .com chilenos legítimos (no usan .cl pero son nacionales)
COM_CHILENOS_WHITELIST = {
    "buhochile.com", "araucomed.com", "farmacia.araucomed.com",
    "med-chile.com", "autotest-chile.com", "mrgreencl.com",
    "market-care.com", "tienda.market-care.com",
}

# Plataformas que NO son comercio formal (precios no confiables / ruido)
REDES_Y_NO_COMERCIO = [
    "instagram", "facebook", "tiktok", "threads", "youtube", "twitter",
    "linkedin", "pinterest", "scribd", "ubereats", "rappi",
]

# TLD internacionales explícitos → no representan mercado chileno
RE_TLD_INTL = re.compile(
    r'\.com\.(ar|co|mx|pe|br|uy|ve|ec|bo|py|gt|sv|do|pa|cr|ni|hn)\b'
    r'|\.(ar|mx|pe|br|uy|ve|ec|bo|py|es|us|pt|co)$',
    re.IGNORECASE
)


def fuente_es_chilena(nombre_o_dominio):
    """Decide si una fuente representa el mercado comercial chileno."""
    s = str(nombre_o_dominio).lower().strip()
    if not s:
        return False
    if any(rs in s for rs in REDES_Y_NO_COMERCIO):
        return False
    if RE_TLD_INTL.search(s):
        return False
    if s.endswith('.cl'):
        return True
    if s in COM_CHILENOS_WHITELIST:
        return True
    # Nombre legible mapeado (sin punto): "Cruz Verde", "Salcobrand", etc.
    if '.' not in s:
        return True
    # Cualquier otro dominio (.com no listado, etc.): rechazar (conservador)
    return False


def filtrar_outliers_iqr(precios):
    """IQR + refuerzo por mediana (misma lógica que el espía)."""
    if len(precios) < 3:
        return list(precios)
    ordenados = sorted(precios)
    mediana = statistics.median(ordenados)
    if len(ordenados) >= 4:
        q1, _, q3 = statistics.quantiles(ordenados, n=4)
        iqr = q3 - q1
        lim_inf = q1 - 1.5 * iqr
        lim_sup = q3 + 1.5 * iqr
    else:
        lim_inf = float("-inf")
        lim_sup = float("inf")
    med_inf = mediana / 3
    med_sup = mediana * 3
    return [p for p in ordenados if lim_inf <= p <= lim_sup and med_inf <= p <= med_sup]


def cargar_costos_mediven():
    """Mapa sku -> costo_neto desde mediven_full.json (para sanity check)."""
    if not os.path.exists(ARCHIVO_MEDIVEN):
        print(f"⚠️  No existe {ARCHIVO_MEDIVEN}; se omite el sanity check contra costo.")
        return {}
    with open(ARCHIVO_MEDIVEN, "r", encoding="utf-8") as f:
        productos = json.load(f)
    costos = {}
    for p in productos:
        sku = str(p.get("Codigo", "")).strip()
        try:
            costos[sku] = float(p.get("Precio", 0) or 0)
        except (TypeError, ValueError):
            costos[sku] = 0.0
    return costos


def recalibrar_estudio(detalle, costo_neto):
    """
    Recalibra un estudio a partir de su detalle completo.

    Returns:
        dict nuevo datos_mercado, o None si el SKU debe pasar a Monopolio.
    """
    # Tomar TODAS las fuentes del detalle (válidas + descartadas por IQR),
    # quedarnos solo con las chilenas.
    fuentes_chilenas = []
    for d in detalle or []:
        farmacia = d.get("farmacia", "")
        precio = d.get("precio")
        if precio is None:
            continue
        if fuente_es_chilena(farmacia):
            fuentes_chilenas.append({"farmacia": farmacia, "precio": precio})

    if len(fuentes_chilenas) < MIN_FUENTES_VALIDAS:
        return None

    # Re-aplicar IQR sobre los precios chilenos
    precios = [f["precio"] for f in fuentes_chilenas]
    validos = filtrar_outliers_iqr(precios)

    if len(validos) < MIN_FUENTES_VALIDAS:
        return None

    # Regla de dispersión cuando quedan exactamente 2 fuentes
    if len(validos) == 2 and min(validos) > 0:
        if max(validos) / min(validos) > RATIO_DISPERSION_MAX:
            return None

    # Sanity check contra costo
    mediana = int(statistics.median(validos))
    minimo = min(validos)
    if costo_neto and costo_neto > 0:
        costo_iva = costo_neto * 1.19
        if mediana < costo_iva * FACTOR_PISO_VS_COSTO:
            return None

    # Reconstruir el detalle con estados nuevos
    set_validos = list(validos)
    nuevo_detalle = []
    for f in fuentes_chilenas:
        if f["precio"] in set_validos:
            nuevo_detalle.append({**f, "estado": "🟢 Válido"})
            set_validos.remove(f["precio"])
        else:
            nuevo_detalle.append({**f, "estado": "🔴 Descartado (outlier IQR)"})

    return {
        "detalle": nuevo_detalle,
        "minimo": minimo,
        "mediana_competitiva": mediana,
        "fuentes_validas": len(validos),
    }


def main():
    parser = argparse.ArgumentParser(description="Recalibra precios_mercado.json sin gastar créditos")
    parser.add_argument("--dry-run", action="store_true", help="No escribe, solo reporta el impacto")
    args = parser.parse_args()

    if not os.path.exists(ARCHIVO_MERCADO):
        print(f"❌ No existe {ARCHIVO_MERCADO}.")
        sys.exit(1)

    print("=" * 70)
    print("RECALIBRACIÓN DE PRECIOS DE MERCADO (sin gastar créditos)")
    print("=" * 70)

    with open(ARCHIVO_MERCADO, "r", encoding="utf-8") as f:
        mercado = json.load(f)
    print(f"📋 Estudios cargados: {len(mercado)}")

    costos = cargar_costos_mediven()
    print(f"💰 Costos Mediven cargados: {len(costos)}")
    print()

    stats = {
        "ya_monopolio": 0,       # ya no tenían datos
        "siguen_validos": 0,     # mantienen estudio
        "mediana_cambio": 0,     # estudio válido pero con mediana distinta
        "nuevos_monopolio": 0,   # tenían datos y ahora no (contaminados)
        "fuentes_eliminadas": 0, # total de fuentes intl/redes removidas
    }

    nuevo_mercado = {}
    for sku, registro in mercado.items():
        fecha = registro.get("fecha")
        datos = registro.get("datos_mercado")

        if not datos:
            stats["ya_monopolio"] += 1
            nuevo_mercado[sku] = {"datos_mercado": None, "fecha": fecha}
            continue

        detalle_original = datos.get("detalle", [])
        n_fuentes_orig = len([d for d in detalle_original if fuente_es_chilena(d.get("farmacia", "")) is False])
        stats["fuentes_eliminadas"] += n_fuentes_orig

        costo_neto = costos.get(sku, 0)
        nuevo_datos = recalibrar_estudio(detalle_original, costo_neto)

        if nuevo_datos is None:
            stats["nuevos_monopolio"] += 1
            nuevo_mercado[sku] = {"datos_mercado": None, "fecha": fecha}
        else:
            stats["siguen_validos"] += 1
            mediana_vieja = datos.get("mediana_competitiva")
            if nuevo_datos["mediana_competitiva"] != mediana_vieja:
                stats["mediana_cambio"] += 1
            nuevo_mercado[sku] = {"datos_mercado": nuevo_datos, "fecha": fecha}

    # === Reporte ===
    print("=" * 70)
    print("📊 RESULTADO DE LA RECALIBRACIÓN")
    print("=" * 70)
    print(f"Estudios que ya eran Monopolio (sin cambios):  {stats['ya_monopolio']}")
    print(f"Estudios que siguen válidos:                   {stats['siguen_validos']}")
    print(f"  - de esos, con mediana CORREGIDA:            {stats['mediana_cambio']}")
    print(f"Estudios que pasan a Monopolio (contaminados): {stats['nuevos_monopolio']}")
    print(f"Fuentes internacionales/redes eliminadas:      {stats['fuentes_eliminadas']}")
    print()

    total_monopolio_final = stats["ya_monopolio"] + stats["nuevos_monopolio"]
    print(f"Monopolio total tras recalibrar: {total_monopolio_final} "
          f"({total_monopolio_final/len(mercado)*100:.1f}%)")
    print(f"Con datos válidos: {stats['siguen_validos']} "
          f"({stats['siguen_validos']/len(mercado)*100:.1f}%)")
    print()

    if args.dry_run:
        print("🔍 DRY-RUN: no se escribió nada. Quita --dry-run para aplicar.")
        return

    # === Backup + guardar ===
    os.makedirs(DIR_BACKUPS, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = os.path.join(DIR_BACKUPS, f"precios_mercado_pre_recalibrado_{ts}.json")
    shutil.copy2(ARCHIVO_MERCADO, backup)
    print(f"💾 Backup del original: {backup}")

    with open(ARCHIVO_MERCADO, "w", encoding="utf-8") as f:
        json.dump(nuevo_mercado, f, indent=2, ensure_ascii=False)
    print(f"✅ Archivo recalibrado guardado: {ARCHIVO_MERCADO}")
    print()
    print("Siguiente paso: vuelve a correr la simulación para ver el efecto:")
    print("  python scripts/diagnostico/simular_precios.py")


if __name__ == "__main__":
    main()
