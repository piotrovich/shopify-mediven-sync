#!/usr/bin/env python
"""
scripts/diagnostico/simular_precios.py

Simulación del motor de precios sobre el catálogo COMPLETO.
NO modifica Shopify. NO ejecuta el espía. Solo lee datos ya almacenados
y aplica la lógica del motor para generar un Excel revisable.

Prerrequisitos:
    1. Debe existir mediven_full.json (catálogo Mediven actualizado).
    2. Debe existir data/precios_mercado.json con estudios del espía.
       Si no existe, todo cae en estrategia 'Monopolio'.
    3. Conexión a Shopify (para descargar precios actuales).
       Después de la primera corrida se guarda en shopify_full.json
       como cache. Para refrescar: --refresh-shopify.

Uso:
    python scripts/diagnostico/simular_precios.py
    python scripts/diagnostico/simular_precios.py --refresh-shopify

Output:
    data/simulacion_precios_YYYYMMDD_HHMMSS.xlsx con varias hojas:
        - Todos: catálogo completo
        - Heroes / Alto_costo / Regular: filtrados por segmento
        - Por Estrategia: agregado para análisis rápido
        - ⚠️ Margen Bajo: SKUs con margen <5% (revisión obligatoria)
        - ⚠️ Sin Datos Espía: SKUs en estrategia Monopolio
"""

import os
import sys
import json
import argparse
from datetime import datetime

import pandas as pd

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, BASE_DIR)

from modulos.nucleo.sync_diagnostico import (
    get_shopify_products,
    normalize_shopify_products,
)
from modulos.finanzas.precios import calcular_precio_final
from modulos.finanzas.segmentacion import (
    precargar_heroes_desde_catalogo,
    clasificar_producto,
)

MEDIVEN_FULL = os.path.join(BASE_DIR, "mediven_full.json")
PRECIOS_MERCADO = os.path.join(BASE_DIR, "data", "precios_mercado.json")
SHOPIFY_CACHE = os.path.join(BASE_DIR, "shopify_full.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "data")

# Mismas exclusiones que sync.py
PALABRAS_EXCLUIDAS = [
    "perro", "perros", "gato", "gatos", "mascota", "veterinaria",
    "mundo animal", "clonazepam", "veterquimica",
]


def cargar_mediven():
    if not os.path.exists(MEDIVEN_FULL):
        print(f"❌ Falta {MEDIVEN_FULL}.")
        print("   Genera el catálogo primero con:")
        print('   python -c "from modulos.nucleo.sync_diagnostico import get_mediven_inventory; get_mediven_inventory()"')
        sys.exit(1)
    with open(MEDIVEN_FULL, "r", encoding="utf-8") as f:
        return json.load(f)


def cargar_precios_mercado():
    if not os.path.exists(PRECIOS_MERCADO):
        print(f"⚠️  No existe {PRECIOS_MERCADO}.")
        print("   Sin datos del espía, TODO caerá en estrategia Monopolio.")
        print("   Corre primero: python -m modulos.finanzas.repesca_precios")
        respuesta = input("\n¿Continuar igual? (s/N): ").strip().lower()
        if respuesta != "s":
            sys.exit(0)
        return {}
    with open(PRECIOS_MERCADO, "r", encoding="utf-8") as f:
        return json.load(f)


def cargar_shopify(forzar_descarga=False):
    if not forzar_descarga and os.path.exists(SHOPIFY_CACHE):
        print(f"📦 Usando cache local: {SHOPIFY_CACHE}")
        with open(SHOPIFY_CACHE, "r", encoding="utf-8") as f:
            return json.load(f)
    print("📦 Descargando productos de Shopify (puede tardar 2-3 min)...")
    shopify_raw = get_shopify_products()
    normalized = normalize_shopify_products(shopify_raw)
    with open(SHOPIFY_CACHE, "w", encoding="utf-8") as f:
        json.dump(normalized, f, ensure_ascii=False)
    print(f"💾 Cache guardado en {SHOPIFY_CACHE}")
    return normalized


def es_excluido(prod):
    txt = " ".join([
        str(prod.get(c, "")).lower()
        for c in ["Descripcion", "Laboratorio", "AccionTerapeutica"]
    ])
    return any(p in txt for p in PALABRAS_EXCLUIDAS)


def categorizar_estrategia(estrategia_raw):
    """Agrupa las estrategias raw en categorías limpias para el reporte."""
    e = estrategia_raw.lower()
    if "loss leader" in e:
        return "Loss Leader (Héroe)"
    if "muralla" in e or "refuerzo" in e:
        return "Muralla de Rescate"
    if "francotirador" in e:
        return "Francotirador"
    if "alto costo" in e and "mediana" in e:
        return "Alto Costo (Mediana)"
    if "alto costo" in e and "muralla" in e:
        return "Alto Costo (Muralla)"
    if "alto costo" in e:
        return "Alto Costo (Monopolio)"
    if "monopolio" in e:
        return "Monopolio (Sin datos)"
    if "sin costo" in e:
        return "Sin Costo"
    return estrategia_raw


def main():
    parser = argparse.ArgumentParser(description="Simulación del motor de precios")
    parser.add_argument("--refresh-shopify", action="store_true",
                        help="Forzar descarga de Shopify ignorando cache")
    parser.add_argument("--solo-segmento", choices=["heroe", "alto_costo", "regular"],
                        help="Filtrar solo un segmento para análisis rápido")
    args = parser.parse_args()

    print("=" * 70)
    print("SIMULACIÓN DE PRECIOS")
    print("=" * 70)

    # === 1. CARGAR DATOS ===
    productos_mediven = cargar_mediven()
    print(f"📋 Mediven: {len(productos_mediven)} productos")

    precios_mercado = cargar_precios_mercado()
    print(f"🌐 Precios mercado: {len(precios_mercado)} SKUs con datos del espía")

    shopify_data = cargar_shopify(args.refresh_shopify)
    print(f"🛒 Shopify: {len(shopify_data)} productos cargados")

    # Indexar Shopify por SKU
    shop_by_sku = {}
    for prod in shopify_data:
        sku = str(prod.get("sku", "")).strip()
        if sku and sku not in shop_by_sku:
            shop_by_sku[sku] = prod

    # === 2. PRECARGAR HÉROES ===
    n_heroes = precargar_heroes_desde_catalogo(productos_mediven)
    print(f"🎯 Héroes detectados automáticamente: {n_heroes}")
    print()

    # === 3. PROCESAR CADA PRODUCTO ===
    filas = []
    contadores = {
        "excluidos": 0,
        "heroe": 0, "alto_costo": 0, "regular": 0,
    }
    estrategia_count = {}

    for prod in productos_mediven:
        sku = str(prod.get("Codigo", "")).strip()
        if not sku:
            continue

        if es_excluido(prod):
            contadores["excluidos"] += 1
            continue

        descripcion = prod.get("Descripcion", "")
        laboratorio = prod.get("Laboratorio", "")
        accion = prod.get("AccionTerapeutica", "")
        costo_neto = float(prod.get("Precio", 0) or 0)

        # Segmento
        segmento = clasificar_producto(
            sku=sku, descripcion=descripcion, costo_neto=costo_neto,
            laboratorio=laboratorio, accion_terapeutica=accion,
        )
        contadores[segmento] += 1

        # Filtro opcional por segmento
        if args.solo_segmento and segmento != args.solo_segmento:
            continue

        # Datos del espía
        datos_sku = precios_mercado.get(sku)

        # Precio propuesto (pasamos el segmento)
        precio_propuesto, estrategia_raw = calcular_precio_final(
            costo_neto, datos_sku, segmento=segmento,
        )
        categoria_estrategia = categorizar_estrategia(estrategia_raw)
        estrategia_count[categoria_estrategia] = estrategia_count.get(categoria_estrategia, 0) + 1

        # Datos mercado (vienen anidados en datos_sku["datos_mercado"])
        datos_mercado = datos_sku.get("datos_mercado") if datos_sku else None
        if datos_mercado:
            minimo = datos_mercado.get("minimo", 0)
            mediana = datos_mercado.get("mediana_competitiva", 0)
            fuentes = datos_mercado.get("fuentes_validas", 0)
            detalle = datos_mercado.get("detalle", [])
            farmacias_validas = [
                d for d in detalle if "🟢" in d.get("estado", "")
            ]
            farmacias_lista = " | ".join([
                f"{d['farmacia']}=${d['precio']:,}"
                for d in farmacias_validas
            ])
        else:
            minimo = mediana = fuentes = 0
            farmacias_lista = ""

        # Precio actual en Shopify
        shop_prod = shop_by_sku.get(sku)
        if shop_prod:
            precio_actual = float(shop_prod.get("price", 0) or 0)
            existe_en_shopify = True
        else:
            precio_actual = 0
            existe_en_shopify = False

        delta_abs = (precio_propuesto - precio_actual) if precio_actual > 0 else 0
        delta_pct = (delta_abs / precio_actual * 100) if precio_actual > 0 else 0

        # Margen neto
        costo_con_iva = costo_neto * 1.19
        if precio_propuesto > 0:
            margen_neto_pct = (precio_propuesto - costo_con_iva) / precio_propuesto * 100
        else:
            margen_neto_pct = 0

        # Ratio vs mediana
        ratio_mediana = (precio_propuesto / mediana * 100) if mediana > 0 else 0

        filas.append({
            "SKU": sku,
            "Descripcion": descripcion,
            "Laboratorio": laboratorio,
            "Costo Neto": int(costo_neto),
            "Costo c/IVA": int(costo_con_iva),
            "Segmento": segmento,
            "Estrategia": categoria_estrategia,
            "Fuentes Espía": fuentes,
            "Mínimo Mercado": int(minimo),
            "Mediana Mercado": int(mediana),
            "Precio Propuesto": int(precio_propuesto),
            "En Shopify": "Sí" if existe_en_shopify else "No",
            "Precio Actual Shopify": int(precio_actual),
            "Δ $": int(delta_abs),
            "Δ %": round(delta_pct, 1),
            "Margen Neto %": round(margen_neto_pct, 1),
            "Ratio vs Mediana %": round(ratio_mediana, 1),
            "Farmacias Espía": farmacias_lista,
            "Estrategia Detalle": estrategia_raw,
        })

    # === 4. CONVERTIR A DATAFRAME Y GENERAR EXCEL ===
    df = pd.DataFrame(filas)

    if df.empty:
        print("⚠️  No quedaron filas tras los filtros.")
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre = f"simulacion_precios_{ts}"
    if args.solo_segmento:
        nombre += f"_{args.solo_segmento}"
    output = os.path.join(OUTPUT_DIR, f"{nombre}.xlsx")

    print(f"\n📝 Generando Excel: {output}")
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Hoja principal
        df.to_excel(writer, sheet_name="Todos", index=False)

        # Hoja por segmento
        for seg in ["heroe", "alto_costo", "regular"]:
            df_seg = df[df["Segmento"] == seg]
            if not df_seg.empty:
                df_seg.to_excel(writer, sheet_name=seg.title(), index=False)

        # Agregado por estrategia
        df_estrat = df.groupby(["Estrategia"]).agg(
            Cantidad=("SKU", "count"),
            Margen_Neto_Promedio_pct=("Margen Neto %", "mean"),
            Delta_Promedio_pct=("Δ %", "mean"),
            Precio_Promedio=("Precio Propuesto", "mean"),
        ).round(2).reset_index().sort_values("Cantidad", ascending=False)
        df_estrat.to_excel(writer, sheet_name="Por Estrategia", index=False)

        # CASOS CRÍTICOS

        # Margen bajo (<5%) — revisar urgente
        df_margen_bajo = df[(df["Margen Neto %"] < 5) & (df["Precio Propuesto"] > 0)].copy()
        if not df_margen_bajo.empty:
            df_margen_bajo = df_margen_bajo.sort_values("Margen Neto %")
            df_margen_bajo.to_excel(writer, sheet_name="⚠️ Margen Bajo", index=False)

        # SKUs sin datos del espía (Monopolio)
        df_sin_datos = df[df["Fuentes Espía"] == 0].copy()
        if not df_sin_datos.empty:
            df_sin_datos.to_excel(writer, sheet_name="⚠️ Sin Datos Espia", index=False)

        # Cambios fuertes vs Shopify actual (delta >20%)
        df_cambios_fuertes = df[(df["Δ %"].abs() > 20) & (df["En Shopify"] == "Sí")].copy()
        if not df_cambios_fuertes.empty:
            df_cambios_fuertes = df_cambios_fuertes.sort_values("Δ %", key=abs, ascending=False)
            df_cambios_fuertes.to_excel(writer, sheet_name="⚠️ Cambios Grandes", index=False)

    # === 5. RESUMEN POR CONSOLA ===
    print()
    print("=" * 70)
    print("📊 RESUMEN")
    print("=" * 70)
    total = len(filas)
    print(f"Total procesados: {total}")
    print(f"Excluidos (vet/clonazepam): {contadores['excluidos']}")
    print()
    print("SEGMENTOS:")
    print(f"  🎯 Héroes:     {contadores['heroe']:>5} ({contadores['heroe']/total*100:.1f}%)")
    print(f"  💎 Alto costo: {contadores['alto_costo']:>5} ({contadores['alto_costo']/total*100:.1f}%)")
    print(f"  📦 Regular:    {contadores['regular']:>5} ({contadores['regular']/total*100:.1f}%)")
    print()
    print("ESTRATEGIAS APLICADAS:")
    for est, n in sorted(estrategia_count.items(), key=lambda x: -x[1]):
        pct = n / total * 100
        print(f"  {est:<28}: {n:>5} ({pct:.1f}%)")
    print()
    print("CASOS PARA REVISIÓN:")
    n_margen_bajo = len(df[(df["Margen Neto %"] < 5) & (df["Precio Propuesto"] > 0)])
    n_sin_datos = len(df[df["Fuentes Espía"] == 0])
    n_cambios = len(df[(df["Δ %"].abs() > 20) & (df["En Shopify"] == "Sí")])
    print(f"  ⚠️  Margen <5%:           {n_margen_bajo}")
    print(f"  ⚠️  Sin datos espía:      {n_sin_datos}")
    print(f"  ⚠️  Cambios fuertes >20%: {n_cambios}")
    print()
    print(f"💾 Excel completo: {output}")
    print()
    print("Próximos pasos:")
    print("  1. Abre el Excel y revisa la hoja 'Por Estrategia' para vista general.")
    print("  2. Revisa '⚠️ Margen Bajo' y '⚠️ Cambios Grandes' (lo más crítico).")
    print("  3. Si algo se ve raro, ajustamos la lógica y re-corremos esta simulación")
    print("     (NO consume créditos Serper porque usa precios_mercado.json existente).")


if __name__ == "__main__":
    main()
