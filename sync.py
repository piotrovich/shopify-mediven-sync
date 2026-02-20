#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import time
import pandas as pd
import subprocess
import crear_diccionario_ia
import subir_a_shopify

# 🔥 Para logs PRO (sin tocar la lógica)
from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from rich.progress import Progress, SpinnerColumn, BarColumn, TimeElapsedColumn, TextColumn

console = Console()

from sync_diagnostico import (
    get_mediven_inventory,
    get_shopify_products,
    normalize_shopify_products,
    calcular_precio,
    generar_excel,
    DELETE_MISSING,
)

from sync_crear import crear_productos_graphql_turbo
from sync_actualizar import graphql_bulk_update_variants
from sync_diagnostico import archive_products_graphql, bulk_update_product_basics


LOCKFILE = "sync.lock"


# ==========================================================
#   Formateo del tiempo total
# ==========================================================
def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.1f} s"
    mins = int(seconds // 60)
    secs = seconds % 60
    return f"{mins} min {secs:.1f} s"


# ==========================================================
#  LOCKFILE
# ==========================================================
def create_lock():
    if os.path.exists(LOCKFILE):
        console.print("[bold red]⚠️ Ya existe una ejecución activa (sync.lock presente). Cancelando.[/bold red]")
        exit(1)
    with open(LOCKFILE, "w") as f:
        f.write(str(int(time.time())))


def remove_lock():
    if os.path.exists(LOCKFILE):
        os.remove(LOCKFILE)


# ==========================================================
#  FLUJO PRINCIPAL — ULTRA PRO
# ==========================================================
def main():

    # 🕒 TIMER
    start_time = time.time()

    console.print(Panel.fit("🚀 [bold cyan]SINCRONIZACIÓN COMPLETA (AUTO)[/bold cyan]", style="bold magenta"))
    create_lock()

    try:
        # ======================================================
        # 1) MEDIVEN
        # ======================================================
        console.print(Rule("[bold white]📥 Cargando datos de Mediven[/bold white]"))

        with console.status("[cyan]Conectando a Mediven…[/cyan]", spinner="dots"):
            mediven_data = get_mediven_inventory()

        console.print(f"[green]✔ Mediven OK:[/green] {len(mediven_data)} productos.")

        # ======================================================
        # 2) SHOPIFY
        # ======================================================
        console.print(Rule("[bold white]📦 Cargando productos desde Shopify[/bold white]"))

        with console.status("[cyan]Descargando datos de Shopify…[/cyan]", spinner="earth"):
            shopify_products = get_shopify_products()

        console.print(f"[green]✔ Shopify OK:[/green] {len(shopify_products)} productos cargados.")

        # DataFrame creation
        df_med = pd.DataFrame(mediven_data)
        df_shop = pd.DataFrame(normalize_shopify_products(shopify_products))

        if not df_shop.empty:
            df_shop["sku"] = df_shop["sku"].astype(str).str.strip()
        else:
            df_shop["sku"] = pd.Series(dtype=str)

        df_med["Codigo"] = df_med["Codigo"].astype(str).str.strip()

        # ======================================================
        # 3) DETECCIÓN DE EXCLUIDOS (VET, CLONAZEPAM, ETC.)
        # ======================================================
        console.print(Rule("[yellow]🐾 Detectando productos excluidos[/yellow]"))

        # LISTA MAESTRA DE EXCLUSIÓN
        palabras_vet = [
            "perro", "perros",
            "gato", "gatos",
            "mascota", "veterinaria",
            "mundo animal",
            "clonazepam",
            "veterquimica"
        ]

        def es_excluido(row):
            txt = " ".join([
                str(row.get(c, "")).lower()
                for c in ["Descripcion", "Laboratorio", "AccionTerapeutica"]
            ])
            return any(p in txt for p in palabras_vet)

        df_med["EsExcluido"] = df_med.apply(es_excluido, axis=1)
        
        # Obtenemos los SKUs que Mediven tiene pero que NOSOTROS prohibimos
        skus_excluidos = set(df_med[df_med["EsExcluido"]]["Codigo"])

        console.print(f"[yellow]🐾 Productos detectados para exclusión:[/yellow] {len(skus_excluidos)}")

        # ======================================================
        # 4) DIAGNÓSTICO
        # ======================================================
        console.print(Rule("[bold magenta]📊 Generando diagnóstico[/bold magenta]"))

        crear = []
        actualizar = []
        archivar = []

        skus_med = set(df_med["Codigo"])

        # Mapeo de Shopify por SKU
        shop_by_sku = {}
        for _, row in df_shop.iterrows():
            sku = row["sku"]
            if sku and sku not in shop_by_sku:
                shop_by_sku[sku] = row

        # --- LÓGICA CREAR / ACTUALIZAR ---
        for _, row in df_med.iterrows():
            sku = row["Codigo"]
            
            # Si el producto es excluido (clonazepam, vet), NO lo creamos ni actualizamos
            if sku in skus_excluidos:
                continue

            precio_med = row.get("Precio", 0)
            nuevo_precio = calcular_precio(precio_med)

            if sku in shop_by_sku:
                shop_row = shop_by_sku[sku]
                precio_actual = float(shop_row["price"] or 0)
                nombre_actual_shopify = str(shop_row.get("product_title", ""))
                estado_actual_shopify = str(shop_row.get("status", "")).lower()

                # Generamos el nombre limpio para comparar
                from sync_diagnostico import formatear_nombre_producto
                nom_gen = formatear_nombre_producto(row)

                # COMPARAR PRECIO, NOMBRE Y ESTADO
                c_pre = abs(precio_actual - nuevo_precio) >= 1
                c_nom = nom_gen != nombre_actual_shopify
                c_status = estado_actual_shopify != "active"

                if c_pre or c_nom or c_status:
                    actualizar.append({
                        "SKU": sku,
                        "Descripcion": nom_gen,
                        "Precio_Shopify": precio_actual,
                        "Nuevo_Precio": nuevo_precio,
                        "variant_id": shop_row["variant_id"],
                        "product_id": shop_row["product_id"],
                        "actualizar_basicos": c_nom or c_status # Flag para actualizar nombre/estado
                    })
            else:
                crear.append({
                    "SKU": sku,
                    "Descripcion": row.get("Descripcion", ""),
                    "Precio": nuevo_precio,
                    "Stock": 100,
                })

        # --- LÓGICA ARCHIVAR (ELIMINAR) ---
        for _, row in df_shop.iterrows():
            sku = row["sku"]
            if not sku:
                continue
            
            # ELIMINAR SI:
            # 1. El SKU está en la lista de excluidos (ej: clonazepam existe en mediven, pero lo prohibimos).
            # 2. El SKU NO existe en Mediven.
            
            es_prohibido = sku in skus_excluidos
            no_existe_mediven = sku not in skus_med

            if es_prohibido or no_existe_mediven:
                motivo = "Excluido/Prohibido" if es_prohibido else "No existe en Mediven"
                archivar.append({
                    "SKU": sku,
                    "product_id": row["product_id"],
                    "Descripcion": row.get("product_title", ""),
                    "Motivo": motivo
                })

        # Panel de diagnóstico
        console.print(
            Panel.fit(
                f"[bold green]CREAR:[/bold green] {len(crear)}\n"
                f"[bold yellow]ACTUALIZAR:[/bold yellow] {len(actualizar)}\n"
                f"[bold red]ARCHIVAR:[/bold red] {len(archivar)}",
                title="📊 DIAGNÓSTICO FINAL",
                style="magenta"
            )
        )

        # ======================================================
        # 5) EXCEL
        # ======================================================
        console.print(Rule("[bold white]📄 Generando Excel[/bold white]"))

        with console.status("[cyan]Generando archivo Excel…[/cyan]", spinner="aesthetic"):
            generar_excel(crear, actualizar, archivar, mediven_data)

        console.print("[green]✔ Excel generado.[/green]")

        # ======================================================
        # 6) APLICAR CAMBIOS
        # ======================================================
        console.print(Rule("[bold cyan]⚙️ Aplicando cambios en Shopify[/bold cyan]"))

        # ARCHIVAR (ELIMINAR)
        if archivar:
            # Forzamos eliminación de excluidos incluso si DELETE_MISSING es false, 
            # pero típicamente DELETE_MISSING controla todo borrado.
            # Asumiremos que si está en 'archivar', queremos borrarlo.
            if DELETE_MISSING:
                with console.status("[red]Eliminando productos excluidos/obsoletos…[/red]"):
                    archive_products_graphql(archivar)
            else:
                console.print("[yellow]ℹ DELETE_MISSING=false — no se eliminarán productos (aunque sean excluidos).[/yellow]")

        # ACTUALIZAR BÁSICOS (TÍTULO Y REACTIVAR ESTADO)
        prods_basicos_nuevo = [p for p in actualizar if p.get("actualizar_basicos")]
        if prods_basicos_nuevo:
            with console.status("[yellow]Actualizando nombres y reactivando estado…[/yellow]"):
                 bulk_update_product_basics(prods_basicos_nuevo)

        # ACTUALIZAR PRECIOS
        if actualizar:
            with console.status("[yellow]Actualizando variantes (precios)…[/yellow]"):
                graphql_bulk_update_variants(actualizar)

        # CREAR
        if crear:
            with console.status("[green]Creando productos nuevos…[/green]"):
                crear_productos_graphql_turbo(crear)

        console.print("[bold green]✔ Cambios aplicados correctamente[/bold green]")

        # ======================================================
        # 7) REMOVE TAX (sin parpadeo)
        # ======================================================
        console.print(Rule("[bold magenta]🔥 ELIMINANDO IMPUESTOS (POST-SYNC)[/bold magenta]"))

        console.print("[cyan]⏳ Ejecutando remove_tax_all_variants.py…[/cyan]")
        subprocess.run(["python", "remove_tax_all_variants.py"])
        console.print("[bold green]✔ Impuestos procesados[/bold green]")

        # ======================================================
        # 8) MOTOR DE IA (NUEVO)
        # ======================================================
        console.print(Rule("[bold magenta]🧠 VERIFICANDO CONTENIDO FALTANTE (IA)[/bold magenta]"))
        
        # Esto ejecutará la lógica de detección de nuevos productos
        # y generará las descripciones solo para lo que falte.
        try:
            crear_diccionario_ia.main()
        except Exception as e:
            console.print(f"[bold red]❌ Error en el módulo de IA: {e}[/bold red]")

        # ======================================================
        # 8.5) ACTUALIZAR SHOPIFY (El paso final)
        # ======================================================
        console.print(Rule("[bold cyan]🎨 ACTUALIZANDO PESTAÑAS EN SHOPIFY[/bold cyan]"))
        try:
            subir_a_shopify.main()
        except Exception as e:
            console.print(f"[bold red]❌ Error actualizando Shopify: {e}[/bold red]")

        # ======================================================
        # 9) FIN + TIMER
        # ======================================================
        total_time = time.time() - start_time

        console.print(
            Panel.fit(
                f"🎉 [bold green]PROCESO COMPLETO EXITOSO[/bold green]\n"
                f"🕒 Tiempo total: [cyan]{format_time(total_time)}[/cyan]",
                style="bold blue",
                title="FIN"
            )
        )

    finally:
        remove_lock()


if __name__ == "__main__":
    main()
