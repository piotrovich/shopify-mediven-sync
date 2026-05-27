"""
Repesca de precios v2.

Cambios estructurales:
  1. Refresca SKUs con estudios > N días (default 30). El espía viejo solo
     procesaba SKUs nuevos; los viejos quedaban congelados para siempre.
  2. Reintenta SKUs con datos_mercado=None (Monopolios sin datos): vale la
     pena volver a buscarlos cada cierto tiempo por si aparecen.
  3. Pasa el costo neto Mediven al espía para validar plausibilidad de los
     precios capturados.
  4. Variables de entorno configurables:
       DIAS_REFRESH (default 30)  → antigüedad máxima del estudio
       LIMITE_DIARIO (default 800) → tope de búsquedas por corrida
       GIT_AUTOSAVE (default false) → si commitea+pushea el JSON cada 100
  5. Prioridad: nuevos > nulos > viejos. Si el límite se llena, lo nuevo
     gana siempre.
"""

import os
import json
import time
import subprocess
from datetime import datetime, timedelta
from dotenv import load_dotenv

from modulos.finanzas.espia_precios import buscar_precio_competencia

from rich.console import Console
from rich.panel import Panel

console = Console()

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv(os.path.join(BASE_DIR, ".env"))

ARCHIVO_MEDIVEN = os.path.join(BASE_DIR, "mediven_full.json")
ARCHIVO_MERCADO = os.path.join(BASE_DIR, "data", "precios_mercado.json")

DIAS_REFRESH = int(os.getenv("DIAS_REFRESH", "30"))
LIMITE_DIARIO = int(os.getenv("LIMITE_DIARIO", "800"))
GIT_AUTOSAVE = os.getenv("GIT_AUTOSAVE", "false").lower() == "true"


def _guardar(precios_mercado):
    os.makedirs(os.path.dirname(ARCHIVO_MERCADO), exist_ok=True)
    with open(ARCHIVO_MERCADO, "w", encoding="utf-8") as f:
        json.dump(precios_mercado, f, indent=2, ensure_ascii=False)


def _commitear_a_github(nuevos):
    if not GIT_AUTOSAVE:
        return
    try:
        subprocess.run(["git", "config", "--global", "user.name", "Robot-Espia"], check=False)
        subprocess.run(["git", "config", "--global", "user.email", "robot@espia.com"], check=False)
        subprocess.run(["git", "add", ARCHIVO_MERCADO], check=False)
        subprocess.run(["git", "commit", "-m", f"Auto-save espia: {nuevos} productos"], check=False)
        subprocess.run(["git", "push"], check=False)
    except Exception:
        pass


def _es_estudio_viejo(info, dias_max):
    fecha_str = (info or {}).get("fecha")
    if not fecha_str:
        return True
    try:
        fecha = datetime.strptime(fecha_str, "%Y-%m-%d")
    except ValueError:
        return True
    return (datetime.now() - fecha) > timedelta(days=dias_max)


def _construir_cola(productos_mediven, precios_mercado):
    """
    Construye la lista de SKUs a investigar priorizada:
      1. NUEVOS: no aparecen en precios_mercado.
      2. NULOS: tienen entrada pero datos_mercado=None (Monopolio sin datos).
      3. VIEJOS: estudio con más de DIAS_REFRESH días.
    """
    info_por_sku = {str(p.get("Codigo", "")): p for p in productos_mediven if p.get("Codigo")}
    skus_mediven = set(info_por_sku.keys())
    skus_mercado = set(precios_mercado.keys())

    nuevos = sorted(skus_mediven - skus_mercado)
    nulos = sorted([
        s for s in skus_mediven & skus_mercado
        if not (precios_mercado.get(s) or {}).get("datos_mercado")
        and not _es_estudio_viejo(precios_mercado.get(s), DIAS_REFRESH)
    ])
    viejos = sorted([
        s for s in skus_mediven & skus_mercado
        if _es_estudio_viejo(precios_mercado.get(s), DIAS_REFRESH)
    ])

    cola = []
    for s in nuevos + nulos + viejos:
        cola.append((s, info_por_sku[s]))

    return cola, len(nuevos), len(nulos), len(viejos)


def ejecutar_repesca_diaria():
    console.print(Panel.fit("🔍 [bold cyan]ESPÍA DE PRECIOS v2 (refresco programado)[/bold cyan]"))

    if not os.path.exists(ARCHIVO_MEDIVEN):
        console.print("[red]❌ Falta mediven_full.json[/red]")
        return

    with open(ARCHIVO_MEDIVEN, "r", encoding="utf-8") as f:
        productos_mediven = json.load(f)

    precios_mercado = {}
    if os.path.exists(ARCHIVO_MERCADO):
        with open(ARCHIVO_MERCADO, "r", encoding="utf-8") as f:
            precios_mercado = json.load(f)

    cola, n_nuevos, n_nulos, n_viejos = _construir_cola(productos_mediven, precios_mercado)

    console.print(
        f"[yellow]📋 Cola:[/yellow] [green]{n_nuevos} nuevos[/green] · "
        f"[cyan]{n_nulos} nulos a reintentar[/cyan] · "
        f"[magenta]{n_viejos} estudios > {DIAS_REFRESH} días[/magenta]"
    )

    if not cola:
        console.print("[green]✔ Nada que espiar. Todos los estudios están al día.[/green]")
        return

    cola = cola[:LIMITE_DIARIO]
    console.print(f"[yellow]⚡ Procesando hasta {len(cola)} productos (tope diario: {LIMITE_DIARIO})[/yellow]")

    procesados = 0
    actualizados = 0
    sin_datos = 0
    fecha_hoy = datetime.now().strftime("%Y-%m-%d")

    for sku, info in cola:
        nombre = info.get("Descripcion", "")
        laboratorio = info.get("Laboratorio", "")
        precio_med = info.get("Precio") or 0
        try:
            costo_neto = float(precio_med)
        except (TypeError, ValueError):
            costo_neto = 0.0

        console.print(f"   🕵️ {sku} · {nombre[:50]} [{laboratorio[:15]}]")
        datos_mercado = buscar_precio_competencia(nombre, laboratorio, costo_neto_mediven=costo_neto)

        if datos_mercado:
            min_fmt = f"${datos_mercado['minimo']:,}".replace(',', '.')
            med_fmt = f"${datos_mercado['mediana_competitiva']:,}".replace(',', '.')
            fuentes = datos_mercado.get('fuentes_validas', '?')
            console.print(f"      [green]💰 Mínimo: {min_fmt} · Mediana: {med_fmt} · Fuentes válidas: {fuentes}[/green]")
            actualizados += 1
        else:
            console.print("      [red]❌ Sin datos confiables (queda como Monopolio)[/red]")
            sin_datos += 1

        precios_mercado[sku] = {
            "datos_mercado": datos_mercado,
            "fecha": fecha_hoy,
        }
        procesados += 1

        # Guardado intermedio cada 100 productos
        if procesados % 100 == 0:
            _guardar(precios_mercado)
            console.print(f"[blue]💾 Progreso guardado ({procesados}/{len(cola)})[/blue]")
            _commitear_a_github(procesados)

        time.sleep(1)  # Respeto al límite de Serper

    # Guardado final
    _guardar(precios_mercado)
    _commitear_a_github(procesados)

    console.print(Panel.fit(
        f"[bold green]✔ Espía terminado[/bold green]\n"
        f"Procesados: {procesados}\n"
        f"Con datos confiables: {actualizados}\n"
        f"Sin datos (Monopolio): {sin_datos}",
        title="📊 RESUMEN"
    ))


if __name__ == "__main__":
    ejecutar_repesca_diaria()
