import os
import json
import time
import subprocess
from dotenv import load_dotenv
# Importamos la función de búsqueda que ya construiste en tu espía principal
from modulos.finanzas.espia_precios import buscar_precio_competencia 

from rich.console import Console
from rich.panel import Panel

console = Console()

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv(os.path.join(BASE_DIR, ".env"))

ARCHIVO_MEDIVEN = os.path.join(BASE_DIR, "mediven_full.json")
ARCHIVO_MERCADO = os.path.join(BASE_DIR, "data", "precios_mercado.json")

def ejecutar_repesca_diaria():
    console.print(Panel.fit("🔍 [bold cyan]INICIANDO MINI-ESPÍA (REPESCA DE PRECIOS)[/bold cyan]"))
    
    if not os.path.exists(ARCHIVO_MEDIVEN):
        console.print("[red]❌ Falta mediven_full.json[/red]")
        return
        
    with open(ARCHIVO_MEDIVEN, "r", encoding="utf-8") as f:
        productos_mediven = json.load(f)
        
    precios_mercado = {}
    if os.path.exists(ARCHIVO_MERCADO):
        with open(ARCHIVO_MERCADO, "r", encoding="utf-8") as f:
            precios_mercado = json.load(f)

    # 1. Buscar SKUs que están en Mediven pero NO en nuestra memoria de mercado
    skus_mediven = {str(p.get("Codigo", "")) for p in productos_mediven}
    skus_mercado = set(precios_mercado.keys())
    
    skus_faltantes = skus_mediven - skus_mercado
    
    if not skus_faltantes:
        console.print("[green]✔ El JSON de mercado está 100% actualizado. Nada que espiar hoy.[/green]")
        return

    console.print(f"[yellow]⚠️ Se detectaron {len(skus_faltantes)} productos nuevos sin estudio de mercado.[/yellow]")
    
    # 🔓 FRENO LIBERADO: Cambiamos 50 por 20000 para que audite TODO
    faltantes_lista = list(skus_faltantes)[:20000]
    
    # Filtramos la data de Mediven para obtener los nombres de esos SKUs
    productos_a_espiar = [p for p in productos_mediven if str(p.get("Codigo", "")) in faltantes_lista]
    
    nuevos_precios = 0
    from datetime import datetime
    
    for p in productos_a_espiar:
        sku = str(p.get("Codigo", ""))
        nombre = p.get("Descripcion", "")
        # 🎯 INYECTAMOS EL LABORATORIO PARA MÁXIMA PRECISIÓN
        laboratorio = p.get("Laboratorio", "")
        
        console.print(f"   🕵️‍♂️ Buscando en Google: {nombre[:40]} [{laboratorio[:15]}]...")
        # Le pasamos el nombre Y el laboratorio al espía
        datos_mercado = buscar_precio_competencia(nombre, laboratorio)
        
        if datos_mercado:
            min_fmt = f"${datos_mercado['minimo']:,}".replace(',', '.')
            mediana_fmt = f"${datos_mercado['mediana_competitiva']:,}".replace(',', '.')
            console.print(f"      [green]💰 Encontrado -> Mínimo: {min_fmt} | Justo: {mediana_fmt}[/green]")
        else:
            console.print("      [red]❌ No encontrado (Quedará como Monopolio)[/red]")
            
        precios_mercado[sku] = {
            "datos_mercado": datos_mercado,
            "fecha": datetime.now().strftime("%Y-%m-%d")
        }
        nuevos_precios += 1
        
        # GUARDADO SEGURO Y SUBIDA A LA NUBE (Cada 100 productos para no saturar GitHub)
        if nuevos_precios % 100 == 0:
            os.makedirs(os.path.dirname(ARCHIVO_MERCADO), exist_ok=True)
            with open(ARCHIVO_MERCADO, "w", encoding="utf-8") as f:
                json.dump(precios_mercado, f, indent=2)
            console.print(f"[blue]💾 Progreso local guardado ({nuevos_precios} productos)...[/blue]")
            
            # ☁️ FORZAR LA SUBIDA A GITHUB EN TIEMPO REAL
            try:
                console.print("[cyan]☁️ Subiendo respaldo a GitHub...[/cyan]")
                subprocess.run(["git", "config", "--global", "user.name", "Robot-Espia"], check=False)
                subprocess.run(["git", "config", "--global", "user.email", "robot@espia.com"], check=False)
                subprocess.run(["git", "add", ARCHIVO_MERCADO], check=False)
                subprocess.run(["git", "commit", "-m", f"Auto-save espia: {nuevos_precios} productos"], check=False)
                subprocess.run(["git", "push"], check=False)
                console.print("[bold green]✅ ¡Respaldo subido a tu GitHub exitosamente![/bold green]")
            except Exception as e:
                console.print(f"[yellow]⚠️ No se pudo sincronizar con GitHub en este paso, reintentará en el próximo.[/yellow]")
            
        time.sleep(1) # Pequeña pausa para no saturar Google Serper

    # 3. Guardar el JSON final cuando termine el ciclo
    if nuevos_precios > 0:
        os.makedirs(os.path.dirname(ARCHIVO_MERCADO), exist_ok=True)
        with open(ARCHIVO_MERCADO, "w", encoding="utf-8") as f:
            json.dump(precios_mercado, f, indent=2)
        console.print(f"[bold green]💾 Se agregaron un total de {nuevos_precios} estudios de mercado al JSON.[/bold green]")
        
        try:
            # Le decimos a Git quién es el bot antes del push final
            subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"], check=False)
            subprocess.run(["git", "config", "--global", "user.email", "github-actions[bot]@users.noreply.github.com"], check=False)
            subprocess.run(["git", "add", ARCHIVO_MERCADO], check=False)
            subprocess.run(["git", "commit", "-m", f"Auto-save espia FINAL: {nuevos_precios} productos"], check=False)
            subprocess.run(["git", "push", "origin", "HEAD"], check=False)
        except Exception:
            pass

if __name__ == "__main__":
    ejecutar_repesca_diaria()
