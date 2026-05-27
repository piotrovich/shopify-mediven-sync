"""
scripts/diagnostico/generar_heroes.py

Detecta candidatos a 'héroes' (loss leaders) en el catálogo Mediven y
genera una propuesta para revisión manual.

FLUJO:
  1. python scripts/diagnostico/generar_heroes.py
     → busca en mediven_full.json y genera data/heroes_propuesta.json
  2. Revisa el JSON propuesto y borra los SKUs que no quieras como héroes
  3. python scripts/diagnostico/generar_heroes.py --aplicar
     → mueve la propuesta editada a data/heroes.json (activa la lista)

REQUIERE que exista mediven_full.json (lo genera 'python sync.py --dry-run').
"""

import os
import sys
import json
import re

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
MEDIVEN_FULL = os.path.join(BASE_DIR, "mediven_full.json")
ARCHIVO_PROPUESTA = os.path.join(BASE_DIR, "data", "heroes_propuesta.json")
ARCHIVO_HEROES = os.path.join(BASE_DIR, "data", "heroes.json")

# Cada candidato a héroe define:
#   - nombre: descripción legible (sale en el output del script)
#   - patrones: lista de regex/keywords. Deben matchear TODOS en la descripción.
#   - max_resultados: cuántos SKUs aceptar (se eligen los de menor costo Mediven)
CANDIDATOS = [
    # === Analgésicos / Antiinflamatorios ===
    {"nombre": "Paracetamol 500mg comprimidos", "patrones": ["paracetamol", r"\b500\s*mg\b", r"\bcom\b|\bcomp"], "max_resultados": 3},
    {"nombre": "Ibuprofeno 400mg comprimidos", "patrones": ["ibuprofeno", r"\b400\s*mg\b"], "max_resultados": 3},
    {"nombre": "Diclofenaco sodico 50mg", "patrones": ["diclofenac", r"\b50\s*mg\b"], "max_resultados": 3},
    {"nombre": "Ketoprofeno 100mg", "patrones": ["ketoprofeno", r"\b100\s*mg\b"], "max_resultados": 2},
    {"nombre": "Naproxeno 550mg", "patrones": ["naproxeno", r"\b550\s*mg\b"], "max_resultados": 2},

    # === Gastrointestinal ===
    {"nombre": "Omeprazol 20mg", "patrones": ["omeprazol", r"\b20\s*mg\b"], "max_resultados": 3},
    {"nombre": "Lansoprazol 30mg", "patrones": ["lansoprazol", r"\b30\s*mg\b"], "max_resultados": 2},
    {"nombre": "Famotidina 20mg", "patrones": ["famotidina"], "max_resultados": 2},
    {"nombre": "Loperamida 2mg", "patrones": ["loperamida"], "max_resultados": 2},

    # === Antialérgicos ===
    {"nombre": "Loratadina 10mg", "patrones": ["loratadina", r"\b10\s*mg\b"], "max_resultados": 3},
    {"nombre": "Cetirizina 10mg", "patrones": ["cetirizina", r"\b10\s*mg\b"], "max_resultados": 3},
    {"nombre": "Desloratadina 5mg", "patrones": ["desloratadina"], "max_resultados": 2},

    # === Cardiovascular / Crónicos ===
    {"nombre": "Losartan 50mg", "patrones": ["losart", r"\b50\s*mg\b"], "max_resultados": 3},
    {"nombre": "Enalapril 10mg", "patrones": ["enalapril", r"\b10\s*mg\b"], "max_resultados": 2},
    {"nombre": "Amlodipino 5mg", "patrones": ["amlodipino", r"\b5\s*mg\b"], "max_resultados": 2},
    {"nombre": "Atorvastatina 20mg", "patrones": ["atorvastatina", r"\b20\s*mg\b"], "max_resultados": 3},
    {"nombre": "Atenolol 50mg", "patrones": ["atenolol"], "max_resultados": 2},
    {"nombre": "Hidroclorotiazida 25mg", "patrones": ["hidroclorotiazida"], "max_resultados": 2},
    {"nombre": "Furosemida 40mg", "patrones": ["furosemida"], "max_resultados": 2},

    # === Endocrino ===
    {"nombre": "Metformina 850mg", "patrones": ["metformina", r"\b850\s*mg\b"], "max_resultados": 3},
    {"nombre": "Levotiroxina 50mcg", "patrones": ["levotiroxina", r"\b50\s*mcg\b|\b50\s*ug\b"], "max_resultados": 3},
    {"nombre": "Levotiroxina 100mcg", "patrones": ["levotiroxina", r"\b100\s*mcg\b|\b100\s*ug\b"], "max_resultados": 3},

    # === Salud mental ===
    {"nombre": "Sertralina 50mg", "patrones": ["sertralina", r"\b50\s*mg\b"], "max_resultados": 3},
    {"nombre": "Sertralina 100mg", "patrones": ["sertralina", r"\b100\s*mg\b"], "max_resultados": 2},
    {"nombre": "Escitalopram 10mg", "patrones": ["escitalopram", r"\b10\s*mg\b"], "max_resultados": 3},
    {"nombre": "Fluoxetina 20mg", "patrones": ["fluoxetina", r"\b20\s*mg\b"], "max_resultados": 2},

    # === Respiratorio ===
    {"nombre": "Salbutamol inhalador", "patrones": ["salbutamol", "inhal|aerosol"], "max_resultados": 2},
    {"nombre": "Budesonida inhalador", "patrones": ["budesonida", "inhal|aerosol"], "max_resultados": 2},

    # === Pediátrico ===
    {"nombre": "Paracetamol pediatrico jarabe/gotas", "patrones": ["paracetamol", "jarabe|jbe|gotas|gts"], "max_resultados": 3},
    {"nombre": "Ibuprofeno pediatrico jarabe/gotas", "patrones": ["ibuprofeno", "jarabe|jbe|gotas|gts"], "max_resultados": 3},
]


def buscar_candidato(productos, candidato):
    """Devuelve productos que matchean TODOS los patrones, ordenados por precio asc."""
    encontrados = []
    for prod in productos:
        descripcion = str(prod.get("Descripcion", "")).lower()
        if all(re.search(p, descripcion, re.IGNORECASE) for p in candidato["patrones"]):
            encontrados.append(prod)
    encontrados.sort(key=lambda x: float(x.get("Precio", 0) or 0))
    return encontrados[:candidato["max_resultados"]]


def aplicar_propuesta():
    if not os.path.exists(ARCHIVO_PROPUESTA):
        print(f"❌ No existe {ARCHIVO_PROPUESTA}.")
        print("   Corre primero: python scripts/diagnostico/generar_heroes.py")
        return
    with open(ARCHIVO_PROPUESTA, "r", encoding="utf-8") as f:
        propuesta = json.load(f)
    with open(ARCHIVO_HEROES, "w", encoding="utf-8") as f:
        json.dump(propuesta, f, indent=2, ensure_ascii=False)
    print(f"✅ Aplicado: {len(propuesta)} héroes activos en {ARCHIVO_HEROES}")


def generar_propuesta():
    if not os.path.exists(MEDIVEN_FULL):
        print(f"❌ Falta {MEDIVEN_FULL}.")
        print("   Corre primero: python sync.py --dry-run")
        return

    with open(MEDIVEN_FULL, "r", encoding="utf-8") as f:
        productos = json.load(f)

    print(f"📋 Buscando héroes en {len(productos)} productos Mediven...")
    print("=" * 70)

    propuesta = []
    sin_matches = []

    for cand in CANDIDATOS:
        matches = buscar_candidato(productos, cand)
        if not matches:
            sin_matches.append(cand["nombre"])
            continue

        print(f"\n✓ {cand['nombre']}: {len(matches)} candidato(s)")
        for m in matches:
            sku = str(m.get("Codigo", ""))
            desc = m.get("Descripcion", "")
            lab = m.get("Laboratorio", "")
            precio = float(m.get("Precio", 0) or 0)
            print(f"    [{sku}] {desc[:55]} · {lab[:20]} · costo: ${precio:,.0f}")
            propuesta.append({
                "sku": sku,
                "descripcion": desc,
                "laboratorio": lab,
                "costo_neto": precio,
                "categoria_heroe": cand["nombre"],
            })

    os.makedirs(os.path.dirname(ARCHIVO_PROPUESTA), exist_ok=True)
    with open(ARCHIVO_PROPUESTA, "w", encoding="utf-8") as f:
        json.dump(propuesta, f, indent=2, ensure_ascii=False)

    print()
    print("=" * 70)
    print(f"📊 RESUMEN")
    print(f"   Candidatos encontrados: {len(propuesta)}")
    print(f"   Sin matches: {len(sin_matches)}")
    if sin_matches:
        print(f"   ⚠️  Faltantes (ajustar patrones si te interesan):")
        for n in sin_matches:
            print(f"        · {n}")
    print(f"\n💾 Propuesta guardada en: {ARCHIVO_PROPUESTA}")
    print()
    print("PRÓXIMOS PASOS:")
    print(f"  1. Abre {ARCHIVO_PROPUESTA} en tu editor")
    print(f"  2. Borra los SKUs que NO quieres como héroes")
    print(f"  3. Aplica con: python scripts/diagnostico/generar_heroes.py --aplicar")


def main():
    if "--aplicar" in sys.argv:
        aplicar_propuesta()
    else:
        generar_propuesta()


if __name__ == "__main__":
    main()
