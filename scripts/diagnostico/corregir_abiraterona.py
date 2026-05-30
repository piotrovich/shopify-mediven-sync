import json, os, shutil
from datetime import datetime

ARCHIVO = "data/precios_mercado.json"
SKU = "7804681740797"        # ABIRATERONA 500 MG COM REC X 60
PRECIO_MERCADO = 1099090     # Farmex (caja completa). Ajusta si quieres ir más abajo.

with open(ARCHIVO, "r", encoding="utf-8") as f:
    mercado = json.load(f)

ts = datetime.now().strftime("%Y%m%d_%H%M%S")
os.makedirs("data/backups", exist_ok=True)
shutil.copy2(ARCHIVO, f"data/backups/precios_mercado_pre_abiraterona_{ts}.json")

antes = mercado.get(SKU, {}).get("datos_mercado", {})
print(f"Antes: mediana ${antes.get('mediana_competitiva', 0):,} ({antes.get('fuentes_validas', 0)} fuentes)")

mercado[SKU] = {
    "datos_mercado": {
        "minimo": PRECIO_MERCADO,
        "mediana_competitiva": PRECIO_MERCADO,
        "fuentes_validas": 2,
        "detalle": [
            {"farmacia": "Farmex (verificado manual)", "precio": PRECIO_MERCADO, "estado": "🟢 Válido (override manual)"},
            {"farmacia": "Farmex fraccionado x60", "precio": PRECIO_MERCADO, "estado": "🟢 Válido (override manual)"}
        ]
    },
    "fecha": datetime.now().strftime("%Y-%m-%d"),
    "override_manual": True
}

with open(ARCHIVO, "w", encoding="utf-8") as f:
    json.dump(mercado, f, indent=2, ensure_ascii=False)

print(f"Después: mediana ${PRECIO_MERCADO:,}")
print("OK. El próximo sync pondrá Abiraterona en ~$1.099.090 (markup 2.7x, competitivo).")
