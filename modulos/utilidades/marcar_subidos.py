import json

ARCHIVO = 'data/diccionario_ia.json'

with open(ARCHIVO, 'r', encoding='utf-8') as f:
    diccionario = json.load(f)

marcados = 0
for sku, datos in diccionario.items():
    # Le agregamos la bandera a todos los productos
    if not datos.get("subido_shopify", False):
        diccionario[sku]["subido_shopify"] = True
        marcados += 1

with open(ARCHIVO, 'w', encoding='utf-8') as f:
    json.dump(diccionario, f, ensure_ascii=False, indent=2)

print(f"✅ ¡Magia! Se marcaron {marcados} productos como 'ya subidos'.")
print("Ahora tu script de subida será ultra rápido.")
