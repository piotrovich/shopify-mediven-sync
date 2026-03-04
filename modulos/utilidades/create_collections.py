import requests
import json
import os
import time
from dotenv import load_dotenv

# ============================
# CONFIGURACIÓN
# ============================
print("\n" + "="*50)
print("📂 CREANDO COLECCIONES AUTOMÁTICAS EN SHOPIFY")
print("==================================================\n")

load_dotenv()

SHOP_DOMAIN = os.getenv("SHOP_DOMAIN")
SHOPIFY_ADMIN_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-10")
GRAPHQL_ENDPOINT = f"https://{SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

# Definición de las Colecciones y sus Reglas (Lógica "O" - Disyuntiva)
# Si el título contiene CUALQUIERA de las palabras clave, entra a la colección.
COLECCIONES = [
    {
        "title": "Medicamentos",
        "keywords": ["Comprimido", "Jarabe", "Solución", "Solucion", "Cápsula", "Capsula", "Inyectable", "Antibiótico"]
    },
    {
        "title": "Cuidado Personal",
        "keywords": ["Shampoo", "Jabón", "Jabon", "Desodorante", "Crema", "Acondicionador", "Gel", "Loción"]
    },
    {
        "title": "Mundo Infantil",
        "keywords": ["Pañal", "Panal", "Bebé", "Bebe", "Infantil", "Mamadera", "Chupete", "Toallitas"]
    },
    {
        "title": "Dermocosmética",
        "keywords": ["Serum", "Facial", "Solar", "Antiarrugas", "Hidratante", "Limpieza", "Agua Micelar"]
    }
]

# ============================
# FUNCIONES
# ============================
def shopify_graphql(query, variables=None):
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN
    }
    try:
        response = requests.post(GRAPHQL_ENDPOINT, json={"query": query, "variables": variables}, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return None

def get_existing_collections():
    """Obtiene los títulos de las colecciones existentes para no duplicar."""
    query = """
    {
      collections(first: 250) {
        edges {
          node {
            title
          }
        }
      }
    }
    """
    data = shopify_graphql(query)
    titles = []
    if data and "data" in data:
        for edge in data["data"]["collections"]["edges"]:
            titles.append(edge["node"]["title"])
    return titles

def create_smart_collection(title, keywords):
    """Crea una colección inteligente basada en reglas."""
    
    # Construir reglas: TITLE CONTAINS Keyword
    rules = []
    for word in keywords:
        rules.append({
            "column": "TITLE",
            "relation": "CONTAINS",
            "condition": word
        })

    mutation = """
    mutation collectionCreate($input: CollectionInput!) {
      collectionCreate(input: $input) {
        collection {
          id
          title
          handle
          ruleSet {
            appliedDisjunctively
            rules {
              column
              relation
              condition
            }
          }
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    variables = {
        "input": {
            "title": title,
            "ruleSet": {
                "appliedDisjunctively": True, # True = OR (Cualquiera de las condiciones)
                "rules": rules
            }
        }
    }

    data = shopify_graphql(mutation, variables)
    
    if data and "data" in data and data["data"]["collectionCreate"]:
        result = data["data"]["collectionCreate"]
        if result["userErrors"]:
            print(f"⚠️ Error creando '{title}': {result['userErrors'][0]['message']}")
        else:
            col = result["collection"]
            print(f"✅ Colección creada: {col['title']} (ID: {col['id'].split('/')[-1]})")
            print(f"   ↳ Reglas: Contiene {', '.join(keywords[:3])}...")
    else:
        print(f"❌ Falló la creación de '{title}'")

# ============================
# MAIN
# ============================
def main():
    if not SHOPIFY_ADMIN_TOKEN:
        print("❌ Error: No se encontró el token de Shopify en el archivo .env")
        return

    print("🔍 Verificando colecciones existentes...")
    existentes = get_existing_collections()
    print(f"   → Encontradas: {existentes}")

    for col in COLECCIONES:
        if col["title"] in existentes:
            print(f"⏭️  La colección '{col['title']}' ya existe. Saltando...")
        else:
            print(f"✨ Creando colección '{col['title']}'...")
            create_smart_collection(col["title"], col["keywords"])
            time.sleep(1) # Pequeña pausa para no saturar

    print("\n✅ Proceso finalizado. Ve a Shopify > Productos > Colecciones para ver cómo se llenan.")

if __name__ == "__main__":
    main()
