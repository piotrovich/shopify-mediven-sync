import requests
import os
import time
from dotenv import load_dotenv

load_dotenv()

SHOP_DOMAIN = os.getenv("SHOP_DOMAIN")
SHOPIFY_ADMIN_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-10")

GRAPHQL_ENDPOINT = f"https://{SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
REST_BASE = f"https://{SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}"

HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN,
}


def gql(query, variables=None):
    for _ in range(6):
        try:
            r = requests.post(GRAPHQL_ENDPOINT, json={"query": query, "variables": variables or {}}, headers=HEADERS)
            if r.status_code == 429:
                time.sleep(float(r.headers.get("Retry-After", "2")))
                continue
            if r.status_code != 200:
                print("⚠️ Error GraphQL:", r.text[:200])
                return None
            return r.json()
        except:
            time.sleep(2)
    return None


def get_all_variants():
    query = """
    query($cursor: String){
      products(first: 100, after: $cursor){
        pageInfo { hasNextPage endCursor }
        edges{
          node{
            id
            title
            variants(first: 100){
              edges{
                node{
                  id
                  sku
                  taxable   # <- obtenemos si tiene impuesto
                }
              }
            }
          }
        }
      }
    }
    """

    variants = []
    cursor = None
    page = 1
    last_log = ""

    print("📦 Leyendo productos/variantes vía GraphQL...")

    while True:
        data = gql(query, {"cursor": cursor})
        if not data or "data" not in data:
            print("⚠️ Error en GraphQL.")
            break

        block = data["data"]["products"]
        edges = block["edges"]

        for e in edges:
            for v in e["node"]["variants"]["edges"]:
                variants.append({
                    "variant_id": v["node"]["id"].split("/")[-1],
                    "sku": v["node"]["sku"],
                    "taxable": v["node"]["taxable"]
                })

        log_msg = f"   → Página {page} (variantes acumuladas: {len(variants)})..."
        if log_msg != last_log:
            print("\r" + log_msg, end="", flush=True)
            last_log = log_msg

        page += 1

        if not block["pageInfo"]["hasNextPage"]:
            break

        cursor = block["pageInfo"]["endCursor"]

    print()
    print(f"✅ Total variantes cargadas: {len(variants)}")
    return variants


def remove_tax_for_variant(variant_id):
    url = f"{REST_BASE}/variants/{variant_id}.json"
    payload = {"variant": {"id": variant_id, "taxable": False}}

    for attempt in range(6):
        try:
            r = requests.put(url, json=payload, headers=HEADERS)
            if r.status_code == 429:
                time.sleep(float(r.headers.get("Retry-After", "2")))
                continue
            return r.status_code in (200, 201)
        except:
            time.sleep(2)
    return False


def main():
    print("============ REMOVE TAX ============")
    variants = get_all_variants()

    print("🔍 Filtrando variantes con impuesto activo...")
    variants_with_tax = [v for v in variants if v["taxable"]]

    print(f"➡️ Variantes con impuesto: {len(variants_with_tax)}")

    if not variants_with_tax:
        print("🎉 No hay variantes con impuesto. Nada que hacer.")
        return

    total = len(variants_with_tax)
    print(f"🔧 Procesando {total} variantes...")

    ok = 0
    err = 0

    for idx, v in enumerate(variants_with_tax, start=1):
        success = remove_tax_for_variant(v["variant_id"])

        if success:
            ok += 1
        else:
            err += 1

        if idx % 50 == 0 or idx == total:
            print(f"\r   → {idx}/{total} variantes procesadas (OK={ok}, errores={err})", end="", flush=True)

    print()
    print(f"✅ FINALIZADO: OK={ok}, errores={err}")
    print("=================================================")


if __name__ == "__main__":
    main()
