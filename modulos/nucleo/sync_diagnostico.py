import requests
import re
import json
import os
import time
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================
# CARGA VARIABLES .ENV
# ============================
load_dotenv()

SHOP_DOMAIN = os.getenv("SHOP_DOMAIN")
SHOPIFY_ADMIN_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-10")

MEDIVEN_USER = os.getenv("MEDIVEN_USER")
MEDIVEN_PASS = os.getenv("MEDIVEN_PASS")

# ID numérico de la ubicación principal (el que sale en la URL de locations)
SHOPIFY_LOCATION_ID = os.getenv("SHOPIFY_LOCATION_ID", "71003603149")
SHOPIFY_LOCATION_GID = f"gid://shopify/Location/{SHOPIFY_LOCATION_ID}"

LOGIN_URL = os.getenv(
    "MEDIVEN_LOGIN_URL",
    "https://b2b.mediven.cl:8389/b2bv3/ws/B2BAuthService/login",
)
INVENTORY_URL = os.getenv(
    "MEDIVEN_INVENTORY_URL",
    "https://b2b.mediven.cl:8389/b2bv3/ws/B2BService/inventario",
)

REPORT_DIR = "reportes"
os.makedirs(REPORT_DIR, exist_ok=True)

# Imagen genérica en Shopify (subida a Archivos)
DEFAULT_IMAGE_URL = os.getenv("SHOPIFY_DEFAULT_IMAGE_URL", "").strip()

GRAPHQL_ENDPOINT = f"https://{SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

# Campo de precio base en Mediven (ej: Precio)
PRICE_FIELD = os.getenv("PRICE_FIELD", "Precio")

# Tamaño seguro del batch para no pasar el costo 1000 de Shopify
BATCH_PRODUCTS = int(os.getenv("BATCH_SIZE", "30"))

# Flag de simulación
SIMULATE = os.getenv("SIMULATE", "false").lower() == "true"

# Flag de borrado definitivo de productos que faltan en Mediven
DELETE_MISSING = os.getenv("DELETE_MISSING", "false").lower() == "true"

# Workers paralelos para creación TURBO
WORKERS_TURBO = int(os.getenv("WORKERS_TURBO", "30"))

# Publicación Online Store (obtenido con get_publications.py)
ONLINE_STORE_PUBLICATION_ID = os.getenv(
    "ONLINE_STORE_PUBLICATION_ID",
    "gid://shopify/Publication/184418173133",  # Online Store
)

# ============================
# DICCIONARIO
# ============================
DICCIONARIO_NOMBRES = {
    " SOL ": " Solución ", " OFT ": " Oftálmica ", " SUSP ": " Suspensión ",
    " INY ": " Inyectable ", " COMP ": " Comprimidos ", " COM ": " Comprimidos ",
    " CAPS ": " Cápsulas ", " CAP ": " Cápsulas ", " JAR ": " Jarabe ",
    " JBE ": " Jarabe ", " FCO ": " Frasco ", " UNG ": " Ungüento ",
    " AER ": " Aerosol ", " AEROS ": " Aerosol ", " GRAT ": " Grageas ", 
    " SOB ": " Sobres ", " SBR ": " Sobres ", " SUP ": " Supositorios ", 
    " CREM ": " Crema ", " CRE ": " Crema ", " TAB ": " Tabletas ", 
    " GTS ": " Gotas ", " DISP ": " Dispersables ",
    " UND ": " Unidades ", " UNI ": " Unidades ", " UN ": " Unidades ",
    " UDS ": " Unidades ", " MTS ": " Metros ", " MT ": " Metros ",
    " REF ": " Referencia ", " M ": " Metros ", " U ": " Unidades ",
    " DES ": " Desodorante ", " ADH ": " Adhesivo ", " PROT ": " Protector ",
    " DENT ": " Dental ", " DEN ": " Dental ", " PVO ": " Polvo ",
    " S/SAB ": " Sin Sabor ", " P NORMAL ": " Piel Normal ", " P SECA ": " Piel Seca ",
    " P MIXTA ": " Piel Mixta ", " EX SECA ": " Extra Seca ", " OTI ": " Otótica ",
    " REPAR & BLANQ ": " Reparación & Blanqueamiento ", " FTE ": " Fuerte ",
}

# ============================
# FUNCIÓN DE LIMPIEZA FUERZA BRUTA
# ============================
def formatear_nombre_producto(item, debug=False):
    nombre_raw = str(item.get("Descripcion", "")).upper()
    principio_activo = str(item.get("Equivalente", "")).strip().upper()
    
    # 1. Diccionario
    nombre_corregido = f" {nombre_raw} "
    for abrev, reemplazo in DICCIONARIO_NOMBRES.items():
        nombre_corregido = nombre_corregido.replace(abrev, reemplazo)
    
    nombre_final = nombre_corregido.strip().title()
    if debug: print(f"   [DEBUG] Post-Diccionario: {nombre_final}")

    # 2. LIMPIEZA FUERZA BRUTA (Split por paréntesis)
    palabras_prohibidas = [
        'MAQUILLAJE', 'CUIDADO', 'PROTECCION', 'CEPILLOS', 'CREMA DENTAL',
        'DESODORANTES', 'DESODORANTE', 'SHAMPOO', 'ENJUAGUES', 'PAÑAL', 'VITAMINA', 'JABON',
        'COLORACION', 'COLONIA', 'PRESERVATIVO', 'APOSITO', 'ADHESIVO', 'GEL',
        'TALCO', 'ACONDICIONADOR', 'DEPILACION', 'PROBIOTICO', 'SOLAR',
        'DESMAQUILLANTE', 'BALSAMO', 'ACCESORIOS', 'BEBES', 'DENTAL',
        'ESPUMAS', 'SUPLEMENTOS', 'TOALLAS', 'PROTECTORES', 'INCONTINENCIA',
        'COLONIAS', 'LOCIONES', 'MAQUINAS', 'AFEITADO', 'DM', 'BE'
    ]
    
    # Dividimos el texto buscando paréntesis: "Nombre (Tag)" -> ["Nombre ", "(Tag)", ""]
    partes = re.split(r'(\([^)]+\))', nombre_final)
    partes_limpias = []
    
    for parte in partes:
        # Si es un paréntesis "(...)"
        if parte.startswith('(') and parte.endswith(')'):
            contenido = parte[1:-1].upper() # Lo de adentro en mayúsculas
            
            # Verificamos si contiene alguna palabra prohibida
            es_basura = False
            
            # Chequeo exacto para códigos cortos
            if contenido.strip() in ['DM', 'BE']:
                es_basura = True
            
            # Chequeo de palabras clave
            if not es_basura:
                for palabra in palabras_prohibidas:
                    # Usamos limites de palabra para evitar borrar cosas legitimas si fuera necesario
                    # pero aqui queremos ser agresivos.
                    if palabra in contenido:
                        es_basura = True
                        if debug: print(f"   [DEBUG] Eliminando '{parte}' por palabra '{palabra}'")
                        break
            
            if not es_basura:
                partes_limpias.append(parte)
        else:
            # Texto normal, se queda
            partes_limpias.append(parte)
            
    nombre_final = "".join(partes_limpias)
    
    # Limpieza extra para Dm/Be sueltos sin paréntesis
    nombre_final = re.sub(r'\bDm\b', '', nombre_final, flags=re.IGNORECASE)
    nombre_final = re.sub(r'\bBe\b', '', nombre_final, flags=re.IGNORECASE)

    # 3. Ajustes unidades
    nombre_final = (nombre_final.replace("Ml", "ml").replace("Mg", "mg")
                                .replace("Grs", "grs").replace("Mcg", "mcg")
                                .replace(" X ", " x ").replace("Ref ", "Referencia "))
    
    # 4. Principio activo
    if principio_activo and principio_activo != "NO APLICA":
        pa_clean = re.sub(r'[^A-Z]', '', principio_activo)
        nom_clean = re.sub(r'[^A-Z]', '', nombre_raw)
        if pa_clean not in nom_clean:
            nombre_final = f"{nombre_final} ({principio_activo.title()})"
            
    return re.sub(r'\s+', ' ', nombre_final).strip()


# ============================
# HELPER SHOPIFY GRAPHQL (MANTENIDO ORIGINAL)
# ============================
def shopify_graphql(query, variables=None, contexto="graphql", max_retries=6):
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN,
    }

    payload = {"query": query}

    if isinstance(variables, dict) and variables:
        payload["variables"] = variables
    elif variables not in (None, {}):
        print(f"⚠️ ADVERTENCIA: 'variables' ignoradas porque no son un dict válido en {contexto}")

    for intento in range(max_retries):
        try:
            resp = requests.post(
                GRAPHQL_ENDPOINT,
                headers=headers,
                json=payload,
                timeout=40,
            )

            if resp.status_code == 429:
                espera = float(resp.headers.get("Retry-After", "2") or "2")
                print(f"\n⚠️ GraphQL rate-limit ({contexto}) → esperando {espera}s...", flush=True)
                time.sleep(espera)
                continue

            if resp.status_code != 200:
                print(f"\n⚠️ HTTP {resp.status_code} en {contexto}: {resp.text[:300]}", flush=True)
                return None

            data = resp.json()

            if "errors" in data and data["errors"]:
                print(f"\n⚠️ Errores GraphQL top-level en {contexto}:")
                for err in data["errors"]:
                    print(f"   → message: {err.get('message')}")

            return data

        except requests.exceptions.RequestException as e:
            backoff = 1 + intento * 2
            print(f"\n⚠️ Error de conexión GraphQL en {contexto} ({e}) → reintento en {backoff}s...", flush=True)
            time.sleep(backoff)

    print(f"\n❌ Falló GraphQL definitivamente en {contexto}", flush=True)
    return None

# ============================
# LOGIN MEDIVEN (MANTENIDO ORIGINAL)
# ============================
def login_mediven():
    payload = {"aUser": MEDIVEN_USER, "aPassword": MEDIVEN_PASS, "aTipo": 0}
    headers = {
        "Content-Type": "application/json",
        "Origin": "https://b2b.mediven.cl:8387",
        "Referer": "https://b2b.mediven.cl:8387/",
    }

    print(f"Iniciando sesión en Mediven como usuario {MEDIVEN_USER}...")
    resp = requests.post(LOGIN_URL, json=payload, headers=headers)
    print("Respuesta login:", resp.status_code)
    resp.raise_for_status()

    data = resp.json()
    token = data.get("JwtToken")
    idsuc = data.get("IdSuc")

    if not token:
        raise Exception("Mediven no entregó token JWT.")

    print(f"✅ Token obtenido. IdSuc: {idsuc}")
    return token, idsuc

# ============================
# INVENTARIO MEDIVEN (MANTENIDO ORIGINAL)
# ============================
def get_mediven_inventory():
    token, idsuc = login_mediven()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Origin": "https://b2b.mediven.cl:8387",
        "Referer": "https://b2b.mediven.cl:8387/",
    }

    payload = {"IdSuc": idsuc}

    print("Descargando inventario desde Mediven...")
    resp = requests.post(INVENTORY_URL, headers=headers, json=payload)
    resp.raise_for_status()

    data = resp.json()
    items_raw = data.get("value", [])
    print(f"✅ Mediven (Bruto): {len(items_raw)} productos.")

    palabras_excluidas = [
        "perro", "perros","cachorro",
        "gato", "gatos",
        "mascota", "veterinaria",
        "mundo animal", "uso veterinario",
        "metilfenidato",
        "clonazepam",
        "clotiazepam",
        "fentermina",
        "alprazolam",
        "lorazepam",
        "abolengo",
        "aromatizante",
        "detergente",
        "arena para gatos",
        "(ec)",
        "lisdexanfetamina",
        "airwick"
    ]

    items_limpios = []
    excluidos = 0

    for item in items_raw:
        texto_busqueda = " ".join([
            str(item.get("Descripcion", "")),
            str(item.get("Laboratorio", "")),
            str(item.get("Equivalente", "")),
            str(item.get("AccionTerapeutica", ""))
        ]).lower()

        es_excluido = False
        for palabra in palabras_excluidas:
            if palabra in texto_busqueda:
                es_excluido = True
                break
        
        if es_excluido:
            excluidos += 1
        else:
            items_limpios.append(item)

    print(f"🧹 Filtrados {excluidos} productos excluidos/veterinarios.")
    print(f"📋 Total final válido: {len(items_limpios)} productos.")

    try:
        with open("mediven_full.json", "w", encoding="utf-8") as f:
            json.dump(items_limpios, f, ensure_ascii=False, indent=2)
        print("💾 Archivo guardado: mediven_full.json (LIMPIO)")
    except Exception as e:
        print(f"⚠️ Error guardando mediven_full.json: {e}")

    # --- NUEVO: CREACIÓN DEL DICCIONARIO PARA EL BOT DE LOGÍSTICA ---
    diccionario_bot = {}
    for item in items_limpios:
        codigo_sku = str(item.get("Codigo", "")).strip()
        id_producto = str(item.get("IdProd", "")).strip()
        
        # Si el producto tiene ambos datos, lo agregamos al diccionario
        if codigo_sku and id_producto:
            diccionario_bot[codigo_sku] = id_producto

    # 📌 Aseguramos que se guarde en la carpeta "data"
    ruta_diccionario = os.path.join("data", "catalogo_mediven.json")
    
    try:
        os.makedirs("data", exist_ok=True) # Crea la carpeta data si no existe
        with open(ruta_diccionario, "w", encoding="utf-8") as f:
            json.dump(diccionario_bot, f, ensure_ascii=False, indent=2)
        print(f"🤖 Diccionario logístico actualizado: {ruta_diccionario}")
    except Exception as e:
        print(f"⚠️ Error guardando diccionario logístico: {e}")
    # ----------------------------------------------------------------

    return items_limpios

# ============================
# EXPORTAR SKUS MEDIVEN (MANTENIDO ORIGINAL)
# ============================
def exportar_skus_mediven():
    print("📥 Descargando inventario Mediven para exportar SKUs...")
    items = get_mediven_inventory()
    skus = sorted(
        {
            str(it.get("Codigo", "")).strip()
            for it in items
            if str(it.get("Codigo", "")).strip()
        }
    )
    with open("skus_mediven.json", "w", encoding="utf-8") as f:
        json.dump(skus, f, ensure_ascii=False, indent=2)
    print(f"✅ Exportados {len(skus)} SKUs a skus_mediven.json")

# ============================
# SHOPIFY - LECTURA POR GRAPHQL (MANTENIDO ORIGINAL)
# ============================
def get_shopify_products():
    print("Descargando productos de Shopify (GraphQL, solo lectura)...")
    products = []

    query = """
    query($cursor: String) {
      products(first: 100, after: $cursor) {
        pageInfo {
          hasNextPage
          endCursor
        }
        edges {
          node {
            id
            title
            bodyHtml
            status
            media(first: 1) { edges { node { id } } }
            variants(first: 100) {
              edges {
                node {
                  id
                  sku
                  price
                  taxable
                }
              }
            }
          }
        }
      }
    }
    """

    cursor = None
    page = 1
    last_log = ""

    while True:
        data = shopify_graphql(
            query,
            variables={"cursor": cursor},
            contexto="get_shopify_products_graphql",
        )
        if not data or "data" not in data or not data["data"].get("products"):
            print("\n⚠️ Respuesta inválida en get_shopify_products (GraphQL).")
            raise Exception("🛑 CRÍTICO: Internet falló al leer Shopify. Abortando sincronización para evitar crear duplicados.")
            break

        prods_block = data["data"]["products"]
        page_info = prods_block.get("pageInfo", {}) or {}
        edges = prods_block.get("edges", []) or []

        for edge in edges:
            node = edge.get("node") or {}
            gid = node.get("id", "")
            product_id = gid.split("/")[-1] if gid else None
            title = node.get("title", "")
            body_html = node.get("bodyHtml", "") or ""
            status = node.get("status", "ACTIVE").lower()
            media_edges = node.get("media", {}).get("edges", [])
            has_image = len(media_edges) > 0

            if status == "active":
                status_norm = "active"
            elif status == "draft":
                status_norm = "draft"
            else:
                status_norm = status.lower()

            variants_edges = node.get("variants", {}).get("edges", []) or []

            rest_variants = []
            for vedge in variants_edges:
                vnode = vedge.get("node") or {}
                vgid = vnode.get("id", "")
                variant_id = vgid.split("/")[-1] if vgid else None
                sku = vnode.get("sku")
                price = vnode.get("price") or "0"
                taxable = vnode.get("taxable", False)
                rest_variants.append(
                    {
                        "id": variant_id,
                        "sku": sku,
                        "price": price,
                        "taxable": taxable,
                    }
                )

            products.append(
                {
                    "id": product_id,
                    "title": title,
                    "bodyHtml": body_html,
                    "status": status_norm,
                    "has_image": has_image,
                    "variants": rest_variants,
                }
            )

        acumulados = len(products)
        log_msg = f"   → Página {page} (acumulados: {acumulados} productos)..."
        if log_msg != last_log:
            print(f"\r{log_msg}", end="", flush=True)
            last_log = log_msg

        page += 1

        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    print()
    print(f"✅ Shopify (GraphQL): {len(products)} productos cargados.")
    return products

def normalize_shopify_products(products):
    rows = []
    for p in products:
        product_id = p.get("id")
        product_title = p.get("title", "")
        status = p.get("status", "active")
        for v in p.get("variants", []):
            rows.append(
                {
                    "product_id": product_id,
                    "product_title": product_title,
                    "bodyHtml": p.get("bodyHtml", ""),
                    "has_image": p.get("has_image", True),
                    "variant_id": v.get("id"),
                    "sku": v.get("sku"),
                    "price": float(v.get("price", 0) or 0),
                    "status": status,
                    "taxable": v.get("taxable", False),
                }
            )
    return rows

def calcular_precio(precio_base):
    try:
        p = float(precio_base)
    except:
        return 0

    if p <= 0:
        return 0

    p = p * 1.71
    p = int(math.ceil(p / 100.0) * 100)
    return p

# ============================
# GENERAR EXCEL (MANTENIDO ORIGINAL)
# ============================
def generar_excel(crear, actualizar, archivar, mediven_data):
    fecha = datetime.now().strftime("%Y-%m-%d_%H-%M")
    ruta = os.path.join(REPORT_DIR, f"diagnostico_sync_{fecha}.xlsx")

    df_crear = pd.DataFrame(crear)
    df_actualizar = pd.DataFrame(actualizar)
    df_archivar = pd.DataFrame(archivar)

    cambios_rows = []
    for item in actualizar:
        cambios_rows.append(
            {
                "SKU": item.get("SKU"),
                "Descripcion": item.get("Descripcion", ""),
                "Precio_Anterior": item.get("Precio_Shopify", 0),
                "Precio_Nuevo": item.get("Nuevo_Precio", 0),
                "Diferencia": item.get("Nuevo_Precio", 0)
                - item.get("Precio_Shopify", 0),
            }
        )
    df_cambios = pd.DataFrame(cambios_rows)

    with pd.ExcelWriter(ruta, engine="openpyxl") as writer:
        df_crear.to_excel(writer, index=False, sheet_name="CREAR")
        df_actualizar.to_excel(writer, index=False, sheet_name="ACTUALIZAR")
        df_archivar.to_excel(writer, index=False, sheet_name="ARCHIVAR")
        df_cambios.to_excel(writer, index=False, sheet_name="CAMBIOS_PRECIO")

    print(f"📊 Excel generado: {ruta}")

# ============================
# SINCRONIZACIÓN PRODUCCIÓN (MANTENIDO ORIGINAL)
# ============================
def sincronizar_con_shopify(crear, actualizar, archivar, solo_archivar=False):
    print("⚙️ Iniciando sincronización con Shopify (solo GraphQL)...")

    ok_total = 0
    err_total = 0

    if archivar:
        if DELETE_MISSING:
            print(f"🗑 Archivando/eliminando {len(archivar)} productos...")
            ok_del, err_del = archive_products_graphql(archivar)
            ok_total += ok_del
            err_total += err_del
        else:
            print("\nℹ️ DELETE_MISSING=false → productos faltantes NO se eliminarán definitivamente.")

    if solo_archivar:
        print("🔁 Solo archivado — fin del proceso.")
        return {"ok": ok_total, "errores": err_total}

    # ACTUALIZAR NOMBRES (NUEVO)
    productos_a_actualizar = [p for p in actualizar if p.get("actualizar_nombre")]
    if productos_a_actualizar:
        bulk_update_product_basics(productos_a_actualizar)

    # ACTUALIZAR PRECIOS (ORIGINAL)
    if actualizar:
        print(f"🔁 Actualizando precios de {len(actualizar)} productos…")
        r = graphql_bulk_update_variants(actualizar)
        ok_total += r.get("ok", 0)
        err_total += r.get("errores", 0)

    # CREAR (ORIGINAL)
    if crear:
        print(f"🆕 Creando {len(crear)} productos (TURBO)…")
        r = crear_productos_graphql_turbo(crear, batch_size=30)
        ok_total += r.get("ok", 0)
        err_total += r.get("errores", 0)

    print("\n=== RESULTADO SINCRONIZACIÓN ===")
    print(f"✔ OK totales: {ok_total}")
    print(f"❌ Errores totales: {err_total}")
    print("==============================\n")

    return {"ok": ok_total, "errores": err_total}

# ============================
# MAIN (MANTENIDO ORIGINAL)
# ============================
def main():
    import sys

    modo = "diagnostico"
    solo_archivar = False
    export_skus = False

    args = [a.lower() for a in sys.argv[1:]]

    if "--export-skus" in args:
        export_skus = True

    if "--solo-archivar" in args:
        modo = "produccion"
        solo_archivar = True
    elif "--modo" in args:
        idx = args.index("--modo")
        if idx + 1 < len(args) and args[idx + 1] == "produccion":
            modo = "produccion"

    if export_skus:
        exportar_skus_mediven()
        return

    print(f"🔧 Modo actual: {modo.upper()}")
    if solo_archivar:
        print("🔁 Opción --solo-archivar ACTIVADA.")
    if SIMULATE:
        print("🧪 SIMULATE=true → NO se aplican cambios en Shopify.")
    print("=== INICIO ===")

    mediven_data = get_mediven_inventory()
    shopify_products = get_shopify_products()

    df_med = pd.DataFrame(mediven_data)
    df_shop = pd.DataFrame(normalize_shopify_products(shopify_products))

    if not df_shop.empty:
        df_shop["sku"] = df_shop["sku"].astype(str).str.strip()
    else:
        df_shop["sku"] = pd.Series(dtype=str)

    df_med["Codigo"] = df_med["Codigo"].astype(str).str.strip()

    skus_med = set(df_med["Codigo"])

    shop_by_sku = {}
    for _, row in df_shop.iterrows():
        sku = row["sku"]
        if sku and sku not in shop_by_sku:
            shop_by_sku[sku] = row

    crear = []
    actualizar = []
    archivar = []

    for _, row in df_med.iterrows():
        sku = row["Codigo"]
        
        # GENERAMOS LOS DATOS NUEVOS USANDO LAS NUEVAS LÓGICAS
        nom_gen = formatear_nombre_producto(row)
        nuevo_precio = calcular_precio(row.get("Precio", 0))

        if sku in shop_by_sku:
            s_row = shop_by_sku[sku]
            precio_actual = float(s_row["price"] or 0)
            nombre_actual_shopify = str(s_row.get("product_title", ""))

            # COMPARAR PRECIO Y NOMBRE
            c_pre = abs(precio_actual - nuevo_precio) >= 1
            c_nom = nom_gen != nombre_actual_shopify

            if c_pre or c_nom:
                actualizar.append(
                    {
                        "SKU": sku,
                        "Descripcion": nom_gen,
                        "Precio_Shopify": precio_actual,
                        "Nuevo_Precio": nuevo_precio,
                        "variant_id": s_row["variant_id"],
                        "product_id": s_row["product_id"],
                        "actualizar_nombre": c_nom
                    }
                )
        else:
            crear.append(
                {
                    "SKU": sku,
                    "Descripcion": nom_gen,
                    "Precio": nuevo_precio,
                    "Stock": 100,
                }
            )

    for _, row in df_shop.iterrows():
        sku = row["sku"]
        if not sku:
            continue
        if sku not in skus_med:
            archivar.append(
                {
                    "SKU": sku,
                    "product_id": row["product_id"],
                    "Descripcion": row.get("product_title", ""),
                    "status_actual": row.get("status", "active"),
                }
            )

    # NUEVA LÓGICA DE CONTEO PARA EL LOG
    ya_archivados = len([p for p in archivar if p.get("status_actual") == "archived"])
    nuevos_por_archivar = len([p for p in archivar if p.get("status_actual") != "archived"])

    print("\n╭─ 📊 DIAGNÓSTICO DETALLADO ─╮")
    print(f"│ CREAR: {len(crear):<18} │")
    print(f"│ ACTUALIZAR: {len(actualizar):<13} │")
    print(f"│ ARCHIVAR (Nuevos): {nuevos_por_archivar:<7} │")
    print(f"│ YA ARCHIVADOS: {ya_archivados:<11} │")
    print("╰────────────────────────────╯\n")

    if modo == "diagnostico" or SIMULATE:
        print("🧪 Modo diagnóstico — sin cambios en Shopify.")
        generar_excel(crear, actualizar, archivar, mediven_data)
    else:
        print("⚙️ Modo producción — aplicando cambios...")
        sincronizar_con_shopify(crear, actualizar, archivar, solo_archivar=solo_archivar)
        generar_excel(crear, actualizar, archivar, mediven_data)

    print("=== FIN ===")

if __name__ == "__main__":
    main()
