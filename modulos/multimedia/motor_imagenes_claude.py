#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Motor de imágenes con Claude (reemplaza al buscador de Google Imágenes vía Serper).

Flujo por producto:
  1. BÚSQUEDA (Claude + herramienta web_search): Claude busca en la web la ficha
     del producto en sitios confiables (laboratorio, farmacias y retail chileno)
     y devuelve hasta 3 páginas candidatas en JSON estructurado.
  2. EXTRACCIÓN (local, sin IA): descargamos cada página candidata y sacamos las
     URLs de imagen de producto (JSON-LD Product.image, og:image, twitter:image,
     <link rel="image_src"> y, como último recurso, etiquetas <img>).
  3. NORMALIZACIÓN (Pillow): lienzo 800x800 con fondo blanco, JPEG calidad 90.
  4. VERIFICACIÓN (Claude con visión): Claude mira la foto ya normalizada y
     confirma que es el producto correcto y una foto de catálogo. Si no lo es,
     se prueba la siguiente candidata.

Variables de entorno:
  ANTHROPIC_API_KEY             obligatoria (si falta, el módulo se salta).
  CLAUDE_MODEL_IMAGENES         modelo (default: claude-opus-5-5).
  CLAUDE_EFFORT_IMAGENES        esfuerzo de la búsqueda: low|medium|high (default: medium).
  IMAGENES_MAX_BUSQUEDAS        búsquedas web por producto (default: 3).
  IMAGENES_MAX_PAGINAS          páginas candidatas a revisar por producto (default: 3).
  IMAGENES_MAX_VERIFICACIONES   fotos que Claude revisa con visión por producto (default: 3).
  IMAGENES_VERIFICAR_VISION     true/false, verificación visual (default: true).
  IMAGENES_LADO_MINIMO          px mínimos del lado mayor de la foto original (default: 250).
"""

import os
import io
import re
import json
import base64
import html as html_lib
from urllib.parse import urljoin, urlparse

import requests
from PIL import Image

import anthropic

# ==========================================
# CONFIGURACIÓN
# ==========================================
MODELO = os.getenv("CLAUDE_MODEL_IMAGENES", "claude-opus-5-5")
EFFORT_BUSQUEDA = os.getenv("CLAUDE_EFFORT_IMAGENES", "medium")
MAX_BUSQUEDAS = int(os.getenv("IMAGENES_MAX_BUSQUEDAS", "3"))
MAX_PAGINAS = int(os.getenv("IMAGENES_MAX_PAGINAS", "3"))
MAX_VERIFICACIONES = int(os.getenv("IMAGENES_MAX_VERIFICACIONES", "3"))
VERIFICAR_VISION = os.getenv("IMAGENES_VERIFICAR_VISION", "true").lower() == "true"
LADO_MINIMO = int(os.getenv("IMAGENES_LADO_MINIMO", "250"))
MAX_IMAGENES_POR_PAGINA = 3
TAMANO_LIENZO = 800

# Dominios que nunca sirven como fuente (nuestra propia tienda, farmex y redes sociales).
DOMINIOS_BLOQUEADOS = [
    "farmaciaslf.cl",
    "farmex.cl",
    "pinterest.com",
    "facebook.com",
    "instagram.com",
    "tiktok.com",
    "youtube.com",
    "x.com",
    "twitter.com",
]

UBICACION_USUARIO = {
    "type": "approximate",
    "city": "Santiago",
    "region": "Región Metropolitana",
    "country": "CL",
    "timezone": "America/Santiago",
}

HEADERS_NAVEGADOR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "es-CL,es;q=0.9,en;q=0.5",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

# Contadores de consumo de la corrida (para el resumen final del log).
USO = {"llamadas": 0, "input_tokens": 0, "output_tokens": 0, "busquedas_web": 0}


# ==========================================
# EXCEPCIONES
# ==========================================
class MotorNoDisponible(Exception):
    """La API de Claude no se puede usar (key inválida, sin crédito, modelo inexistente).
    El orquestador debe detener el módulo SIN castigar a los productos pendientes."""


class ErrorTransitorio(Exception):
    """Fallo de red / rate limit / error 5xx tras los reintentos del SDK.
    El producto se salta en esta corrida y se reintenta en la próxima."""


# ==========================================
# CLIENTE
# ==========================================
_client = None
_usar_fallbacks = True


def motor_disponible():
    return bool(os.getenv("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_AUTH_TOKEN"))


def obtener_cliente():
    global _client
    if _client is None:
        if not motor_disponible():
            raise MotorNoDisponible("Falta ANTHROPIC_API_KEY en el entorno.")
        # Timeout por request de 3 min: las búsquedas web pueden tardar. El SDK
        # reintenta solo 429/5xx/errores de conexión (max_retries).
        _client = anthropic.Anthropic(timeout=180.0, max_retries=2)
    return _client


def _llamar_claude(**kwargs):
    """
    Envía una solicitud a la API de Claude y traduce los errores del SDK a las
    excepciones del motor. Usa el endpoint beta para activar los fallbacks
    server-side ante un `refusal`; si ese beta no estuviera disponible, repite
    la misma solicitud por el endpoint estándar (una sola vez por corrida).
    """
    global _usar_fallbacks
    client = obtener_cliente()
    try:
        if _usar_fallbacks:
            try:
                return client.beta.messages.create(
                    betas=["server-side-fallback-2026-07-01"],
                    fallbacks="default",
                    **kwargs,
                )
            except anthropic.BadRequestError as e:
                texto = str(e).lower()
                if "fallback" in texto or "beta" in texto:
                    print("      ℹ️ Fallbacks server-side no disponibles en esta cuenta; se continúa sin ellos.")
                    _usar_fallbacks = False
                else:
                    raise
        return client.messages.create(**kwargs)

    except anthropic.AuthenticationError as e:
        raise MotorNoDisponible(f"ANTHROPIC_API_KEY inválida: {e.message}") from e
    except anthropic.PermissionDeniedError as e:
        raise MotorNoDisponible(f"La API key no tiene permisos: {e.message}") from e
    except anthropic.NotFoundError as e:
        raise MotorNoDisponible(f"Modelo o endpoint no encontrado ({MODELO}): {e.message}") from e
    except anthropic.BadRequestError as e:
        # 400: solicitud mal formada, modelo no soportado o crédito agotado.
        raise MotorNoDisponible(f"La API rechazó la solicitud (400): {e.message}") from e
    except anthropic.RateLimitError as e:
        raise ErrorTransitorio(f"Límite de velocidad de la API (429) tras reintentos: {e.message}") from e
    except anthropic.APIStatusError as e:
        if e.status_code >= 500:
            raise ErrorTransitorio(f"Error del servidor de Anthropic ({e.status_code})") from e
        raise MotorNoDisponible(f"Error de la API ({e.status_code}): {e.message}") from e
    except anthropic.APIConnectionError as e:
        raise ErrorTransitorio(f"Sin conexión con la API de Anthropic: {e}") from e


def _registrar_uso(respuesta):
    uso = getattr(respuesta, "usage", None)
    if not uso:
        return
    USO["llamadas"] += 1
    USO["input_tokens"] += getattr(uso, "input_tokens", 0) or 0
    USO["output_tokens"] += getattr(uso, "output_tokens", 0) or 0
    stu = getattr(uso, "server_tool_use", None)
    if stu is not None:
        USO["busquedas_web"] += getattr(stu, "web_search_requests", 0) or 0


def resumen_uso():
    return (
        f"{USO['llamadas']} llamadas · {USO['busquedas_web']} búsquedas web · "
        f"{USO['input_tokens']:,} tokens entrada · {USO['output_tokens']:,} tokens salida"
    ).replace(",", ".")


def _texto_de_respuesta(respuesta):
    return "".join(b.text for b in respuesta.content if getattr(b, "type", "") == "text").strip()


# ==========================================
# PASO 1: BÚSQUEDA DE PÁGINAS CANDIDATAS
# ==========================================
PROMPT_SISTEMA_BUSQUEDA = f"""Eres el asistente de catálogo de Farmacias LF, una farmacia online de Chile (farmaciaslf.cl).
Recibes el nombre de un producto tal como está en nuestra tienda y debes encontrar, usando web_search, páginas de producto confiables que muestren una foto de catálogo del producto EXACTO: misma marca y misma presentación (forma farmacéutica, dosis, cantidad de unidades, ml o gramos).

Fuentes preferidas, en este orden:
1. El sitio del laboratorio o de la marca.
2. Farmacias y retail de Chile: Farmacias Ahumada, Salcobrand, Cruz Verde, Dr. Simi, Ecofarmacias, Farmacia Roraima, Mercadofarma, Redfarma, Preunic, Jumbo, Líder, Falabella, Paris.
3. Otras tiendas de habla hispana que vendan el mismo producto.

Evita notas de prensa, blogs, redes sociales, videos, PDFs, fotos de usuarios en marketplaces y productos parecidos pero de otra presentación.

Haz como máximo {MAX_BUSQUEDAS} búsquedas; si la primera ya entrega una ficha de producto clara, no sigas buscando.
Devuelve hasta 3 candidatos ordenados de mayor a menor confianza. "url_pagina" es la URL de la ficha del producto. "url_imagen" solo si viste una URL directa a la imagen (termina en .jpg, .jpeg, .png o .webp, o es claramente un CDN de imágenes); si no, null.
Si no encuentras nada confiable, devuelve "candidatos": [] y "sin_resultados": true."""

ESQUEMA_CANDIDATOS = {
    "type": "object",
    "properties": {
        "nombre_normalizado": {"type": "string"},
        "candidatos": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "url_pagina": {"type": "string"},
                    "url_imagen": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                    "tienda": {"type": "string"},
                    "confianza": {"type": "string", "enum": ["alta", "media", "baja"]},
                    "motivo": {"type": "string"},
                },
                "required": ["url_pagina", "url_imagen", "tienda", "confianza", "motivo"],
                "additionalProperties": False,
            },
        },
        "sin_resultados": {"type": "boolean"},
    },
    "required": ["nombre_normalizado", "candidatos", "sin_resultados"],
    "additionalProperties": False,
}

_ORDEN_CONFIANZA = {"alta": 0, "media": 1, "baja": 2}


def _es_url_http(valor):
    try:
        p = urlparse(str(valor))
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def _dominio_bloqueado(url):
    dominio = urlparse(url).netloc.lower()
    return any(dominio == d or dominio.endswith("." + d) for d in DOMINIOS_BLOQUEADOS)


def _parsear_candidatos(texto):
    try:
        datos = json.loads(texto)
    except (json.JSONDecodeError, TypeError):
        return []
    candidatos = []
    for c in datos.get("candidatos", []) or []:
        if not isinstance(c, dict):
            continue
        url_pagina = str(c.get("url_pagina") or "").strip()
        url_imagen = c.get("url_imagen")
        url_imagen = str(url_imagen).strip() if url_imagen else None
        if not _es_url_http(url_pagina) and not (url_imagen and _es_url_http(url_imagen)):
            continue
        if url_pagina and _dominio_bloqueado(url_pagina):
            continue
        if url_imagen and (not _es_url_http(url_imagen) or _dominio_bloqueado(url_imagen)):
            url_imagen = None
        candidatos.append({
            "url_pagina": url_pagina if _es_url_http(url_pagina) else None,
            "url_imagen": url_imagen,
            "tienda": str(c.get("tienda") or ""),
            "confianza": c.get("confianza") if c.get("confianza") in _ORDEN_CONFIANZA else "baja",
            "motivo": str(c.get("motivo") or ""),
        })
    candidatos.sort(key=lambda c: _ORDEN_CONFIANZA[c["confianza"]])
    return candidatos[:3]


def buscar_candidatos(titulo, laboratorio="", descripcion_original=""):
    """
    Pide a Claude (con web_search) hasta 3 fichas de producto candidatas.
    Devuelve una lista de dicts {url_pagina, url_imagen, tienda, confianza, motivo}
    ordenada por confianza. Lista vacía = no se encontró nada confiable.
    """
    consulta = (
        f"Producto en nuestra tienda: {titulo}\n"
        f"Laboratorio / marca (según el mayorista): {laboratorio or 'desconocido'}\n"
        f"Descripción original del mayorista: {descripcion_original or '-'}\n\n"
        "Busca la ficha del producto y responde con el JSON pedido."
    )
    herramientas = [{
        "type": "web_search_20260209",
        "name": "web_search",
        "max_uses": MAX_BUSQUEDAS,
        "blocked_domains": DOMINIOS_BLOQUEADOS,
        "user_location": UBICACION_USUARIO,
    }]
    mensajes = [{"role": "user", "content": consulta}]

    # Las búsquedas largas pueden devolver `pause_turn`: se reenvía la conversación
    # con el turno pausado para que la API la continúe (máximo 4 veces).
    for _ in range(5):
        respuesta = _llamar_claude(
            model=MODELO,
            max_tokens=8192,
            system=PROMPT_SISTEMA_BUSQUEDA,
            messages=mensajes,
            tools=herramientas,
            output_config={
                "effort": EFFORT_BUSQUEDA,
                "format": {"type": "json_schema", "schema": ESQUEMA_CANDIDATOS},
            },
        )
        _registrar_uso(respuesta)

        if respuesta.stop_reason == "pause_turn":
            mensajes = mensajes + [{"role": "assistant", "content": respuesta.content}]
            continue
        if respuesta.stop_reason == "refusal":
            print("      ⚠️ Claude declinó la búsqueda (refusal).")
            return []
        if respuesta.stop_reason == "max_tokens":
            print("      ⚠️ Respuesta cortada por max_tokens; se descarta.")
            return []
        return _parsear_candidatos(_texto_de_respuesta(respuesta))

    print("      ⚠️ La búsqueda quedó pausada demasiadas veces; se descarta.")
    return []


# ==========================================
# PASO 2: EXTRACCIÓN DE IMÁGENES DE UNA PÁGINA (SIN IA)
# ==========================================
_EXT_IMAGEN = (".jpg", ".jpeg", ".png", ".webp")
_PISTAS_PRODUCTO = (
    "product", "produto", "producto", "catalog", "catalogo", "arquivos/ids",
    "images/products", "cdn.shopify.com", "jumpseller", "cloudinary", "vtexassets",
    "demandware", "/dw/image/", "media/catalog", "sku", "/p/",
)
_PISTAS_DESCARTE = (
    "logo", "icon", "sprite", "banner", "flag", "payment", "pago", "webpay",
    "placeholder", "loading", "spinner", "pixel", "tracking", "avatar", "badge",
    ".svg", ".gif", "facebook", "instagram", "whatsapp", "visa", "mastercard",
)


def _descargar_html(url):
    r = requests.get(url, headers=HEADERS_NAVEGADOR, timeout=15, allow_redirects=True)
    if r.status_code != 200:
        return None, url
    tipo = r.headers.get("Content-Type", "")
    if "html" not in tipo and "xml" not in tipo and "text" not in tipo:
        return None, r.url
    return r.text[:3_000_000], r.url


def _imagenes_de_jsonld(html, base):
    urls = []

    def recorrer(nodo):
        if isinstance(nodo, list):
            for n in nodo:
                recorrer(n)
            return
        if not isinstance(nodo, dict):
            return
        tipo = nodo.get("@type")
        tipos = tipo if isinstance(tipo, list) else [tipo]
        if any(isinstance(t, str) and "Product" in t for t in tipos):
            img = nodo.get("image")
            for item in (img if isinstance(img, list) else [img]):
                if isinstance(item, str):
                    urls.append(item)
                elif isinstance(item, dict):
                    u = item.get("url") or item.get("contentUrl")
                    if isinstance(u, str):
                        urls.append(u)
        for clave in ("@graph", "mainEntity", "itemListElement", "item", "offers"):
            if clave in nodo:
                recorrer(nodo[clave])

    for bloque in re.findall(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, flags=re.I | re.S):
        bloque = bloque.strip()
        if not bloque:
            continue
        try:
            recorrer(json.loads(bloque))
        except (json.JSONDecodeError, ValueError):
            continue
    return [urljoin(base, html_lib.unescape(u)) for u in urls]


def _atributos(tag):
    return {k.lower(): html_lib.unescape(v) for k, v in re.findall(r'([a-zA-Z_:-]+)\s*=\s*["\']([^"\']*)["\']', tag)}


def _imagenes_de_meta(html, base):
    og, twitter, links = [], [], []
    for tag in re.findall(r"<meta\b[^>]*>", html, flags=re.I):
        a = _atributos(tag)
        clave = (a.get("property") or a.get("name") or "").lower()
        contenido = (a.get("content") or "").strip()
        if not contenido:
            continue
        if clave in ("og:image", "og:image:secure_url", "og:image:url"):
            og.append(contenido)
        elif clave in ("twitter:image", "twitter:image:src"):
            twitter.append(contenido)
    for tag in re.findall(r"<link\b[^>]*>", html, flags=re.I):
        a = _atributos(tag)
        if (a.get("rel") or "").lower() == "image_src" and a.get("href"):
            links.append(a["href"])
    return [urljoin(base, u) for u in og + twitter + links]


def _imagenes_de_img(html, base):
    encontradas = []
    for tag in re.findall(r"<img\b[^>]*>", html, flags=re.I):
        a = _atributos(tag)
        src = a.get("data-src") or a.get("data-original") or a.get("data-lazy-src") or a.get("src") or ""
        if not src and a.get("srcset"):
            src = a["srcset"].split(",")[0].strip().split(" ")[0]
        src = src.strip()
        if not src or src.startswith("data:"):
            continue
        bajo = src.lower()
        if any(p in bajo for p in _PISTAS_DESCARTE):
            continue
        if not (bajo.endswith(_EXT_IMAGEN) or any(p in bajo for p in _PISTAS_PRODUCTO)):
            continue
        encontradas.append(urljoin(base, src))
        if len(encontradas) >= 5:
            break
    return encontradas


def _dedupe(urls):
    vistas, salida = set(), []
    for u in urls:
        if not u or not _es_url_http(u):
            continue
        u = u.strip()
        if u in vistas:
            continue
        vistas.add(u)
        salida.append(u)
    return salida


def extraer_imagenes_de_pagina(url, html=None):
    """
    Devuelve URLs absolutas de imagen de producto encontradas en la página, en
    orden de confianza: JSON-LD Product.image > og:image/twitter:image/link >
    etiquetas <img> con pinta de catálogo. Máximo MAX_IMAGENES_POR_PAGINA.
    `html` permite pasar el contenido ya descargado (tests).
    """
    base = url
    if html is None:
        try:
            html, base = _descargar_html(url)
        except requests.RequestException:
            return []
        if not html:
            return []
    urls = _imagenes_de_jsonld(html, base) + _imagenes_de_meta(html, base) + _imagenes_de_img(html, base)
    urls = [u for u in _dedupe(urls) if not _dominio_bloqueado(u)]
    return urls[:MAX_IMAGENES_POR_PAGINA]


# ==========================================
# PASO 3: DESCARGA Y NORMALIZACIÓN (PILLOW 800x800)
# ==========================================
def normalizar_imagen(contenido):
    """Convierte bytes de imagen en un JPEG 800x800 con fondo blanco. Devuelve bytes o None."""
    try:
        img = Image.open(io.BytesIO(contenido))
        img.load()
    except Exception:
        return None

    if max(img.size) < LADO_MINIMO:
        return None

    if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
        img = img.convert("RGBA")
        fondo = Image.new("RGBA", img.size, (255, 255, 255, 255))
        fondo.paste(img, (0, 0), img)
        img = fondo.convert("RGB")
    else:
        img = img.convert("RGB")

    img.thumbnail((TAMANO_LIENZO, TAMANO_LIENZO), Image.Resampling.LANCZOS)
    lienzo = Image.new("RGB", (TAMANO_LIENZO, TAMANO_LIENZO), (255, 255, 255))
    lienzo.paste(img, ((TAMANO_LIENZO - img.width) // 2, (TAMANO_LIENZO - img.height) // 2))

    buffer = io.BytesIO()
    lienzo.save(buffer, format="JPEG", quality=90)
    return buffer.getvalue()


def descargar_y_estandarizar_imagen(url, referer=None):
    """Descarga la imagen y la devuelve normalizada (bytes JPEG 800x800) o None."""
    headers = dict(HEADERS_NAVEGADOR)
    headers["Accept"] = "image/avif,image/webp,image/apng,image/*,*/*;q=0.8"
    if referer:
        headers["Referer"] = referer
    try:
        r = requests.get(url, headers=headers, timeout=15)
    except requests.RequestException:
        return None
    if r.status_code != 200 or not r.content:
        return None
    return normalizar_imagen(r.content)


# ==========================================
# PASO 4: VERIFICACIÓN VISUAL CON CLAUDE
# ==========================================
PROMPT_SISTEMA_VISION = """Eres el control de calidad de fotos de catálogo de Farmacias LF (farmacia online de Chile).
Te mostraremos una imagen y el nombre de un producto. Evalúa dos cosas:
1. "coincide": la imagen muestra ese producto exacto (misma marca y misma presentación). Si muestra otro producto, otra dosis o cantidad claramente distinta, un pack cuando el producto es unitario (o al revés), o no se puede identificar el producto, responde false.
2. "es_foto_catalogo": es una foto limpia del envase o empaque, tipo ficha de producto; no un banner, logo, captura de pantalla, foto de uso o lifestyle, collage ni imagen con texto promocional grande.
Sé estricto con la marca y la presentación, y tolerante con diferencias menores de diseño del empaque (versiones antiguas o nuevas del mismo producto)."""

ESQUEMA_VERIFICACION = {
    "type": "object",
    "properties": {
        "coincide": {"type": "boolean"},
        "es_foto_catalogo": {"type": "boolean"},
        "motivo": {"type": "string"},
    },
    "required": ["coincide", "es_foto_catalogo", "motivo"],
    "additionalProperties": False,
}


def verificar_imagen(jpeg_bytes, titulo, laboratorio=""):
    """Devuelve (aprobada: bool, motivo: str)."""
    imagen_b64 = base64.standard_b64encode(jpeg_bytes).decode("utf-8")
    respuesta = _llamar_claude(
        model=MODELO,
        max_tokens=2048,
        system=PROMPT_SISTEMA_VISION,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": imagen_b64}},
                {"type": "text", "text": (
                    f"Producto esperado: {titulo}\n"
                    f"Laboratorio / marca: {laboratorio or 'desconocido'}\n"
                    "Responde con el JSON pedido."
                )},
            ],
        }],
        output_config={
            "effort": "low",
            "format": {"type": "json_schema", "schema": ESQUEMA_VERIFICACION},
        },
    )
    _registrar_uso(respuesta)
    if respuesta.stop_reason in ("refusal", "max_tokens"):
        return False, f"sin veredicto ({respuesta.stop_reason})"
    try:
        datos = json.loads(_texto_de_respuesta(respuesta))
    except (json.JSONDecodeError, TypeError):
        return False, "respuesta no interpretable"
    aprobada = bool(datos.get("coincide")) and bool(datos.get("es_foto_catalogo"))
    return aprobada, str(datos.get("motivo") or "")


# ==========================================
# ORQUESTACIÓN POR PRODUCTO
# ==========================================
def encontrar_imagen_producto(titulo, laboratorio="", descripcion_original="", log=None):
    """
    Busca, descarga, normaliza y verifica la foto de un producto.

    Devuelve dict {"jpeg": bytes, "url_imagen": str, "url_pagina": str|None, "tienda": str}
    o None si no se encontró ninguna foto aprobada.
    Lanza MotorNoDisponible / ErrorTransitorio cuando el problema es de la API.
    """
    log = log or (lambda *a, **k: None)
    candidatos = buscar_candidatos(titulo, laboratorio, descripcion_original)
    if not candidatos:
        log("sin candidatos")
        return None

    verificaciones = 0
    paginas = 0
    for cand in candidatos:
        urls = []
        if cand.get("url_imagen"):
            urls.append(cand["url_imagen"])
        if cand.get("url_pagina"):
            if paginas >= MAX_PAGINAS:
                break
            paginas += 1
            urls.extend(extraer_imagenes_de_pagina(cand["url_pagina"]))
        urls = _dedupe(urls)[:MAX_IMAGENES_POR_PAGINA + 1]
        if not urls:
            log(f"sin imágenes en {cand.get('tienda') or cand.get('url_pagina')}")
            continue

        for url_imagen in urls:
            jpeg = descargar_y_estandarizar_imagen(url_imagen, referer=cand.get("url_pagina"))
            if not jpeg:
                continue
            if VERIFICAR_VISION:
                if verificaciones >= MAX_VERIFICACIONES:
                    log("tope de verificaciones visuales alcanzado")
                    return None
                verificaciones += 1
                aprobada, motivo = verificar_imagen(jpeg, titulo, laboratorio)
                if not aprobada:
                    log(f"foto rechazada ({cand.get('tienda') or 'fuente'}): {motivo[:80]}")
                    continue
            return {
                "jpeg": jpeg,
                "url_imagen": url_imagen,
                "url_pagina": cand.get("url_pagina"),
                "tienda": cand.get("tienda") or "",
            }
    return None
