"""
ws_vivienda.py
Scraper de inmuebles en MercadoLibre México.
Pensado para correrse con cron en Linux.

Uso manual:
    python ws_vivienda.py

Con cron (ejemplo: todos los días a las 3 AM):
    0 3 * * * /usr/bin/python3 /ruta/a/ws_vivienda.py >> /ruta/a/logs/ws_vivienda.log 2>&1
"""

# ── Librerías ──────────────────────────────────────────────────────────────────
import os
import re
import time
import json
import random
import logging
import requests
import pandas as pd
import signal
from datetime import date, datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
from collections import namedtuple
from datetime import date

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "ws_vivienda.log")),
        logging.StreamHandler(),          # también imprime en consola
    ],
)
log = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────
PAGINAS          = 4
ESPERA_S         = 30   # pausa entre páginas (segundos)
RUTA_ARCHIVO     = "ws_vivienda.csv"
RUTA_CHECKPOINT  = "checkpoint.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9",
}

# ── Generación de URLs ─────────────────────────────────────────────────────────
base       = "https://inmuebles.mercadolibre.com.mx"
tipos      = ["casas", "departamentos"]
operaciones = ["venta", "renta"]

cdmx_alcaldias = [
    "alvaro-obregon", "azcapotzalco", "benito-juarez", "coyoacan",
    "cuajimalpa-de-morelos", "cuauhtemoc", "gustavo-a-madero", "iztacalco",
    "iztapalapa", "la-magdalena-contreras", "miguel-hidalgo", "milpa-alta",
    "tlahuac", "tlalpan", "venustiano-carranza", "xochimilco",
]

edomex_metro = [
    "atizapan-de-zaragoza", "coacalco-de-berriozabal", "cuautitlan",
    "cuautitlan-izcalli", "chalco", "chicoloapan", "chimalhuacan",
    "ecatepec-de-morelos", "huixquilucan", "ixtapaluca", "la-paz",
    "naucalpan-de-juarez", "nezahualcoyotl", "nicolas-romero", "tecamac",
    "teoloyucan", "tepotzotlan", "texcoco", "tlalnepantla-de-baz",
    "tultitlan", "valle-de-chalco-solidaridad", "zumpango",
]

estados_generales = [
    "aguascalientes", "baja-california", "baja-california-sur", "campeche",
    "chiapas", "chihuahua", "coahuila", "colima", "durango", "guanajuato",
    "guerrero", "hidalgo", "jalisco", "michoacan", "morelos", "nayarit",
    "nuevo-leon", "oaxaca", "puebla", "queretaro", "quintana-roo",
    "san-luis-potosi", "sinaloa", "sonora", "tabasco", "tamaulipas",
    "tlaxcala", "veracruz", "yucatan", "zacatecas",
]

ciudades_grandes = {
    "aguascalientes":      ["aguascalientes", "jesus-maria"],
    "baja-california":     ["tijuana", "mexicali", "ensenada", "tecate", "playas-de-rosarito"],
    "baja-california-sur": ["la-paz", "los-cabos"],
    "campeche":            ["campeche", "carmen"],
    "chiapas":             ["tuxtla-gutierrez", "san-cristobal-de-las-casas", "tapachula"],
    "chihuahua":           ["chihuahua", "juarez", "delicias"],
    "coahuila":            ["saltillo", "torreon", "monclova", "piedras-negras", "ramos-arizpe"],
    "colima":              ["colima", "manzanillo", "villa-de-alvarez"],
    "durango":             ["durango", "gomez-palacio", "lerdo"],
    "guanajuato":          ["leon", "celaya", "irapuato", "salamanca", "guanajuato", "san-miguel-de-allende"],
    "guerrero":            ["acapulco-de-juarez", "chilpancingo-de-los-bravo", "iguala-de-la-independencia"],
    "hidalgo":             ["pachuca-de-soto", "mineral-de-la-reforma", "tizayuca", "tula-de-allende"],
    "jalisco":             ["guadalajara", "zapopan", "tlajomulco-de-zuniga", "san-pedro-tlaquepaque", "tonala", "puerto-vallarta"],
    "michoacan":           ["morelia", "uruapan", "zamora", "lazaro-cardenas"],
    "morelos":             ["cuernavaca", "jiutepec", "temixco", "yautepec", "emiliano-zapata", "xochitepec", "cuautla"],
    "nayarit":             ["tepic", "bahia-de-banderas"],
    "nuevo-leon":          ["monterrey", "guadalupe", "san-nicolas-de-los-garza", "apodaca", "general-escobedo", "santa-catarina", "san-pedro-garza-garcia", "garcia", "juarez"],
    "oaxaca":              ["oaxaca-de-juarez", "santa-cruz-xoxocotlan", "san-juan-bautista-tuxtepec"],
    "puebla":              ["puebla", "san-andres-cholula", "san-pedro-cholula", "cuautlancingo", "tehuacan", "coronango", "atlixco"],
    "queretaro":           ["queretaro", "el-marques", "corregidora", "san-juan-del-rio"],
    "quintana-roo":        ["benito-juarez", "solidaridad", "tulum", "othon-p-blanco", "isla-mujeres", "puerto-morelos"],
    "san-luis-potosi":     ["san-luis-potosi", "soledad-de-graciano-sanchez"],
    "sinaloa":             ["culiacan", "mazatlan", "ahome", "guasave"],
    "sonora":              ["hermosillo", "cajeme", "nogales", "san-luis-rio-colorado"],
    "tabasco":             ["centro", "paraiso", "comalcalco"],
    "tamaulipas":          ["reynosa", "matamoros", "nuevo-laredo", "tampico", "ciudad-madero", "altamira", "victoria"],
    "tlaxcala":            ["tlaxcala", "apizaco", "chiautempan", "huamantla"],
    "veracruz":            ["veracruz", "boca-del-rio", "xalapa", "coatzacoalcos", "cordoba", "orizaba", "poza-rica-de-hidalgo"],
    "yucatan":             ["merida", "progreso", "valladolid"],
    "zacatecas":           ["zacatecas", "guadalupe", "fresnillo"],
}

urls = []
urls += [f"{base}/{t}/{o}/distrito-federal/{a}/"   for a in cdmx_alcaldias   for t in tipos for o in operaciones]
urls += [f"{base}/{t}/{o}/estado-de-mexico/{m}/"   for m in edomex_metro      for t in tipos for o in operaciones]
urls += [f"{base}/{t}/{o}/{e}/"                    for e in estados_generales for t in tipos for o in operaciones]
urls += [f"{base}/{t}/{o}/{e}/{c}/"
         for e, ciudades in ciudades_grandes.items()
         for c in ciudades for t in tipos for o in operaciones]
urls = list(dict.fromkeys(urls))   # elimina duplicados manteniendo orden
log.info(f"Total URLs generadas: {len(urls)}")

# ── Namedtuple ─────────────────────────────────────────────────────────────────
Articulo = namedtuple(
    "Articulo",
    ["fecha", "titulo", "recamaras", "banos", "superficie_m2",
     "precio_actual", "link", "imagen"]
)

# ── Funciones auxiliares ───────────────────────────────────────────────────────
def guardar_dataframe(df: pd.DataFrame, ruta: str) -> None:
    """Guarda (o agrega) el dataframe en CSV, deduplicando por link."""
    if os.path.exists(ruta):
        df_existente = pd.read_csv(ruta)
        df_combinado = pd.concat([df_existente, df], ignore_index=True)
        df_combinado.drop_duplicates(subset=["link"], inplace=True)
        df_combinado.to_csv(ruta, index=False)
    else:
        df.to_csv(ruta, index=False)


def extraer_datos_url(url: str):
    """Extrae tipo_vivienda, operacion, estado y municipio de la URL."""
    m1 = re.search(r"/(casas|departamentos)/(venta|renta)/([^/]+)/([^/]+)/", url)
    if m1:
        tipo_vivienda, operacion, estado, municipio = m1.groups()
        return tipo_vivienda, operacion, estado, municipio

    m2 = re.search(r"/(casas|departamentos)/(venta|renta)/([^/]+)/", url)
    if m2:
        tipo_vivienda, operacion, estado = m2.groups()
        return tipo_vivienda, operacion, estado, None

    return None, None, None, None


def limpia_precio(txt: str):
    if not txt:
        return None
    t = txt.replace("\xa0", " ").replace(".", "").replace(",", "").replace("$", "").strip()
    try:
        return int("".join(ch for ch in t if ch.isdigit()))
    except ValueError:
        return None


def parse_item(li) -> Articulo:
    """Parsea un elemento <li> de la lista de resultados de ML."""
    titulo = link = None
    a = li.select_one("h3.poly-component__title-wrapper a.poly-component__title[href]")
    if a:
        titulo = a.get_text(strip=True)
        link   = a["href"]

    img    = li.find("img")
    imagen = (img.get("data-src") or img.get("src")) if img else None

    precio_actual = None
    actual_span   = li.select_one(".poly-price__current .andes-money-amount__fraction")
    if actual_span:
        precio_actual = limpia_precio(actual_span.get_text())

    recamaras = banos = superficie_m2 = None
    for it in li.select("ul.poly-attributes_list li.poly-attributes_list__item"):
        txt = it.get_text(" ", strip=True).lower()
        m = re.search(r"(\d+)\s*(recámaras|recamaras|habitaciones|dormitorios)", txt)
        if m:
            recamaras = int(m.group(1))
            continue
        m = re.search(r"(\d+)\s*(baños|banos)", txt)
        if m:
            banos = int(m.group(1))
            continue
        m = re.search(r"(\d+(?:\.\d+)?)\s*m²", txt)
        if m:
            superficie_m2 = float(m.group(1))

    return Articulo(date.today(), titulo, recamaras, banos, superficie_m2,
                    precio_actual, link, imagen)


def get_lat_lon(url: str, headers=HEADERS):
    """Extrae latitud y longitud del HTML del anuncio individual."""
    url = url.split("?")[0].split("#")[0]
    try:
        resp = requests.get(url, headers=headers, timeout=25)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"Error al obtener coordenadas: {e}")
        return None, None

    m = re.search(
        r'"map_info".*?"location":\{"latitude":"?([-0-9.]+)"?,"longitude":"?([-0-9.]+)"?\}',
        resp.text,
        flags=re.DOTALL,
    )
    if m:
        return float(m.group(1)), float(m.group(2))

    log.info("No se encontraron coordenadas")
    return None, None


def get_descripcion(url: str, headers=HEADERS):
    """Devuelve la descripción completa del anuncio. Retorna None si no existe."""
    try:
        resp = requests.get(url, headers=headers, timeout=25)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"Error al obtener descripción: {e}")
        return None

    soup      = BeautifulSoup(resp.text, "html.parser")
    desc_node = soup.select_one("p.ui-pdp-description__content")
    if not desc_node:
        desc_node = soup.select_one("[data-testid='content']")
    if not desc_node:
        log.info("No se encontró descripción en la página.")
        return None

    time.sleep(random.randint(30, 60))
    return desc_node.get_text(separator=" ", strip=True)


def scrap_page(paginas: int, url: str, headers=HEADERS) -> pd.DataFrame:
    """
    Scrapea múltiples páginas de resultados de una URL de ML.
    Retorna un DataFrame con todos los artículos encontrados.
    """
    articulos = []
    for page in range(1, paginas + 1):
        log.info(f"Scrapeando página {page}")
        sep     = "" if url.endswith("/") else "/"
        url_pag = url if page == 1 else f"{url}{sep}_Desde_{(page - 1) * 48 + 1}"

        try:
            resp = requests.get(url_pag, headers=headers, timeout=20)
            resp.raise_for_status()
        except requests.HTTPError as e:
            log.warning(f"{e} -> {url_pag}")
            break

        soup  = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("li.ui-search-layout__item, li.ui-search-layout__item--stack")
        if not items:
            items = soup.select("li.ui-search-result__wrapper")
        if not items:
            log.warning(f"No se hallaron items en página {page}.")
            break

        for li in items:
            try:
                articulos.append(parse_item(li))
            except Exception as exc:
                log.warning(f"Item con error: {exc}")

        time.sleep(ESPERA_S)

    return pd.DataFrame(articulos)


# ── Scraping principal ─────────────────────────────────────────────────────────
def main():
    # Cargar checkpoint para reanudar si el proceso fue interrumpido
    if os.path.exists(RUTA_CHECKPOINT):
        with open(RUTA_CHECKPOINT) as f:
            urls_procesadas = set(json.load(f))
        log.info(f"Reanudando: {len(urls_procesadas)} URLs ya procesadas")
    else:
        urls_procesadas = set()

    urls_pendientes = [u for u in urls if u not in urls_procesadas]
    log.info(f"URLs pendientes: {len(urls_pendientes)}")

    for i, url_base in enumerate(tqdm(urls_pendientes)):
        # ── Verificar hora límite ──────────────────────────
        if datetime.now().hour >= 7:
            log.info("⏰ Hora límite alcanzada, terminando limpiamente...")
            break
        log.info(f"[{i+1}/{len(urls_pendientes)}] {url_base}")

        try:
            df_scrap = scrap_page(PAGINAS, url_base)
        except Exception as e:
            log.error(f"Falló scrap_page en {url_base}: {e}")
            continue

        if df_scrap is None or df_scrap.empty:
            log.warning(f"No se obtuvo información para {url_base}")
            urls_procesadas.add(url_base)
            continue

        log.info(f"Scraped: {df_scrap.shape[0]} artículos")

        # Coordenadas
        lats, lons = [], []
        for j in range(df_scrap.shape[0]):
            log.info(f"  Coordenadas {j+1}/{df_scrap.shape[0]}")
            lat, lon = get_lat_lon(df_scrap.loc[j, "link"])
            lats.append(lat)
            lons.append(lon)
            time.sleep(random.randint(15, 30))

        df_scrap["latitud"]  = lats
        df_scrap["longitud"] = lons

        # Descripciones
        descs = []
        for j in range(df_scrap.shape[0]):
            log.info(f"  Descripción {j+1}/{df_scrap.shape[0]}")
            descs.append(get_descripcion(df_scrap.loc[j, "link"]))
            time.sleep(random.randint(15, 30))

        df_scrap["descripcion"] = descs

        # Metadatos de la URL
        tipo_vivienda, operacion, estado, municipio = extraer_datos_url(url_base)
        df_scrap["tipo_vivienda"] = tipo_vivienda
        df_scrap["operacion"]     = operacion
        df_scrap["estado"]        = estado
        df_scrap["municipio"]     = municipio

        guardar_dataframe(df_scrap, RUTA_ARCHIVO)

        # Guardar checkpoint
        urls_procesadas.add(url_base)
        with open(RUTA_CHECKPOINT, "w") as f:
            json.dump(list(urls_procesadas), f)

    log.info("✅ Scraping completado.")


if __name__ == "__main__":
    main()
