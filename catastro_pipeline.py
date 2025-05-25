# catastro_pipeline.py

import os
import time
import pandas as pd
import requests
import zipfile
import logging
import geopandas as gpd
from datetime import datetime
from sqlalchemy import create_engine, text, inspect
from geoalchemy2 import Geometry
import fiona


# --- Logging Setup ---
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/catastro_pipeline.log",
    filemode="a",
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# -----------------------------
# Configuration & Constants
# -----------------------------
MUNICIPALITIES = {
    "ALARO": "07001",
    "ALAIOR": "07002",
    "ALCUDIA": "07003",
    "ALGAIDA": "07004",
    "ANDRATX": "07005",
    "ARTA": "07006",
    "BANYALBUFAR": "07007",
    "BINISSALEM": "07008",
    "BUGER": "07009",
    "BUNYOLA": "07010",
    "CALVIA": "07011",
    "CAMPANET": "07012",
    "CAMPOS": "07013",
    "CAPDEPERA": "07014",
    "CIUTADELLA DE MENORCA": "07015",
    "CONSELL": "07016",
    "COSTITX": "07017",
    "DEIA": "07018",
    "ESCORCA": "07019",
    "ESPORLES": "07020",
    "ESTELLENCS": "07021",
    "FELANITX": "07022",
    "FERRERIES": "07023",
    "FORMENTERA": "07024",
    "FORNALUTX": "07025",
    "EIVISSA": "07026",
    "INCA": "07027",
    "LLORET DE VISTALEGRE": "07028",
    "LLOSETA": "07029",
    "LLUBI": "07030",
    "LLUCMAJOR": "07031",
    # "MAO": "07032",
    "MANACOR": "07033",
    "MANCOR DE LA VALL": "07034",
    "MARIA DE LA SALUT": "07035",
    "MARRATXI": "07036",
    "ES MERCADAL": "07037",
    "MONTUIRI": "07038",
    "MURO": "07039",
    "PALMA": "07040",
    "PETRA": "07041",
    # "POLLENÃ‡A": "07042",
    "PORRERES": "07043",
    "SA POBLA": "07044",
    "PUIGPUNYENT": "07045",
    "SANT ANTONI DE PORTMANY": "07046",
    "SENCELLES": "07047",
    "SANT JOSEP DE SA TALAIA": "07048",
    "SANT JOAN": "07049",
    "SANT JOAN DE LABRITJA": "07050",
    # "SANT LLORENC DES CARDASSAR": "07051",
    "SANT LLUIS": "07052",
    "SANTA EUGENIA": "07053",
    "SANTA EULARIA DES RIU": "07054",
    "SANTA MARGALIDA": "07055",
    "SANTA MARIA DEL CAMI": "07056",
    "SANTANYI": "07057",
    "SELVA": "07058",
    "SES SALINES": "07059",
    "SINEU": "07060",
    "SOLLER": "07061",
    "SON SERVERA": "07062",
    "VALLDEMOSSA": "07063",
    "ES CASTELL": "07064",
    # "VILAFRANCA BONANY": "07065",
    "ARIANY": "07066",
    "ES MIGJORN GRAN": "07067"
}

BASE_URL = "http://www.catastro.hacienda.gob.es/INSPIRE/{layer}/07/{code}-{name}/A.ES.SDGC.{layer_code}.{code}.zip"
GML_DIR = "Balearic_GML"
OUTPUT_DIR = "Enriched_Output"
POSTGIS_CONN = "postgresql+psycopg2://username:password@localhost:5432/postgis_catastro"
PARCEL_TABLE = "catastro_parcels"
UNIT_TABLE = "catastro_units"
SLEEP_BETWEEN_REQUESTS = 15
BATCH_SAVE_INTERVAL = 100

API_URL = "https://ovc.catastro.meh.es/OVCServWeb/OVCWcfCallejero/COVCCallejero.svc/json/Consulta_DNPRC?RefCat={refcat}"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www1.sedecatastro.gob.es/"
}

# Ensure necessary directories exist
os.makedirs(GML_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Connect to PostGIS
engine = create_engine(POSTGIS_CONN)

def download_and_extract(url, extract_to):
    """
    Downloads and extracts a ZIP archive from the given URL into the specified directory.
    Returns the path to the first .gml file extracted (or None if failure).
    """
    try:
        zip_path = os.path.join(extract_to, os.path.basename(url))
        response = requests.get(url)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            f.write(response.content)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
        return next((os.path.join(extract_to, f) for f in os.listdir(extract_to) if f.endswith(".gml")), None)
    except Exception as e:
        logger.error(f"Failed to download or extract {url}: {e}")
        return None

def process_municipality(name, code):
    """
    Processes a single municipality: downloads, extracts, filters, and inserts cadastral parcel data.
    """
    try:
        logger.info(f"Processing municipality: {name} ({code})")
        layer_dir = os.path.join(GML_DIR, name, "CP")
        os.makedirs(layer_dir, exist_ok=True)
        url = BASE_URL.format(layer="CadastralParcels", code=code, name=name, layer_code="CP")
        gml_path = download_and_extract(url, layer_dir)
        if not gml_path:
            return

        gdf_cp = gpd.read_file(gml_path).to_crs(epsg=25830)
        gdf_cp["referencia_catastral"] = gdf_cp["nationalCadastralReference"]
        gdf_cp["municipio"] = name
        gdf_cp["codigo_municipio"] = code
        gdf_cp["provincia"] = "Illes Balears"
        gdf_cp["codigo_provincia"] = "07"
        gdf_cp["last_update"] = datetime.now()

            # Ensure projection and calculate area
        if gdf.crs.to_epsg() != 25830:
            print(f"🔄 Reprojecting from {gdf.crs.to_epsg()} to EPSG:25830...")
            gdf_proj = gdf.to_crs(epsg=25830)
            gdf["superficie_parcela"] = gdf_proj.geometry.area
            gdf = gdf.to_crs(epsg=25830)
        else:
            gdf["superficie_parcela"] = gdf.geometry.area
        
        gdf["uso_parcela"] = gdf.get("landUse", None)

        gdf_cp = gdf_cp[[
            "referencia_catastral", "municipio", "codigo_municipio", "provincia",
            "codigo_provincia", "geometry", "superficie_parcela", "uso_parcela"
        ]]

        # Deduplicate against existing DB records
        existing_refs = pd.read_sql(f"SELECT referencia_catastral FROM {PARCEL_TABLE}", engine)
        # Ensure type and formatting consistency
        gdf_cp["referencia_catastral"] = gdf_cp["referencia_catastral"].astype(str).str.strip()
        existing_refs["referencia_catastral"] = existing_refs["referencia_catastral"].astype(str).str.strip()

        # Remove duplicates within the new dataset first
        gdf_cp = gdf_cp.drop_duplicates(subset="referencia_catastral")

        # Drop rows already in DB
        gdf_cp = gdf_cp[~gdf_cp["referencia_catastral"].isin(existing_refs["referencia_catastral"])]

        # Insert into PostGIS and export to GeoJSON
        gdf_cp.to_postgis(PARCEL_TABLE, con=engine, if_exists='append', index=False)
        gdf_cp.to_file(os.path.join(OUTPUT_DIR, f"{name}_catastro_parcels.geojson"), driver="GeoJSON")
        logger.info(f"Inserted {len(gdf_cp)} new parcels for {name}.")
    except Exception as e:
        logger.error(f"Error processing municipality {name}: {e}")

def safe_float(value):
    """
    Converts a string value with comma decimal separator into float safely.
    Returns None if input is invalid.
    """
    try:
        return float(str(value).replace(",", ".")) if value not in [None, ""] else None
    except:
        return None

def extract_unit(unit, single=False):
    """
    Extracts and transforms unit-level information into a dictionary for either
    a single-unit response or multi-unit list format from Catastro API.
    """
    if single:
        ref = unit.get("idbi", {}).get("rc", {})
        dt = unit.get("dt", {})
        debi = unit.get("debi", {})
        loint = dt.get("locs", {}).get("lous", {}).get("lourb", {}).get("loint", {})
        dir_data = dt.get("locs", {}).get("lous", {}).get("lourb", {}).get("dir", {})
        return {
            "parcel_ref": f"{ref.get('pc1')}{ref.get('pc2')}",
            "unit_ref": unit.get("idbi", {}).get("cn"),
            "use_type": debi.get("luso"),
            "floor_area": safe_float(debi.get("sfc")),
            "year_built": debi.get("ant"),
            "participation": safe_float(debi.get("cpt")),
            "street_name": dir_data.get("nv"),
            "floor": loint.get("pt"),
            "door": loint.get("pu"),
            "postal_code": dt.get("locs", {}).get("lous", {}).get("lourb", {}).get("dp"),
            "municipality": dt.get("nm"),
            "province": dt.get("np"),
            "last_update": pd.Timestamp.now()
        }
    else:
        ref = unit.get("rc", {})
        dt = unit.get("dt", {})
        debi = unit.get("debi", {})
        loint = dt.get("locs", {}).get("lous", {}).get("lourb", {}).get("loint", {})
        dir_data = dt.get("locs", {}).get("lous", {}).get("lourb", {}).get("dir", {})
        return {
            "parcel_ref": f"{ref.get('pc1')}{ref.get('pc2')}",
            "unit_ref": f"{ref.get('pc1')}{ref.get('pc2')}{ref.get('car')}{ref.get('cc1')}{ref.get('cc2')}",
            "use_type": debi.get("luso"),
            "floor_area": safe_float(debi.get("sfc")),
            "year_built": debi.get("ant"),
            "participation": safe_float(debi.get("cpt")),
            "street_name": dir_data.get("nv"),
            "floor": loint.get("pt"),
            "door": loint.get("pu"),
            "postal_code": dt.get("locs", {}).get("lous", {}).get("lourb", {}).get("dp"),
            "municipality": dt.get("nm"),
            "province": dt.get("np"),
            "last_update": pd.Timestamp.now()
        }

def fetch_units_for_parcel(refcat, retries=3, delay=5):
    """
    Makes a request to the Catastro API for a given parcel reference.
    Handles retries, 403 responses, and JSON structure variations.
    """
    for attempt in range(retries):
        try:
            response = requests.get(API_URL.format(refcat=refcat), headers=HEADERS, timeout=15)
            if response.status_code == 200:
                data = response.json()
                result = []
                units = data.get("consulta_dnprcResult", {}).get("lrcdnp", {}).get("rcdnp", [])
                if units:
                    return [extract_unit(u) for u in units]
                unit = data.get("consulta_dnprcResult", {}).get("bico", {}).get("bi", {})
                if unit:
                    return [extract_unit(unit, single=True)]
            elif response.status_code == 403:
                logger.warning(f"Rate limited for {refcat}, sleeping {delay}s.")
                time.sleep(delay)
        except Exception as e:
            logger.error(f"Failed to fetch units for {refcat}: {e}")
        time.sleep(delay * (2 ** attempt))
    return []

def extract_units(municipality):
    """
    For a given municipality, fetches unit-level data for all parcels
    not yet processed and stores them in the database.
    """
    try:
        with engine.connect() as conn:
            parcels = pd.read_sql(
                text(f"SELECT referencia_catastral FROM {PARCEL_TABLE} WHERE municipio = :municipio"),
                conn, params={"municipio": municipality}
            )
            inspector = inspect(engine)
            existing_refs = set()
            if inspector.has_table(UNIT_TABLE):
                try:
                    df = pd.read_sql(f"SELECT DISTINCT parcel_ref FROM {UNIT_TABLE}", conn)
                    existing_refs = set(df["parcel_ref"].dropna())
                except Exception as e:
                    logger.warning(f"Could not load existing unit refs: {e}")
            refs = [r for r in parcels["referencia_catastral"] if r not in existing_refs]

        batch = []
        for i, refcat in enumerate(refs):
            logger.info(f"Fetching units for {refcat} ({i+1}/{len(refs)})")
            batch.extend(fetch_units_for_parcel(refcat))
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            if len(batch) >= BATCH_SAVE_INTERVAL:
                pd.DataFrame(batch).to_sql(UNIT_TABLE, con=engine, if_exists='append', index=False)
                logger.info(f"Inserted {len(batch)} unit records.")
                batch = []

        if batch:
            pd.DataFrame(batch).to_sql(UNIT_TABLE, con=engine, if_exists='append', index=False)
            logger.info(f"Inserted remaining {len(batch)} unit records.")
    except Exception as e:
        logger.error(f"Failed unit extraction for {municipality}: {e}")

# Entrypoint
if __name__ == "__main__":
    for name, code in MUNICIPALITIES.items():
        process_municipality(name, code)
        extract_units(name)