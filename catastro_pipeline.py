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
    "MAO MAHON": "07032",
    "MANACOR": "07033",
    "MANCOR DE LA VALL": "07034",
    "MARIA DE LA SALUT": "07035",
    "MARRATXI": "07036",
    "ES MERCADAL": "07037",
    "MONTUIRI": "07038",
    "MURO": "07039",
    "PALMA": "07040",
    "PETRA": "07041",
    "POLLEN A": "07042",
    "PORRERES": "07043",
    "SA POBLA": "07044",
    "PUIGPUNYENT": "07045",
    "SANT ANTONI DE PORTMANY": "07046",
    "SENCELLES": "07047",
    "SANT JOSEP DE SA TALAIA": "07048",
    "SANT JOAN": "07049",
    "SANT JOAN DE LABRITJA": "07050",
    "SANT LLOREN DES CARDASSAR": "07051",
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
    "VILAFRANCA DE BONANY": "07065",
    "ARIANY": "07066",
    "ES MIGJORN GRAN": "07067"
}

BASE_URL = "http://www.catastro.hacienda.gob.es/INSPIRE/{layer}/07/{code}-{name}/A.ES.SDGC.{layer_code}.{code}.zip"
GML_DIR = "Balearic_GML"
OUTPUT_DIR = "Enriched_Output"
POSTGIS_CONN = "postgresql+psycopg2://postgres:Abimbola@localhost:5432/postgis_catastro"
PARCEL_TABLE = "catastro_parcels"
UNIT_TABLE = "catastro_units"
SLEEP_BETWEEN_REQUESTS = 15
BATCH_SAVE_INTERVAL = 10

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
        if gdf_cp.crs.to_epsg() != 25830:
            print(f"🔄 Reprojecting from {gdf_cp.crs.to_epsg()} to EPSG:25830...")
            gdf_proj = gdf_cp.to_crs(epsg=25830)
            gdf_cp["superficie_parcela"] = gdf_proj.geometry.area
            gdf_cp = gdf_cp.to_crs(epsg=25830)
        else:
            gdf_cp["superficie_parcela"] = gdf_cp.geometry.area
        
        gdf_cp["uso_parcela"] = gdf_cp.get("landUse", None)

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
    Fetches detailed unit and building data from Catastro API for a given cadastral reference (refcat).
    
    This function handles both single-unit and multi-unit JSON responses, extracts usable attributes
    for cadastral units and associated buildings (if available), and returns structured lists.
    
    Args:
        refcat (str): Cadastral reference (referencia catastral) to query.
        retries (int): Number of retry attempts on failure or rate-limiting.
        delay (int): Delay in seconds between retries (with exponential backoff).

    Returns:
        dict: {
            'units': list of unit dicts,
            'buildings': list of building dicts (if present)
        }
    """
    for attempt in range(retries):
        try:
            response = requests.get(API_URL.format(refcat=refcat), headers=HEADERS, timeout=15)
            if response.status_code == 200:
                data = response.json()
                result = {"units": [], "buildings": []}
                main_data = data.get("consulta_dnprcResult", {})

                # Multi-unit response
                units = main_data.get("lrcdnp", {}).get("rcdnp", [])
                if units:
                    result["units"] = [extract_unit(u) for u in units]
                    return result

                # Single unit response
                bico = main_data.get("bico", {})
                bi = bico.get("bi", {})
                if isinstance(bi, list):
                    bi = bi[0]
                if bi:
                    unit_data = extract_unit(bi, single=True)
                    result["units"] = [unit_data]

                    lcons = bico.get("lcons", [])
                    if lcons:
                        logger.info(f"🏗️ Found {len(lcons)} building(s) for parcel {refcat}")
                        result["buildings"] = extract_buildings_from_lcons(
                            refcat,
                            unit_data.get("municipality"),
                            unit_data.get("province"),
                            lcons
                        )
                    return result
            elif response.status_code == 403:
                logger.warning(f"Rate limited for {refcat}, sleeping {delay}s.")
                time.sleep(delay)
        except Exception as e:
            logger.error(f"Failed to fetch units for {refcat}: {e}")
        time.sleep(delay * (2 ** attempt))
    return {"units": [], "buildings": []}


def extract_buildings_from_lcons(refcat, municipality, province, lcons):
    """
    Parses and extracts individual building data from the `lcons` list provided in Catastro API response.
    
    Each building includes attributes like building type, use description, built area,
    and interior identifiers such as floor, door, and staircase.

    Args:
        refcat (str): Parcel reference the buildings belong to.
        municipality (str): Municipality name for reference.
        province (str): Province name for reference.
        lcons (list): List of building construction entries from API.

    Returns:
        list: List of dictionaries representing buildings ready for database insert.
    """
    buildings = []
    for b in lcons:
        loint = b.get("dt", {}).get("lourb", {}).get("loint", {})
        dfcons = b.get("dfcons", {})
        dvcons = b.get("dvcons", {})

        buildings.append({
            "parcel_ref": refcat,
            "building_type": b.get("lcd"),
            "description": dvcons.get("dtip"),
            "built_area": safe_float(dfcons.get("stl")),
            "staircase": loint.get("es"),
            "floor": loint.get("pt"),
            "door": loint.get("pu"),
            "municipality": municipality,
            "province": province,
            "last_update": datetime.now()
        })
    return buildings

def extract_units(municipality):
    """
    For a given municipality, fetches unit-level data for all parcels
    not yet processed and stores both unit and building details in the database.
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

        batch_units = []
        batch_buildings = []  # ✅ Add this line

        for i, refcat in enumerate(refs):
            logger.info(f"Fetching units for {refcat} ({i+1}/{len(refs)})")
            result = fetch_units_for_parcel(refcat)

            batch_units.extend(result["units"])
            batch_buildings.extend(result["buildings"])  # ✅ Collect building data too

            time.sleep(SLEEP_BETWEEN_REQUESTS)

            if len(batch_units) >= BATCH_SAVE_INTERVAL:
                pd.DataFrame(batch_units).to_sql(UNIT_TABLE, con=engine, if_exists='append', index=False)
                logger.info(f"Inserted {len(batch_units)} unit records.")
                batch_units = []

            if len(batch_buildings) >= BATCH_SAVE_INTERVAL:
                logger.info(f"🧱 Prepared {len(batch_buildings)} building records for insertion.")
                pd.DataFrame(batch_buildings).to_sql("catastro_buildings", con=engine, if_exists='append', index=False)
                logger.info(f"Inserted {len(batch_buildings)} building records.")
                batch_buildings = []

        if batch_units:
            df = pd.DataFrame(batch_units)
            df.to_sql(UNIT_TABLE, con=engine, if_exists='append', index=False)
            logger.info(f"Inserted final {len(df)} unit records for {municipality}.")

            # Add geometry to units
            with engine.connect() as conn:
                gdf_parcels = gpd.read_postgis(
                    f"SELECT referencia_catastral, geometry FROM {PARCEL_TABLE} WHERE municipio = %s",
                    conn,
                    params=(municipality,),
                    geom_col='geometry'
                )

            df_merged = pd.merge(df, gdf_parcels, left_on="parcel_ref", right_on="referencia_catastral", how="left")
            df_merged.drop(columns=["referencia_catastral"], errors="ignore", inplace=True)
            gdf_units = gpd.GeoDataFrame(df_merged, geometry="geometry", crs="EPSG:25830")
            gdf_units = gdf_units[~gdf_units.geometry.isna()]
            gdf_units.to_file(os.path.join(OUTPUT_DIR, f"{municipality}_catastro_units.geojson"), driver="GeoJSON")
            # Save GeoDataFrame (with geometry) to PostGIS
            gdf_units.to_postgis(UNIT_TABLE, con=engine, if_exists='append', index=False)
            logger.info(f"Exported {len(gdf_units)} unit records with geometry.")

        if batch_buildings:
            logger.info(f"🧱 Final batch: preparing {len(batch_buildings)} building records for {municipality}")
            dfb = pd.DataFrame(batch_buildings)
            dfb.to_sql("catastro_buildings", con=engine, if_exists='append', index=False)
            logger.info(f"Inserted final {len(dfb)} building records for {municipality}.")

    except Exception as e:
        logger.error(f"Failed unit/building extraction for {municipality}: {e}")

# Entrypoint
if __name__ == "__main__":
    for name, code in MUNICIPALITIES.items():
        process_municipality(name, code)
        extract_units(name)