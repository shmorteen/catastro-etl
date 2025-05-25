# 🗺️ Catastro ETL Pipeline for Balearic Islands

A robust Python pipeline to **download**, **parse**, and **store** cadastral parcel and unit-level property data from the Spanish Catastro INSPIRE and Consulta_DNPRC services into a **PostGIS-enabled PostgreSQL** database.

---

## 📌 Features

- ✅ GML download & extraction from [Catastro INSPIRE](https://www.catastro.hacienda.gob.es/)
- ✅ API integration with `Consulta_DNPRC` for unit-level metadata
- ✅ Parcel and unit data saved into `catastro_parcels` and `catastro_units`
- ✅ GeoJSON export of enriched datasets
- ✅ Logging, retry logic, exception handling
- ✅ CRON-ready for automated monthly updates

---

## 📁 Project Structure

```bash
catastro-pipeline/
├── catastro_pipeline.py     # Main script (entrypoint)
├── logs/                    # Log output
├── Balearic_GML/            # Temporary GML storage
├── Enriched_Output/         # Output GeoJSON files
├── requirements.txt         # Python dependencies
└── README.md                # This file

## Run the Pipeline
```bash
python catastro_pipeline.py

