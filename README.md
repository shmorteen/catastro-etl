# Spanish Catastro ETL Pipeline

This Python pipeline automates extraction and storage of parcel and unit data for Balearic municipalities from the Catastro (land registry).

## Features

- GML downloads from Inspire services
- JSON unit detail via Consulta_DNPRC
- PostgreSQL/PostGIS storage
- Monthly automation-ready (cron)
- GeoJSON export

## Run the Pipeline
```bash
python catastro_pipeline.py
