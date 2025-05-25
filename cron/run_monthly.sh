#!/bin/bash
source ~/miniconda3/bin/activate catastro_env
cd /path/to/catastro-etl
python catastro_pipeline.py >> logs/pipeline_$(date +\%Y-\%m-\%d).log 2>&1
