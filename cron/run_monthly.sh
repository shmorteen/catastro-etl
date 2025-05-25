#!/bin/bash
source ~/miniconda3/bin/activate catastro_env
cd /path/to/catastro-etl
python catastro_pipeline.py >> logs/pipeline_$(date +\%Y-\%m-\%d).log 2>&1

crontab -e
0 2 1 * * /bin/bash /home/youruser/catastro-pipeline/cron/monthly.sh
