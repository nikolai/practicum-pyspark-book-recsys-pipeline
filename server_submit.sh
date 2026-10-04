#!/bin/bash
set -e
cd "$(dirname "$0")"

set -a
source spark.env
set +a

spark-submit \
  --master yarn \
  --conf spark.driver.memory=4g \
  main.py \
  --audition_path s3a://s3-ds-source/audition.parquet \
  --content_path s3a://s3-ds-source/content.parquet \
  --output_path "s3a://${S3_BUCKET}/results/S24_project_data" \
  "$@"
