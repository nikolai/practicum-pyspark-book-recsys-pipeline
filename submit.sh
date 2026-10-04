#!/bin/bash
set -e
cd "$(dirname "$0")"

# hadoop-aws тянет slf4j-api без binding -> SLF4J: StaticLoggerBinder
SPARK_PACKAGES='org.apache.hadoop:hadoop-aws:3.5.0,org.slf4j:slf4j-simple:1.7.36'

export S3_ENDPOINT_URL="${S3_ENDPOINT_URL:-http://s3mock:9090}"

docker compose exec -T spark /opt/spark/bin/spark-submit \
  --master local[2] \
  --conf spark.driver.memory=2g \
  --packages "$SPARK_PACKAGES" \
  main.py "$@"
