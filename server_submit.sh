spark-submit \
  --master yarn \
  main.py \
  --driver-memory 3g \
  --audition_path s3a://s3-ds-source/audition.parquet \
  --content_path s3a://s3-ds-source/content.parquet \
  --output_path "s3a://${S3_BUCKET}/results/S24_project_data"

