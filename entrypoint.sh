#!/bin/bash

echo "--- Starting Weekly Agricultural ETL Pipeline ---"

# 1. Run the Extractor
# This extracts data, gets weather, and saves raw parquet to /app/data/raw
echo "Executing Extraction and Weather Fetch..."
python /app/scripts/etl_extractor.py
if [ $? -ne 0 ]; then
    echo "Extraction failed. Exiting."
    exit 1
fi

# 2. Run the PySpark Transformation
# This reads the raw parquet, transforms it, and saves ML-ready files to /app/data/processed
echo "Executing PySpark Transformation and Feature Engineering..."
python /app/scripts/etl_transformer.py
if [ $? -ne 0 ]; then
    echo "Transformation failed. Exiting."
    exit 1
fi

echo "--- Pipeline Complete ---"

# --- Next Steps for Cloud Deployment ---
# In a real Fargate/Lambda setup, you would add logic here to upload the results 
# to S3, for example using the AWS CLI (which is often pre-installed):
# aws s3 sync /app/data/processed/ s3://your-agri-data-bucket/processed/