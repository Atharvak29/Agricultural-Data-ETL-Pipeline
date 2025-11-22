from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ensure scripts are importable
# (Adjust path based on your Airflow environment structure)
sys.path.append('/opt/airflow/dags/repo') 

from scripts.etl_extractor import AgriExtractor
from scripts.etl_transformer import process_data
from scripts.init_lookups import generate_static_data

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_extraction_task():
    # Ensure lookups exist before extraction
    if not os.path.exists(os.path.join(os.getcwd(), 'data/static/soil_lookup.csv')):
        generate_static_data()
        
    extractor = AgriExtractor()
    path = extractor.run_extraction()
    if not path:
        raise ValueError("Extraction failed or no data found")
    return path

with DAG(
    'maharashtra_agri_pipeline',
    default_args=default_args,
    description='Weekly ETL for Agri Data (Agmarknet + Weather + Soil)',
    schedule_interval='@weekly', # Runs once a week
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['agriculture', 'pyspark', 'ml-ops'],
) as dag:

    # Task 1: Refresh Lookups (Optional, ensures static files exist)
    t1_check_lookups = PythonOperator(
        task_id='check_generate_lookups',
        python_callable=generate_static_data
    )

    # Task 2: Extract (Scrape + API)
    t2_extract = PythonOperator(
        task_id='scrape_and_fetch_weather',
        python_callable=run_extraction_task
    )

    # Task 3: Transform (PySpark)
    t3_transform = PythonOperator(
        task_id='pyspark_transformation',
        python_callable=process_data
    )

    # Pipeline dependency
    t1_check_lookups >> t2_extract >> t3_transform