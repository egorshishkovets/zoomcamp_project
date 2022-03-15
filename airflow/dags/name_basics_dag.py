import os
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from scripts.prepare import format_to_parquet
from scripts.upload_to_gcs import upload_to_gcs

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'imdb_data')

file = 'https://datasets.imdbws.com/name.basics.tsv.gz'
archive_name = file.split('/')[-1]
file_name = archive_name.replace('.gz','')
parquet_file = file_name.replace('.tsv', '.parquet')

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022,3,14),
    "retries": 1,
}

with DAG(
    dag_id="imdb_name_basics",
    schedule_interval="@once",
    default_args=default_args,
    catchup=True,
    max_active_runs=2,
    tags=['dtc-de-imdb'],
    ) as dag:

    download_file_task = BashOperator(
        task_id="download_file_task",
        bash_command=f"curl -sS {file} > {path_to_local_home}/{archive_name}"
    )

    unpacking_archive_data = BashOperator(
       task_id="unpacking_archive_data",
       bash_command=f"gunzip {path_to_local_home}/{archive_name}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{file_name}",
        }
    )
    
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    remove_uploaded_files = BashOperator(
        task_id="remove_uploaded_file",
        bash_command=f"rm {path_to_local_home}/{file_name} {path_to_local_home}/{parquet_file}",
    )

    download_file_task >> unpacking_archive_data >> format_to_parquet_task  >> local_to_gcs_task >> remove_uploaded_files
