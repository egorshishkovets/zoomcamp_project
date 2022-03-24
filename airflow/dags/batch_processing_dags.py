import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, \
    DataprocDeleteClusterOperator, ClusterGenerator, DataprocSubmitJobOperator
from scripts.upload_to_gcs import upload_to_gcs

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PYSPARK_JOB_URI = path_to_local_home + "/dags/spark_jobs/movies_ratings.py"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022,3,14),
    "retries": 1,
}

with DAG(
        dag_id="batch_processing_data",
        schedule_interval="@once",
        default_args=default_args,
        catchup=True,
        max_active_runs=2,
        tags=['dtc-de-imdb'],
) as dag:
    CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone="europe-west6-a",
    master_machine_type="n1-standard-2",
    worker_machine_type="n1-standard-2",
    master_disk_size=500,
    worker_disk_size=500,
    num_workers=2,
    image_version="2.0-debian10",
    metadata={'bigquery-connector-url': 'gs://path/to/custom/hadoop/bigquery/connector.jar',
              'spark-bigquery-connector-url': 'gs://path/to/custom/spark/bigquery/connector.jar'}
    ).make()

    create_cluster_operator = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        cluster_name="spark-dag",
        project_id=PROJECT_ID,
        region="europe-west6",
        cluster_config=CLUSTER_GENERATOR_CONFIG,
        )

    local_to_gcs_task = PythonOperator(
        task_id=f"spark_job_local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"movies_ratings.py",
            "local_file": f"{PYSPARK_JOB_URI}",
        }
    )

    PYSPARK_JOB = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": "spark-dag"},
        "pyspark_job": {"main_python_file_uri": "gs://movielens-dataset-de/movies_ratings.py",
                        "jar_file_uris":["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
        }
    }
    
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", job=PYSPARK_JOB, region="europe-west6", project_id=PROJECT_ID
    )
    
    delete_cluster = DataprocDeleteClusterOperator(
            task_id="delete_cluster", 
            project_id=PROJECT_ID, 
            cluster_name="spark-dag", 
            region="europe-west6"
        )

    create_cluster_operator >> local_to_gcs_task >> pyspark_task >> delete_cluster