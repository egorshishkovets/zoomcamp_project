import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

from scripts.prepare import format_to_parquet
from scripts.upload_to_gcs import upload_to_gcs

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

movie_zip_link = "https://files.grouplens.org/datasets/movielens/ml-latest.zip"
zip_name = movie_zip_link.split('/')[-1]
zip_folder_name = zip_name.split('.')[0]
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
path_to_files = path_to_local_home + "/data/"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'movielens_dataset')

partition_files = ["tags.csv", "ratings.csv"]
movieid_cluster = ["links.csv", "movies.csv","genome-scores.csv"]
files = ["tags.csv", "ratings.csv", \
         "genome-scores.csv", "genome-tags.csv",\
         "links.csv", "movies.csv"]

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022,3,14),
    "retries": 1,
}

with DAG(
        dag_id="movielens_dag",
        schedule_interval="@once",
        default_args=default_args,
        catchup=True,
        max_active_runs=2,
        tags=['dtc-de-imdb'],
) as dag:

    if not os.path.exists(path_to_local_home + "/data/"):
        os.mkdir(path_to_local_home + "/data/")

    download_file_task = BashOperator(
        task_id="download_file_task",
        bash_command=f"curl -sS {movie_zip_link} > {path_to_local_home}/{zip_name}"
    )

    unpacking_archive_data = BashOperator(
    task_id="unpacking_archive_data",
    bash_command=f"unzip {path_to_local_home}/{zip_name} -d {path_to_files}"
    )

    remove_uploaded_zip = BashOperator(
        task_id="remove_uploaded_zip",
        bash_command=f"rm {path_to_local_home}/{zip_name}",
    )

    remove_data_files = BashOperator(
        task_id="remove_data_files",
        bash_command=f"rm -rf {path_to_files}",
    )
    
    for file in files:
        if file.endswith(".csv"):
            file_name = file.split('.')[0]
            format_to_parquet_task = PythonOperator(
                task_id=f"{file_name}_to_parquet_task",
                python_callable=format_to_parquet,
                op_kwargs={
                    "src_file": f"{path_to_files}/{zip_folder_name}/{file}",
                },
                dag=dag
            )

            parquet_file = file.replace('.csv','.parquet')

            local_to_gcs_task = PythonOperator(
                task_id=f"{file_name}_local_to_gcs_task",
                python_callable=upload_to_gcs,
                op_kwargs={
                    "bucket": BUCKET,
                    "object_name": f"raw/{parquet_file}",
                    "local_file": f"{path_to_files}/{zip_folder_name}/{parquet_file}",
                },
                dag=dag
            )


            imdb_external_table_task = BigQueryCreateExternalTableOperator(
                task_id=f"{file_name}_external_table_task",
                table_resource={
                    "tableReference": {
                        "projectId": PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET,
                        "tableId": f"{file_name.replace('-','_')}_external_table",
                    },
                    "externalDataConfiguration": {
                        "autodetect": "True",
                        "sourceFormat": f"{'parquet'.upper()}",
                        "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
                    },
                },
            )

            date_part_col = ""
            if file in movieid_cluster:
                part_conditions = "cluster by movieId"
            elif file in partition_files:
                part_conditions = "partition by date_part \
                                cluster by userID, movieId"
                date_part_col = ", DATE_TRUNC(date(TIMESTAMP_MILLIS(timestamp*1000)), Month) as date_part"
            else:
                part_conditions = "cluster by tagId"

            CREATE_BQ_TBL_QUERY = (
                f"CREATE OR REPLACE TABLE {PROJECT_ID}.{BIGQUERY_DATASET}.{file_name.replace('-','_')} \
                {part_conditions}  \
                AS \
                SELECT * {date_part_col} FROM {PROJECT_ID}.{BIGQUERY_DATASET}.{file_name.replace('-','_')}_external_table;"
            )

            movielens_create_partitioned_table_job = BigQueryInsertJobOperator(
                task_id=f"movielens_create_{file_name}_partitioned_table_task",
                configuration={
                    "query": {
                        "query": CREATE_BQ_TBL_QUERY,
                        "useLegacySql": False,
                    }
                }
            )

            DROP_BQ_EXT_TBL_QUERY = (
                f"DROP TABLE {PROJECT_ID}.{BIGQUERY_DATASET}.{file_name.replace('-','_')}_external_table;"
            )
            movielens_drop_external_table_job = BigQueryInsertJobOperator(
                task_id=f"movielens_drop_{file_name}_exteranl_table_task",
                configuration={
                    "query": {
                        "query": DROP_BQ_EXT_TBL_QUERY,
                        "useLegacySql": False,
                    }
                }
            )

  
            download_file_task >> \
                unpacking_archive_data >> format_to_parquet_task  >> \
                    local_to_gcs_task >> imdb_external_table_task >> \
                        movielens_create_partitioned_table_job >> movielens_drop_external_table_job >> \
                            remove_uploaded_zip >> remove_data_files
