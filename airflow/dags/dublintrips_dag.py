from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime
import os
import sys

# Import scripts
from download_raw_data import download_files
from upload_files_to_s3 import main as upload_files_to_s3
from load_s3_to_redshift import main as load_s3_to_redshift

with DAG(
    dag_id='dublintrips_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@monthly',
    catchup=False,
    description='End-to-end orchestration for Dublin Trips project'
) as dag:

    # Task 1: Download Raw Data
    task_download_raw_data = PythonOperator(
        task_id='download_raw_data',
        python_callable=download_files
    )

    # Task 2: Upload Files to S3
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_files_to_s3
    )

    # Task 3: Load Data from S3 to Redshift
    task_load_to_redshift = PythonOperator(
        task_id='load_data_to_redshift',
        python_callable=load_s3_to_redshift
    )

    # Task 4: Run dbt job in dbt Cloud
    task_run_dbt_job = DbtCloudRunJobOperator(
        task_id='run_dbt_cloud_job',
        dbt_cloud_conn_id='dublintrips_conn',
        job_id=70471823452790,
        check_interval=60,
        timeout=300
    )

    # Set the task sequence
    task_download_raw_data >> task_upload_to_s3 >> task_load_to_redshift >> task_run_dbt_job
