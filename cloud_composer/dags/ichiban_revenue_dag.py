"""A dag for triggering the cloud functions jobs from "turnover form" > "raw" > "processed"."""
import airflow

from airflow import models
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from cloud_composer.dag_utils import invoke_cloud_function

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with models.DAG(
    'ichiban_revenue',
    default_args=default_args,
    description='Dag for processing turnover form > raw > processed',
    schedule_interval=None,
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
) as dag:

    t1 = PythonOperator(
        task_id="appdata_to_raw", 
        python_callable=invoke_cloud_function,
        op_kwargs={
            "cloud_function_url":'.../j1-and-j2-raw-appdata-to-bigquery-to-processed-plot-data'
        }
    )

    t1