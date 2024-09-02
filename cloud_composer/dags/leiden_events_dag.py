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
    'leiden_events',
    default_args=default_args,
    description='Dag for processing Leiden events for raw > processed',
    schedule_interval="0 1 1 1 *",
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
) as dag:

    t1 = PythonOperator(
        task_id="leiden_events_sheet_to_raw", 
        python_callable=invoke_cloud_function,
        op_kwargs={
            "cloud_function_url":'.../j1-leiden-event-google-sheet-to-bigquery'
        }
    )

    t2 = PythonOperator(
        task_id="public_holidays_to_raw", 
        python_callable=invoke_cloud_function,
        op_kwargs={
            "cloud_function_url":'.../j1-raw-public-holidays-data-to-bigquery'
        }
    )

    t3 = PythonOperator(
        task_id="school_holidays_to_raw", 
        python_callable=invoke_cloud_function,
        op_kwargs={
            "cloud_function_url":'.../j1-raw-school-holidays-data-to-bigquery'
        }
    )

    t4 = PythonOperator(
        task_id="raw_to_processed", 
        python_callable=invoke_cloud_function,
        op_kwargs={
            "cloud_function_url":'.../j2-bigquery-to-processed-event-data'
        }
    )

    (t1, t2, t3) >> t4