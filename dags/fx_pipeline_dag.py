from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from scripts.ddl import load_dim_run
from scripts.extract import extract_run
from scripts.transform import transfrom

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

dag = DAG(
    dag_id="fx_pipeline",
    default_args=default_args,
    description="Daily fx_rates ETL pipeline",
    schedule="15 16 * * *",
    catchup=False,
)


def run_extract():
    extract_run()


def run_transform():
    transfrom()


def load_dim():
    load_dim_run()


extract_task = PythonOperator(task_id="extract", python_callable=run_extract, dag=dag)


ddl_tables = PythonOperator(task_id="ddl_tables", python_callable=load_dim, dag=dag)

transfrom_load_task = PythonOperator(
    task_id="transform_and_load", python_callable=run_transform, dag=dag
)


ddl_tables >> extract_task >> transfrom_load_task
