from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.GEE import run as run_gee
from scripts.copernicus_tide import run as run_copernicus

with DAG(
    dag_id="main",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   
    catchup=False,
    tags=["clean"],
) as dag:

    step1 = PythonOperator(
        task_id="GEE",
        python_callable=run_gee
    )

    step2 = PythonOperator(
        task_id="Copernicus",
        python_callable=run_copernicus
    )

    step1 >> step2
