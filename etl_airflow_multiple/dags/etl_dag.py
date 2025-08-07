from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
import os

# Agregar el path del script para importar etl.py
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
from etl import run_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL DAG para cargar datos a PostgreSQL en AWS',
    schedule=None,  # ejecución manual o por API
    catchup=False,
)

run_etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl,
    dag=dag,
)

trigger_post_process = TriggerDagRunOperator(
    task_id='lanzar_post_process',
    trigger_dag_id='post_process_pipeline',
    wait_for_completion=False,  # o True si quieres que espere
    dag=dag,
)

run_etl_task >> trigger_post_process