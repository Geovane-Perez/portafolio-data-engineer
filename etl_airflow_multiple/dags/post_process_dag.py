from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Agrega la ruta al script mover_archivo.py
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
from mover_archivo import mover_archivo_procesado

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='post_process_pipeline',
    default_args=default_args,
    description='DAG que mueve el archivo procesado luego del ETL',
    schedule=None,
    catchup=False,
    tags=['etl', 'postprocesado'],
) as dag:

    mover_archivo = PythonOperator(
        task_id='mover_archivo_procesado',
        python_callable=mover_archivo_procesado,
    )

    mover_archivo