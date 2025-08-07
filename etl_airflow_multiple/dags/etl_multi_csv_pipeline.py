# etl_multi_csv_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 23),
    'retries': 1
}

def run_etl():
    print("🚀 Ejecutando etl_multi.py desde DAG")
    try:
        result = subprocess.run(
            ['python3', '/opt/airflow/scripts/etl_multi.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=os.environ.copy()
        )
        print("✅ STDOUT:\n", result.stdout)
        print("⚠️ STDERR:\n", result.stderr)
        result.check_returncode()
    except subprocess.CalledProcessError as e:
        print(f"❌ Error en la ejecución del script: {e}")
        raise

with DAG(
    dag_id='etl_multi_csv_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['etl_multi', 'multi_csv']
) as dag:

    ejecutar_etl = PythonOperator(
        task_id='procesar_csv_multiples',
        python_callable=run_etl
    )

    ejecutar_etl

# 🔥 Importante: aseguramos que el DAG quede referenciado para Airflow
dag = dag