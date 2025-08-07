from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 0,
}

with DAG(
    dag_id='etl_multi_config_dag',
    default_args=default_args,
    schedule_interval=None,  # Solo bajo demanda
    catchup=False,
    tags=['etl', 'csv', 'config'],
    description='Ejecuta el pipeline ETL basado en configuración JSON para múltiples archivos',
) as dag:

    ejecutar_etl = BashOperator(
        task_id='ejecutar_etl_configurable',
        bash_command='python /opt/airflow/scripts/etl_multi_config.py'
    )