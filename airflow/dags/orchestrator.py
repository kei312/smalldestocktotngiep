from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/api-request')

def safe_main_callable():
    from inse import main
    return main()

default_args = {
    'description': 'A DAG to orchestrate data',
    'start_date': datetime(2024, 4, 30),
    'catchup': False,
}

dag = DAG(
    dag_id='teststock',
    default_args=default_args,
    schedule=timedelta(minutes=5)
)

with dag:
    task1 = PythonOperator(
        task_id='ingest_data_task',
        python_callable=safe_main_callable
    )