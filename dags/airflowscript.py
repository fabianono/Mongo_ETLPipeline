from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_argument = {
    'owner': 'Russell Westbrook',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id = 'airflowscript',
    default_args=default_argument,
    start_date=datetime(2024,11,4),
    schedule_interval='*/5 * * * *',
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    task1 = BashOperator(
        task_id='into_kafka',
        bash_command="python /opt/airflow/main/kafkascript.py"
    )