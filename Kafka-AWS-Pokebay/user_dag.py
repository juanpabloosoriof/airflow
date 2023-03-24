from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import Capstone_Kafka

default_args = {
    'owner': 'JP',
    'retries': 5,
    'retry_delay': timedelta(seconds=20),
}

with DAG(
    dag_id = 'User',
    default_args = default_args,
    description = 'Prueba del user con consumer/producer',
    start_date = datetime(2023, 3, 10),
    schedule = timedelta(minutes=4),
    catchup = False
    ) as dag:
    start_user_producer = PythonOperator(
    task_id='start_user_producer',
    python_callable=Capstone_Kafka.start_producer,
    op_kwargs={'topic': 'capsUSER', 'url': 'https://fakerapi.it/api/v1/users?_quantity=500'}
    )
    start_user_consumer = PythonOperator(
        task_id='start_user_consumer',
        python_callable=Capstone_Kafka.start_user_consumer
    )
    
    start_user_producer >> start_user_consumer

