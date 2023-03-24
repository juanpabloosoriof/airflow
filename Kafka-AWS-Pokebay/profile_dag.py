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
    dag_id = 'Profile',
    default_args = default_args,
    description = 'Prueba del profile con consumer/producer',
    start_date = datetime(2023, 3, 10),
    schedule = timedelta(minutes=4),
    catchup = False
    ) as dag:
    start_profile_producer = PythonOperator(
    task_id='start_profile_producer',
    python_callable=Capstone_Kafka.start_producer,
    op_kwargs={'topic': 'capsINFO', 'url': 'https://fakerapi.it/api/v1/custom?_quantity=500&customfield1=pokemon&customfield2=card_number&customfield3=card_type&customfield4=card_expiration&customfield5=country&customfield6=dateTime'}
    )
    start_profile_consumer = PythonOperator(
        task_id='start_profile_consumer',
        python_callable=Capstone_Kafka.start_profile_consumer
    )
    
    start_profile_producer >> start_profile_consumer

