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
    dag_id = 'Price',
    default_args = default_args,
    description = 'Prueba del profile con consumer/producer',
    start_date = datetime(2023, 3, 10),
    schedule = timedelta(minutes=4),
    catchup = False
    ) as dag:
    start_prod_producer = PythonOperator(
    task_id='start_prod_producer',
    python_callable=Capstone_Kafka.start_producer,
    op_kwargs={'topic': 'capsPROD', 'url': 'https://fakerapi.it/api/v1/products?_quantity=500&_price_min=2999.99&_price_max=49999.99&_taxes=9'}
    )
    start_prod_consumer = PythonOperator(
        task_id='start_prod_consumer',
        python_callable=Capstone_Kafka.start_prod_consumer
    )
    
    start_prod_producer >> start_prod_consumer

