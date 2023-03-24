#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import Capstone_Kafka

default_args = {
    'owner': 'JP',
    'retries': 5,
    'retry_delay': timedelta(seconds=20),
}

def go():
    print("Go")

with DAG(
    dag_id = 'Capstone',
    default_args = default_args,
    description = 'Prueba totaaal',
    start_date = datetime(2023, 3, 10),
    schedule = timedelta(minutes=5),
    catchup = False
    ) as dag:
    trigger = PythonOperator(
        task_id = 'Trigger',
        python_callable=go,
    )
    trigger2 = PythonOperator(
        task_id = 'Trigger2',
        python_callable=go,
    )
    start_user_producer = PythonOperator(
        task_id='start_user_producer',
        python_callable=Capstone_Kafka.start_producer,
        op_kwargs={'topic': 'capsUSER', 'url': 'https://fakerapi.it/api/v1/users?_quantity=500'}
    )
    start_user_consumer = PythonOperator(
        task_id='start_user_consumer',
        python_callable=Capstone_Kafka.start_user_consumer
    )
    start_profile_producer = PythonOperator(
        task_id='start_profile_producer',
        python_callable=Capstone_Kafka.start_producer,
        op_kwargs={'topic': 'capsINFO', 'url': 'https://fakerapi.it/api/v1/custom?_quantity=500&customfield1=pokemon&customfield2=card_number&customfield3=card_type&customfield4=card_expiration&customfield5=country&customfield6=dateTime'}
    )
    start_profile_consumer = PythonOperator(
        task_id='start_profile_consumer',
        python_callable=Capstone_Kafka.start_profile_consumer
    )
    start_prod_producer = PythonOperator(
        task_id='start_prod_producer',
        python_callable=Capstone_Kafka.start_producer,
        op_kwargs={'topic': 'capsPROD', 'url': 'https://fakerapi.it/api/v1/products?_quantity=500&_price_min=2999.99&_price_max=49999.99&_taxes=9'}
    )
    start_prod_consumer = PythonOperator(
        task_id='start_prod_consumer',
        python_callable=Capstone_Kafka.start_prod_consumer
    )
    load_new_csv = PythonOperator(
        task_id='load_new_csv',
        python_callable=Capstone_Kafka.csv_load,
    )
    
    trigger >> [start_user_producer, start_profile_producer, start_prod_producer] >> trigger2 >> [start_user_consumer, start_profile_consumer, start_prod_consumer] >> load_new_csv

