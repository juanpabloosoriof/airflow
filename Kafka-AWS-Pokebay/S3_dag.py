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

with DAG(
    dag_id = 'CSV_Load',
    default_args = default_args,
    description = 'Prueba uploading csv a S3 con schedule',
    start_date = datetime(2023, 3, 10),
    schedule = timedelta(minutes=6),
    catchup = False
    ) as dag:
    load_new_csv = PythonOperator(
    task_id='load_new_csv',
    python_callable=Capstone_Kafka.csv_load,
    )
    
    load_new_csv

