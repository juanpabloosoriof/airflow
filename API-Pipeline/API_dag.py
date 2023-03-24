#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
from time import sleep
import json
from collections import defaultdict
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.http_sensor import HttpSensor

default_args = {
    'owner': 'JP',
    'retries': 5,
    'retry_delay': timedelta(seconds=20),
}

def api_load():
    BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
    API_KEY = '270639d6ad59a005ade7a0ebcbf1220a'
    georgia = ['Atlanta', 'Alpharetta', 'Roswell', 'Valdosta', 'Savannah']
    cali = ['Los Angeles', 'San Diego', 'San Jose', 'San Francisco', 'Fresno']
    dict_cities = defaultdict(list)
    for i in range(1):
        for city in cali:
            request_url = f"{BASE_URL}?appid={API_KEY}&q={city}"
            response = requests.get(request_url)
            data = json.loads(response.content)
            temp = round(data['main']['temp'])
            dict_cities['city'].append(city)
            dict_cities['temp K'].append(temp)
    df_cali = pd.DataFrame(dict_cities)
    df_cali['state'] = 'California'
    dict_cities = defaultdict(list)
    for i in range(1):
        for city in georgia:
            request_url = f"{BASE_URL}?appid={API_KEY}&q={city}"
            response = requests.get(request_url)
            data = json.loads(response.content)
            temp = round(data['main']['temp'])
            dict_cities['city'].append(city)
            dict_cities['temp K'].append(temp)
    df_ga = pd.DataFrame(dict_cities)
    df_ga['state'] = 'Georgia'
    df = pd.concat([df_ga, df_cali], ignore_index = True)
    now = datetime.now()
    date_time = now.strftime("%Y-%m-%d-%H-%M-%S")
    path = '/home/hadoop/python/Python/Airflow/file-' + date_time + '.csv'
    df.to_csv(path, header=True, index=False)

with DAG(
    dag_id = 'API_Pipeline',
    default_args = default_args,
    description = 'Proyecto post training EhIT',
    start_date = datetime(2023, 3, 23),
    schedule = timedelta(minutes=3),
    catchup = False
    ) as dag:
    test = HttpSensor(
        task_id='API_test',
        method='GET',
        http_conn_id='http_default',
        endpoint='/api.openweathermap.org/data/2.5/weather?appid=270639d6ad59a005ade7a0ebcbf1220a&q=Savannah',
        response_check=lambda response: True if response.json()['cod'] == 200 else False,
        poke_interval=5,
        timeout=20
    )
    load = PythonOperator(
        task_id = 'API_load',
        python_callable=api_load,
    )

    test >> load

