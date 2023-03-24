from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
from json import dumps
from collections import defaultdict
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import mysql.connector as connection
import requests
import time
from time import sleep
from datetime import datetime
from datetime import datetime, timedelta
import numpy as np
from io import StringIO 
import boto3


def start_user_consumer():
    consumer = KafkaConsumer(
        'capsUSER',
         bootstrap_servers=['localhost:9092'],
         auto_offset_reset='earliest',
         enable_auto_commit=True,
         group_id='my-group',
         consumer_timeout_ms=20000,
         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    df_users = None
    for message in consumer:
        users = message.value
        dict_users = defaultdict(list)
        for i in range(500):
            nombre = users['data'][i]['firstname']
            apellido = users['data'][i]['lastname']
            email = users['data'][i]['email']
            dict_users['first_name'].append(nombre)
            dict_users['last_name'].append(apellido)
            dict_users['email'].append(email)
        df_users = pd.DataFrame(dict_users)
        print(df_users)
        config = {"user" : "jposof" , "password" : "569063" , "host" : "localhost" , "database" : "DB_test"}
        engine = create_engine("mysql+pymysql://" + config["user"] + ":" + config["password"] + "@" + config["host"] + "/" + config["database"])
        df_users.to_sql('users', con = engine, if_exists = 'append', index = False, chunksize = 1000)
    consumer.close()


def start_profile_consumer():
    consumer = KafkaConsumer(
        'capsINFO',
         bootstrap_servers=['localhost:9092'],
         auto_offset_reset='earliest',
         enable_auto_commit=True,
         group_id='my-group',
         consumer_timeout_ms=20000,
         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    df_profile = None
    for message in consumer:
        profile = message.value
        dict_profile = defaultdict(list)
        for i in range(500):
            pokemon = profile['data'][i]['customfield1']
            cardnum = profile['data'][i]['customfield2']
            card = profile['data'][i]['customfield3']
            cardexp = profile['data'][i]['customfield4']
            country = profile['data'][i]['customfield5']
            time = profile['data'][i]['customfield6']['date']
            dict_profile['pokemon'].append(pokemon)
            dict_profile['card_num'].append(cardnum)
            dict_profile['card'].append(card)
            dict_profile['card_exp'].append(cardexp)
            dict_profile['country'].append(country)
            dict_profile['date_time'].append(time)
        df_profile = pd.DataFrame(dict_profile)
        print(df_profile)
        config = {"user" : "jposof" , "password" : "569063" , "host" : "localhost" , "database" : "DB_test"}
        engine = create_engine("mysql+pymysql://" + config["user"] + ":" + config["password"] + "@" + config["host"] + "/" + config["database"])
        df_profile.to_sql('profiles', con = engine, if_exists = 'append', index = False, chunksize = 1000)
    consumer.close()


def start_prod_consumer():
    consumer = KafkaConsumer(
        'capsPROD',
         bootstrap_servers=['localhost:9092'],
         auto_offset_reset='earliest',
         enable_auto_commit=True,
         group_id='my-group',
         consumer_timeout_ms=20000,
         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    df_prices = None
    for message in consumer:
        product = message.value
        dict_products = defaultdict(list)
        for i in range(500):
            wotax = product['data'][i]['net_price']
            precio = product['data'][i]['price']
            dict_products['net_price'].append(wotax)
            dict_products['gross_price'].append(precio)
        df_prices = pd.DataFrame(dict_products)
        print(df_prices)
        config = {"user" : "jposof" , "password" : "569063" , "host" : "localhost" , "database" : "DB_test"}
        engine = create_engine("mysql+pymysql://" + config["user"] + ":" + config["password"] + "@" + config["host"] + "/" + config["database"])
        df_prices.to_sql('prices', con = engine, if_exists = 'append', index = False, chunksize = 1000)
    consumer.close()


def start_producer(topic, url):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    for e in range(10):
        response = requests.get(url)
        data = json.loads(response.content)
        producer.send(topic, value=data)
        sleep(3)
    producer.close()


def csv_load():
    #Connection to the local database
    mydb = connection.connect(host="localhost", database = 'DB_test',user="jposof", passwd="569063",use_pure=True)
    #Connection to the S3 bucket
    s3 = boto3.client("s3",\
                      region_name='us-east-1',\
                      aws_access_key_id='AKIATVKL55OGD4BSBJO3',\
                      aws_secret_access_key='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')
    #Variables needed
    bucket = 'jposof-capstone'
    now = datetime.now()
    date_time = now.strftime("%Y-%m-%d-%H-%M-%S")
    num = 5000
    #Import of every table into a dataframe
    user = f"Select * from users limit {num};"
    user_df = pd.read_sql(user,mydb)
    products = f"Select * from prices limit {num};"
    prod_df = pd.read_sql(products,mydb)
    profile = f"Select * from profiles limit {num};"
    profile_df = pd.read_sql(profile,mydb)
    #Joining and cleaning of dataframe
    df = pd.concat([user_df, prod_df, profile_df], ignore_index = True, axis = 1)
    cols = df.columns
    df.rename(columns = {cols[0]: 'first_name', cols[1]: 'last_name', cols[2]: 'email', cols[3]: 'net_price', cols[4]: 'gross_price', cols[5]: 'pokemon', cols[6]: 'card_num', cols[7]: 'card', cols[8]: 'card_exp', cols[9]: 'country', cols[10]: 'date_time'}, inplace = True)
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    df = df[['full_name', 'country', 'email', 'pokemon', 'net_price', 'gross_price', 'card_num', 'card', 'card_exp', 'date_time']]
    #Loading of csv into S3
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key='csv_dump/csv-' + date_time + '.csv')
    sql = "DROP TABLE users, prices, profiles;"
    mycursor = mydb.cursor()
    mycursor.execute(sql)

