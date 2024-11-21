import sys
sys.path.append('./pipeline')

from custom_func.cred import *
from datetime import datetime
import json 
from kafka import KafkaConsumer, KafkaProducer
import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from pymongo.mongo_client import MongoClient
import requests
from sqlalchemy import create_engine


# Functions for streaming process
def create_conn_mongodb(uri=URI):
    '''
    Create connection with MongoDB Atlas service
    
    :param uri: 
    :return client:
    '''
    try:
        client = MongoClient(uri)
        return client
    except:
        print('MongoDB server is not available')
        return


def create_kafka_conn(kafka_topic=None, from_beginning=False):
    '''
    Create connection with Kafka Cluster, if kafka_topic exists then create connection as Kafka Consumer.
    Another way create connection with Kafka Producer.

    :param kafka_topic: str name of Topic
    :return kafka: client of Kafka Consumer/Producer
    '''

    try:
        if kafka_topic:
            if from_beginning:
                kafka = KafkaConsumer(kafka_topic,
                                      bootstrap_servers=['localhost:9092'],
                                      value_deserializer=lambda x: x.decode('utf-8'),
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True
                                      )
            
            else:
                kafka = KafkaConsumer(kafka_topic)
        else:
            kafka = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        
        return kafka
    
    except:
        print('Kafka cluster is not available')
        return
    

def get_data_from_server(priv_ip=PRIV_IP):
    '''
    Recive data from webserver, which is created via NodeMCU and data provided by DTH22 device.

    :priv_ip: private IP of IoT device
    :return data: dict -> {'id': 1, 'temperature': 20.00, 'humidity': 50.00, 'timestamp': '2000-01-01 00:00:00.000001'}
    '''

    now = str(datetime.now())

    try:
        r = requests.get(f'http://{priv_ip}/', timeout=3)
        if r.status_code == 200:
            try:
                data = json.loads(str(r.text))
                data['timestamp'] = now
            except:
                data = {
                    'id': -1,
                    'error': 'Data not received',
                    'timestamp': now
                }

    except:
        data = {
            'id': -1,
            'error': 'Cannot connect with webserver',
            'timestamp': now
        }

    finally:
        return data


# Functions for ETL process
def create_engine_postgres():
    '''
    Create engine for PostgreSQL Database using in HumidityControl project.
    :return: engine
    '''

    db_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}"
    engine = create_engine(db_url)
    return engine


def create_conn_postgres():
        conn = psycopg2.connect(
            dbname=PG_DBNAME, 
            user=PG_USER, 
            password=PG_PASSWORD, 
            host=PG_HOST, 
            port=PG_PORT
        )
        cursor = conn.cursor()

        return conn, cursor


def create_insert_query():
    insert_query = sql.SQL("""
        INSERT INTO {table} (id, temperature, humidity, timestamp, temp_change, hum_change)
        VALUES (%s, %s, %s, %s, %s, %s)
    """).format(table=sql.Identifier(PG_TABLENAME))
    
    return insert_query

def load_newest_csv(folder_path):
    """
    Get the newest CSV file from a folder and load it into a Pandas DataFrame.
    :param folder_path: str Path to the folder containing CSV files.
    :return newest_file: str name of file
    :return pd.DataFrame: DataFrame created from the newest CSV file.
    """
    # Get a list of CSV files in the folder
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    
    if not csv_files:
        raise FileNotFoundError("No CSV files found in the specified folder.")
    
    # Get the newest CSV file based on modification time
    newest_file = max(
        [os.path.join(folder_path, f) for f in csv_files], 
        key=os.path.getmtime
    )
    
    print(f"Loading newest CSV file: {newest_file}")
    
    # Load the newest CSV file into a DataFrame
    return newest_file, pd.read_csv(newest_file)


if __name__=='__main__':
    data = get_data_from_server()
    print(data)
