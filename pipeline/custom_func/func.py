from custom_func.cred import URI, PRIV_IP
from datetime import datetime, timedelta
import json 
from kafka import KafkaConsumer, KafkaProducer
from pymongo.mongo_client import MongoClient
import requests


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


def create_kafka_conn(kafka_topic=None):
    '''
    Create connection with Kafka Cluster, if kafka_topic exists then create connection as Kafka Consumer.
    Another way create connection with Kafka Producer.

    :param kafka_topic: str name of Topic
    :return kafka: client of Kafka Consumer/Producer
    '''

    try:
        if kafka_topic:
            kafka = KafkaConsumer(kafka_topic)
        else:          
            kafka = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                    value_serializer=lambda v: json.dumps(v).encode('utf-8'))
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


if __name__=='__main__':
    data = get_data_from_server()
    print(data)