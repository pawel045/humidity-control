import os, sys
# sys.path.append('./custom_dags_func/')
# import custom_dags_func.dags_func as dag_f

import ast
from airflow import DAG
from airflow.operators.python import PythonOperator
import custom_func.func as f
from datetime import datetime, timedelta
import glob
import pandas as pd


def extract():
    print('CONSUMING DATA FROM LAST 7 DAYS...')
    try:
        # create clients for Kafka and specify datetime
        msg_list = []
        consumer = f.create_kafka_conn('hum_temp', from_beginning=True)

        now = datetime.now()
        seven_days_ago = now - timedelta(days=7)

        # receive data from kafka cluster
        for message in consumer:
            msg_timestamp = message.timestamp / 1000
            msg_datetime = datetime.fromtimestamp(msg_timestamp)
            
            if msg_datetime >= seven_days_ago:
                msg_list.append(message.value)

            if msg_datetime > now:
                print(f'Recived all messages from up to now - {now}.')
                print('STOP CONSUMING...')
                break

        consumer.close()

        print('SAVING DATA...')
        # save data as csv
        today = datetime.today().strftime('%d_%m_%y')

        # tranform list of data to dictionary
        msg_dict = {
            'id': [],
            'temperature': [],
            'humidity': [],
            'timestamp': []
        }
        for row in msg_list:
            row_dict = ast.literal_eval(row)

            # drop invalid rows
            if row_dict['id'] == -1:
                continue

            # add row to dict
            msg_dict['id'].append(row_dict['id'])
            msg_dict['temperature'].append(row_dict['temperature'])
            msg_dict['humidity'].append(row_dict['humidity'])
            msg_dict['timestamp'].append(row_dict['timestamp'])

        df = pd.DataFrame(msg_dict)
        df.to_csv(f'./pipeline/data/dht22_{today}.csv')
        print('DATA SAVE AS CSV FILE. GOODBYE :)')
    except:
        print('CONSUMING DATA FAILED')


def transform():
    print('TRANSFORM DATA\n')

    # try:
        # get data from csv
    newest_file, df = f.load_newest_csv('./pipeline/data')

    # temp/hum change
    df['temp_change'] = df['temperature'].diff().fillna(0).round(1)
    df['hum_change'] = df['humidity'].diff().fillna(0).round(1)

    # save changes
    df.to_csv(newest_file[:-4] + '_transformed.csv')
    print('TRANSFORMATION SUCCEDED\n')
    # except:
    #     print('TRANSFORMATION FAILED\n')


def load():
    print('LOADING DATA...\n')

    # Connection
    conn, cursor = f.create_conn_postgres()
    query = f.create_insert_query()

    # Data
    _, df = f.load_newest_csv('./pipeline/data')
    df = df[['id','temperature','humidity','timestamp','temp_change','hum_change']]

    try:
        for _, row in df.iterrows():
            cursor.execute(query, tuple(row))
        print('\nLOADING DATA SUCCEDED\n')
    except:
        print('LOADING DATA FAILED\n')
    finally:
        # Commit the transaction and close connection
        conn.commit()
        cursor.close()
        conn.close()


def del_data():
    print('START DELETING CSV FILES...\n')
    try:
        path_to_del = os.path.abspath(os.getcwd() + '/pipeline/data/*.csv')
        files = glob.glob(path_to_del)
        
        for f in files:
            os.remove(f)

            file_name = f.split('/')[-1]
            print(f'{file_name} -> DELETED')
    finally:
        print('\nDELETING COMPLETED.')


with DAG('etl_dth', 
         start_date=datetime(2024, 11, 11), 
         schedule_interval='@weekly', 
         catchup=False) as dag:
    
    extract = PythonOperator(task_id='extract',
                             python_callable=extract)

    transform = PythonOperator(task_id='transform',
                               python_callable=transform)

    load = PythonOperator(task_id='load',
                          python_callable=load)

    del_data = PythonOperator(task_id='del_data',
                              python_callable=del_data)

    extract >> transform >> load >> del_data


if __name__=='__main__':
    del_data()