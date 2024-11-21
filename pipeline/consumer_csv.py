# GET DATA FROM LAST 7 DAYS AND SAVE AS CSV

import sys
from datetime import datetime, timedelta
import json
sys.path.append('./pipeline/custom_func/')

import ast
import custom_func.func as f
import pandas as pd


print('CONSUMING DATA FROM LAST 7 DAYS...')

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

# save as csv
df = pd.DataFrame(msg_dict)
df.to_csv(f'./pipeline/data/dht22_{today}.csv')
print('DATA SAVE AS CSV FILE. GOODBYE :)')
