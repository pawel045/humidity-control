# START LISTINING FOR DATA FROM PRODUCERS
# ADD DATA TO MONGODB
# REMOVE OLD DATA

import custom_func.func as f
from datetime import datetime, timedelta
import json

# define how many days keep data
DAYS_TO_EXPIRE = 7

print('CONSUMING DATA...')

# create clients for Kafka and MongoDB
consumer = f.create_kafka_conn('hum_temp')
client = f.create_conn_mongodb()

# specify database and collection
db = client.HumidityControl
hum_temp = db.hum_temp

# send data from Kafka to mongoDB
for msg in consumer:
    data = json.loads(msg.value.decode('utf-8'))
    data['timestamp'] = datetime.fromisoformat(data['timestamp'])
    print('Sending to MongoDB', end=" -> ")
    print(data)
    hum_temp.insert_one(data)

    # deleting old data
    seven_days_ago = datetime.now() - timedelta(days=DAYS_TO_EXPIRE)
    hum_temp.delete_many({'timestamp': {'$lt': seven_days_ago}})
