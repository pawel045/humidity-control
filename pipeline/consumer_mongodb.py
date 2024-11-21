# START LISTINING FOR DATA FROM PRODUCERS
# ADD DATA TO MONGODB
# REMOVE OLD DATA

import sys
sys.path.append('./pipeline/custom_func/')

import custom_func.func as f
from datetime import datetime, timedelta
import json

# define how many days keep data
DAYS_TO_EXPIRE = 1

print('CONSUMING DATA...')

# create clients for Kafka and MongoDB
consumer = f.create_kafka_conn('hum_temp')
client = f.create_conn_mongodb()

# specify database and collection
db = client.HumidityControl
hum_temp = db.hum_temp

try:
    # send data from Kafka to mongoDB
    for msg in consumer:
        data = json.loads(msg.value.decode('utf-8'))
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        print('Sending to MongoDB', end=" -> ")
        print(data)
        hum_temp.insert_one(data)

        # deleting old data
        to_del = datetime.now() - timedelta(days=DAYS_TO_EXPIRE)
        hum_temp.delete_many({'timestamp': {'$lt': to_del}})

except KeyboardInterrupt:
    pass

finally:
    consumer.close()