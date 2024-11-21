# SEND DATA FROM IOT DEVICES EVERY 5 SECONDS

import sys
sys.path.append('./pipeline/custom_func/')

import custom_func.func as f
from time import sleep

print('START PRODUCING DATA...')        
while True:
    producer = f.create_kafka_conn()

    data = f.get_data_from_server()
    print(data)
    producer.send('hum_temp', data)

    # ensure messages are sent before finishing
    producer.flush()
    sleep(5)
