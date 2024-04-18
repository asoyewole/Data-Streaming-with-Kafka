import json
import time
import datetime
import logging
import os
from random import randint
from kafka.producer import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Configure Logger
if not os.path.exists('./log_folder'):
    os.mkdir('./log_folder')
logging.basicConfig(filename = './log_folder/producer_logs.log',
                    format = '%(asctime)s::%(levelname)s:%(message)s',
                    datefmt = '%m/%d/%Y %I:%M:%S %p',
                    encoding = 'utf-8',
                    filemode = 'w',
                    level = logging.INFO
                    )
                    
logging.info('LOGGER FOR INFO AND WARNINGS IN ASSIGNMENT 6 - PRODUCER')


# Create a new kafka topic for this project
admin_client = KafkaAdminClient(bootstrap_servers = 'localhost:9092',
                                client_id = 'test'
                                )
topic_list = []
topic_list.append(NewTopic(name = "weather_app", 
                           num_partitions = 1, 
                           replication_factor = 1
                           )
                  )
# Create a topic if it does not exist, otherwise continue.
try:
    admin_client.create_topics(topic_list)
    logging.info('Created topic - weather_app')
except TopicAlreadyExistsError:
    logging.info('weather_app already created')
    pass





def weather_generator():
    '''Weather generator to be used in weather generator app. 
    The ranges are hard coded into the function for simplicity sake.
    Ideally they would be made into a function variable.
    '''
    weather = {}
    weather['winnipeg'] = {'city': 'Winnipeg',
                            'temp': randint(-30,30),
                            'wind_speed': randint(0,50),
                            'humidity': randint(20,80)
                            }
    logging.info(f'winnipeg weather data at {datetime.datetime.now()} captured')

    weather['vancouver'] = {'city': 'Vancouver', 
                            'temp': randint(-10,25),
                            'wind_speed': randint(0,30),
                            'humidity': randint(30,99)
                            }
    logging.info(f'winnipeg weather data at {datetime.datetime.now()} captured')
    logging.info('converted weather data to json file encoded as utf-8')
    return json.dumps(weather, indent = 4).encode('utf-8')


producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])

count = 0
while True:
    try:
        data = weather_generator()
        producer.send('weather_app', value = data)
        logging.info(f'{data} transmitted successfully')
        time.sleep(5)
        count+=1
    except KeyboardInterrupt:
        logging.info(f'user terminated after tranmitting {count} records to topic')
        break
