import json
import psycopg2
import logging
import os
from datetime import datetime
from psycopg2.errors import DuplicateDatabase
from kafka import KafkaConsumer


# Configure Logger
if not os.path.exists('./log_folder'):
    os.mkdir('./log_folder')
logging.basicConfig(filename = './log_folder/consumer_logs.log',
                    format = '%(asctime)s::%(levelname)s:%(message)s',
                    datefmt = '%m/%d/%Y %I:%M:%S %p',
                    encoding = 'utf-8',
                    filemode = 'w',
                    level = logging.INFO
                    )
                    
logging.info('LOGGER FOR INFO AND WARNINGS IN ASSIGNMENT 6 - CONSUMER')



con = psycopg2.connect(host = '127.0.0.1',
                       port = '5432',
                       user = 'postgres',
                       password = 'postgres')
logging.info('successfully connected to postgre server')
con.autocommit = True
cursor = con.cursor()

# Create database if it does not already exist
try:
    cursor.execute('CREATE DATABASE assg6;')
    logging.info('Created Database successfully')
except DuplicateDatabase:
    logging.info('Database created in the previous run')
    pass
con.close()


# Connect to the created database
con = psycopg2.connect(database = 'assg6',
                 user = 'postgres',
                 password = 'postgres',
                 host = '127.0.0.1',
                 port = '5432')
                 
logging.info('Successfully connected to assg6 database')
con.autocommit = True
cursor = con.cursor()

winnipeg_payload = '''CREATE TABLE IF NOT EXISTS winnipeg_weather_app (Received_at timestamp,
                                                    Temperature_deg_C int,
                                                    Wind_Speed_kmph int,
                                                    Humidity_pcnt int);'''

vancouver_payload = '''CREATE TABLE IF NOT EXISTS vancouver_weather_app (Received_at timestamp,
                                                    Temperature_deg_C int,
                                                    Wind_Speed_kmph int,
                                                    Humidity_pcnt int);'''

cursor.execute(winnipeg_payload)
logging.info('winnipeg_weather_app table created successfully')
cursor.execute(vancouver_payload)
logging.info('vancouver_weather_app table created successfully')

def table_loader(table_name, weather_json):
    received_at = datetime.fromtimestamp((data.timestamp)/1e3)
    temp = weather_json['temp']
    wind_speed = weather_json['wind_speed']
    humidity = weather_json['humidity']
    insert_payload = f'''INSERT INTO {table_name} VALUES ('{received_at}', 
                                                        {temp}, 
                                                        {wind_speed}, 
                                                        {humidity}
                                                       )'''
    cursor.execute(insert_payload)



consumer = KafkaConsumer('weather_app',
                         bootstrap_servers = 'localhost:9092',
                         auto_offset_reset = 'earliest',
                         enable_auto_commit = True)

count = 0
while True:
    try:
        for data in consumer:
            message = json.loads(data.value.decode('utf-8'))
            winnipeg = message['winnipeg']
            vancouver = message['vancouver']
            count+=1            
            table_loader('winnipeg_weather_app', winnipeg)
            logging.info(f'loaded {winnipeg} to database successfully')
            table_loader('vancouver_weather_app', vancouver)
            logging.info(f'loaded {vancouver} to database successfully')
    except KeyboardInterrupt:
        logging.info(f'user terminated after loading {count} records to database')
        break
        


con.close()