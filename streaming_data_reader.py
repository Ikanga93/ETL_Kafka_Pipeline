"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
from config import load_config
import uuid

TOPIC='toll'
DATABASE = 'toll_data'
USERNAME = 'postgres'
PASSWORD = 'D2racine4ac#'
HOST = 'localhost'

# Load configuration
# config = load_config()

# Connect to the database
# print("Connecting to the database")

# Connect to the PostgreSQL database server.
try: 
    # connect to the PostgreSQL server
    # with psql.connect(**config) as conn:
    conn = psycopg2.connect(dbname=DATABASE, user=USERNAME, password=PASSWORD, host=HOST)
    print('Connected to the PostgreSQL server...')
    cur = conn.cursor()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)

# Connect to Kafka
print("Connecting to Kafka")
try:
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: x.decode('utf-8'))
    print("Connected to Kafka")
except Exception as e:
    print(f"Could not connect to Kafka. Please check if the Kafka server is running. {e}")
    exit()

print(f"Reading messages from the topic {TOPIC}")
try:
    for msg in consumer:

        # Extract information from kafka
        message = msg.value

        # Transform the date format to suit the database schema
        (timestamp, vehcile_id, vehicle_type, plaza_id) = message.split(",")
        dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
        timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

        # Loading data into the database table
        sql = "insert into livetolldata (timestamp, vehicle_id, vehicle_type, plaza_id) values(%s,%s,%s,%s)"
        result = cur.execute(sql, (timestamp, vehcile_id, vehicle_type, plaza_id))
        conn.commit()
        print(f"A {vehicle_type} was inserted into the database")
except Exception as e:
    print(f"An error occured while inserting data into the database. {e}")
    cur.close()
    conn.close()
    consumer.close()