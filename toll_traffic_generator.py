"""
Top Traffic Simulator
"""

# Import necessary modules from Python standard library
from time import sleep, time, ctime
from random import random, randint, choice
from kafka import KafkaProducer

# Initialize a kafka producer to send messages to kafka.
# The producer will connect to the kafka server running on localhost at port 9092.
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Define the Kafka topic to which messages will be sent.
TOPIC = 'toll'

# Define a tuple of vehicle types, where the probability of a selecting a 'car' is higher than the other vehicle types.
VEHICLE_TYPES = ("car", "car", "car", "car", "car", "car", "car", "car",
                 "car", "car", "car", "truck", "truck", "truck",
                 "truck", "van", "van")

# Start a loop that will run 100,000 times to simulate vehicle data
for _ in range(100000):
    vehicle_id = randint(10000, 10000000)
    vehicle_type = choice(VEHICLE_TYPES)
    now = ctime(time())
    plaza_id = randint(4000, 4010)
    message = f"{now},{vehicle_id},{vehicle_type},{plaza_id}"
    message = bytearray(message.encode("utf-8"))
    print(f"A {vehicle_type} has passed by the toll plaza {plaza_id} at {now}.")
    producer.send(TOPIC, message)
    sleep(random() * 2)
