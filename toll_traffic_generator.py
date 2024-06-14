"""
Top Traffic Simulator
"""

# Import necessary modules from Python standard library
from time import sleep, time, ctime
from random import random, randint, choice
from kafka import KafkaProducer
import csv

# Initialize a kafka producer to send messages to kafka.
# The producer will connect to the kafka server running on localhost at port 9092.
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Define the Kafka topic to which messages will be sent.
TOPIC = 'toll'

# Define a tuple of vehicle types, where the probability of a selecting a 'car' is higher than the other vehicle types.
VEHICLE_TYPES = ("car", "car", "car", "car", "car", "car", "car", "car",
                 "car", "car", "car", "truck", "truck", "truck",
                 "truck", "van", "van")

# File path to save the live toll data
file_path = '/Users/jbshome/Desktop/ETL_kafka_pipeline/live_toll_data.csv'
# Open the CSV file in write mode
with open(file_path, mode='w', newline='') as file:
    writer = csv.writer(file)

    # Write the header row to the CSV file
    writer.writerow(['timestamp', 'vehicle_id', 'vehicle_type', 'plaza_id'])

# Start a loop that will run 100,000 times to simulate vehicle data
    for _ in range(100000):
        # Generate a random vehicle id between 10,000 and 10,000,000 
        vehicle_id = randint(10000, 10000000)

        # Randomly select a vehicle type from the vehicle_type tuple
        vehicle_type = choice(VEHICLE_TYPES)

        # Get the current time and format it as a string
        now = ctime(time())

        # Generate a random plaza_id between 4000 and 4010
        plaza_id = randint(4000, 4010)

        # Create a message string with the current time, vehicle id, vehicle type, and plaza id
        message = f"{now},{vehicle_id},{vehicle_type},{plaza_id}"

        # Convert the message string to a byte array
        message = bytearray(message.encode("utf-8"))

        # # Print a message to the console for debugging/monitoring
        print(f"A {vehicle_type} has passed by the toll plaza {plaza_id} at {now}.")

        # Send the message to the specified Kafka topic
        producer.send(TOPIC, message)

        # Write the row to the CSV file
        writer.writerow([now, vehicle_id, vehicle_type, plaza_id])

        # Flush the file buffer to ensure the data is written to the file
        file.flush()

        # # Sleep for a random amount of time (between 0 and 2 seconds) before sending the next message
        sleep(random() * 2)
