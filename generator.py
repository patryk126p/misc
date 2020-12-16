from time import sleep
from datetime import datetime
import json
import random
import argparse
from faker import Faker
from kafka import KafkaProducer

parser = argparse.ArgumentParser(description="Kafka data generator")
parser.add_argument("kafka", help="address of one of kafka servers")
args = parser.parse_args()

fake = Faker()

producer = KafkaProducer(bootstrap_servers=[f"{args.kafka}:9092"],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                         api_version=(2, 6, 0))

print("Producer started")

try:
    while True:
        message = {"time": int(datetime.utcnow().timestamp()), "age": random.randint(0, 100), 
                   "name": fake.name(), "address": fake.address().replace("\n", " "), "zipcode": fake.zipcode(),
                   "action": random.choice("abcdef")}
        producer.send("data", value=message)
        sleep(1)
except KeyboardInterrupt:
    producer.close()
