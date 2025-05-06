# Продюсер читатиме з csv файлу дані й на базі кожного запису формувати
# повідомлення/подію яку відправлятиме до Kafka кластер
# Kafka кластер повинен мати два топіки (Topic1,Topic2), кожне сформоване повідомлення
# продюсером потрібно публікувати у Topic1 і Topic2
from kafka import KafkaProducer
import time
import csv
import json
import os


BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'broker-1:9092,broker-2:9093').split(',')
TOPICS = os.getenv('TOPICS', 'Topic1,Topic2').split(',')
FILE_PATH = os.getenv('FILE_PATH', '/app/data/Divvy_Trips_2019_Q4.csv')

print(f"BOOTSTRAP_SERVERS: {BOOTSTRAP_SERVERS}")
print(f"TOPICS: {TOPICS}")
print(f"FILE_PATH: {FILE_PATH}")

producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        time.sleep(3)


ROW_READ_LIMIT = 15
with open(FILE_PATH, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for i, row in enumerate(reader):
        if i >= ROW_READ_LIMIT:
            break
        for topic in TOPICS:
            producer.send(topic, value=row)
        print(f"Sent: {row}")
        time.sleep(1)
        producer.flush()

producer.close()