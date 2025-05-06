# Створити python або java проєкти які будуть відігравати роль споживачів (Consumer1,
# Consumer2).
# 6. Таким чином, один споживач буде відповідати за обробку повідомлень з одного топік
# простим виводом даних у консоль
from kafka import KafkaConsumer
import time
import json
import os


BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'broker-1:9092,broker-2:9093').split(',')
TOPIC = os.getenv('TOPIC', '')

print(f"BOOTSTRAP_SERVERS: {BOOTSTRAP_SERVERS}")
print(f"TOPIC: {TOPIC}")

consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
        )
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        time.sleep(3)

for message in consumer:
    print(f"Received message: {message.value}")

consumer.close()


