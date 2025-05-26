# Створити python або java проєкти які будуть відігравати роль споживачів (Consumer1,
# Consumer2).
# 6. Таким чином, один споживач буде відповідати за обробку повідомлень з одного топік
# простим виводом даних у консоль
# ---------
# Споживачі акумулюють їх по місяцях і зберігають у CSV файли з наступною назвою
# month_year.csv.
# 4. До всіх споживачів додати залежність бібліотеки minio. Посилання з інструкцією буде
# вказана в низу.
# 5. Як тільки споживачі почнуть записувати повідомлення у файл наступного місяця (e.g.
# may_2020.csv) розпочати завантаження попереднього файлу (e.g. april_2020.csv) до
# сховища об’єктів.
# 6. Після успішного завантаження файлу до сховища об’єктів видаліть його на боці
# споживача
from kafka import KafkaConsumer
import time
import json
import os
import csv
import datetime
from minio import Minio
from MonthlyFileHandler import MonthlyFileHandler

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'broker-1:9092,broker-2:9093').split(',')
TOPIC = os.getenv('TOPIC', '')

print(f"BOOTSTRAP_SERVERS: {BOOTSTRAP_SERVERS}")
print(f"TOPIC: {TOPIC}")

minio_client = Minio(
    os.getenv('MINIO_ENDPOINT', 'minio:9000'),
    access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
    secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
    secure=False
)
bucket_name = os.getenv('MINIO_BUCKET', 'default-bucket')

consumer = None
while consumer is None:
    try:
        print("Connecting to Kafka...")
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
        )
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        time.sleep(3)

if not minio_client.bucket_exists(bucket_name):
    print(f"Bucket {bucket_name} does not exist, creating it.")
    minio_client.make_bucket(bucket_name)

file_handler = MonthlyFileHandler(minio_client)
for message in consumer:
    print(f"Received message: {message.value}")
    file_handler.process_message(message.value)

consumer.close()


