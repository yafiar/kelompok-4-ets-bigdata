import time
import json
import requests
from kafka import KafkaProducer
import logging

# Configuration
TOPIC_NAME = 'pangan-api'
KAFKA_SERVER = 'localhost:9092'
API_URL = 'https://api.example.com/pangan' # Ganti dengan API asli

logging.basicConfig(level=logging.INFO)

def get_pangan_data():
    # Ambil data dari API
    # response = requests.get(API_URL)
    # return response.json()
    return {"status": "placeholder", "data": []}

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    while True:
        data = get_pangan_data()
        producer.send(TOPIC_NAME, data)
        logging.info(f"Sent data to {TOPIC_NAME}")
        time.sleep(60) # Interval polling

if __name__ == "__main__":
    run_producer()
