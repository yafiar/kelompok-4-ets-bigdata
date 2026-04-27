import json
from kafka import KafkaConsumer
import logging
import os

# Configuration
TOPICS = ['pangan-api', 'pangan-rss']
KAFKA_SERVER = 'localhost:9092'
LOCAL_DATA_PATH = '../dashboard/data/'

logging.basicConfig(level=logging.INFO)

def run_consumer():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    
    for message in consumer:
        data = message.value
        topic = message.topic
        logging.info(f"Received from {topic}")
        
        # Simpan ke HDFS (Placeholder)
        # client = InsecureClient('http://namenode:9870', user='hadoop')
        # hdfs_path = f'/data/pangan_raw/{topic.split("-")[-1]}/{message.timestamp}.json'
        # client.write(hdfs_path, json.dumps(data))
        
        # Simpan ke lokal untuk dashboard
        filename = f"live_{topic.split('-')[-1]}.json"
        with open(os.path.join(LOCAL_DATA_PATH, filename), 'w') as f:
            json.dump(data, f)

if __name__ == "__main__":
    run_consumer()
