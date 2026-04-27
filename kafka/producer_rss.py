import time
import json
import feedparser
from kafka import KafkaProducer
import logging

# Configuration
TOPIC_NAME = 'pangan-rss'
KAFKA_SERVER = 'localhost:9092'
RSS_URL = 'https://example.com/rss' # Ganti dengan RSS asli

logging.basicConfig(level=logging.INFO)

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    seen_entries = set()
    
    while True:
        feed = feedparser.parse(RSS_URL)
        for entry in feed.entries:
            if entry.id not in seen_entries:
                producer.send(TOPIC_NAME, entry)
                seen_entries.add(entry.id)
                logging.info(f"Sent RSS entry: {entry.title}")
        
        time.sleep(300) # Check every 5 minutes

if __name__ == "__main__":
    run_producer()
