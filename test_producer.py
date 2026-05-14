#!/usr/bin/env python3
"""
Simple test producer to verify Kafka is working on localhost
"""
import json
import logging
import time
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("test_producer")

def test_produce():
    log.info("Connecting to Kafka at localhost:9092...")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Test data
    test_record = {
        "nama_komoditas": "Beras Kualitas Bawah I",
        "harga": 12500,
        "provinsi": "DKI Jakarta",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
    }
    
    try:
        future = producer.send('pangan-api', test_record)
        record_metadata = future.get(timeout=10)
        log.info(f"✓ Message sent successfully to {record_metadata.topic} partition {record_metadata.partition}")
        log.info(f"  Data: {test_record}")
    except Exception as e:
        log.error(f"Failed to send message: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    test_produce()
