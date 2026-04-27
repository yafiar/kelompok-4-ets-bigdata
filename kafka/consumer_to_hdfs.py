import json
import os
import time
from kafka import KafkaConsumer

KAFKA_SERVER = "localhost:9092"
TOPICS = ["pangan-api", "pangan-rss"]

# AUTO PATH
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Dashboard (WAJIB SESUAI SOAL)
DASHBOARD_PATH = os.path.join(BASE_DIR, "dashboard", "data")

# Data Lake (simulasi HDFS)
DATA_LAKE_API = os.path.join(BASE_DIR, "data", "pangan", "api")
DATA_LAKE_RSS = os.path.join(BASE_DIR, "data", "pangan", "rss")

# buat folder
os.makedirs(DASHBOARD_PATH, exist_ok=True)
os.makedirs(DATA_LAKE_API, exist_ok=True)
os.makedirs(DATA_LAKE_RSS, exist_ok=True)

def run_consumer():
    print("🚀 Consumer jalan...")

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    buffer = []
    last_save_time = time.time()

    for message in consumer:
        topic = message.topic
        data = message.value

        print(f"📥 {topic}: {data['judul']}")
        buffer.append((topic, data))

        # 🔥 BUFFER 10 DETIK (biar cepat kelihatan, bisa ubah ke 120 nanti)
        if time.time() - last_save_time >= 10:
            print("\n💾 Simpan batch...")

            for topic, data in buffer:

                if topic == "pangan-rss":
                    dash_file = os.path.join(DASHBOARD_PATH, "live_rss.json")
                    lake_file = os.path.join(DATA_LAKE_RSS, "rss_data.json")
                else:
                    dash_file = os.path.join(DASHBOARD_PATH, "live_api.json")
                    lake_file = os.path.join(DATA_LAKE_API, "api_data.json")

                # ✅ Dashboard (latest only)
                with open(dash_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4, ensure_ascii=False)

                # ✅ Data Lake (append)
                with open(lake_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(data) + "\n")

            buffer = []
            last_save_time = time.time()

            print("✅ Berhasil simpan ke:")
            print("   - dashboard/data/")
            print("   - data/pangan/")

if __name__ == "__main__":
    run_consumer()