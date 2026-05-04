import json
import os
import time
from datetime import datetime, timezone
import tempfile
from kafka import KafkaConsumer

KAFKA_SERVER = "localhost:9092"
TOPICS = ["pangan-api", "pangan-rss"]

# AUTO PATH
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Dashboard
DASHBOARD_PATH = os.path.join(BASE_DIR, "dashboard", "data")

# Data Lake
DATA_LAKE_API = os.path.join(BASE_DIR, "data", "pangan", "api")
DATA_LAKE_RSS = os.path.join(BASE_DIR, "data", "pangan", "rss")

# buat folder
os.makedirs(DASHBOARD_PATH, exist_ok=True)
os.makedirs(DATA_LAKE_API, exist_ok=True)
os.makedirs(DATA_LAKE_RSS, exist_ok=True)

def atomic_write_json(path, data):
    """Writes JSON atomically using a temporary file to avoid race conditions.

    On Windows the destination file may be briefly locked by another process
    (e.g. a reader or antivirus). Retry a few times on PermissionError
    before giving up.
    """
    dirpath = os.path.dirname(path)
    retries = 5
    delay = 0.1

    for attempt in range(retries):
        fd, temp_path = tempfile.mkstemp(dir=dirpath)
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            try:
                os.replace(temp_path, path)
                return
            except PermissionError:
                # Cleanup temp and retry after a short backoff
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except Exception:
                    pass
                if attempt < retries - 1:
                    time.sleep(delay * (2 ** attempt))
                    continue
                raise
        except Exception:
            # Ensure temp file removed on unexpected errors
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass
            raise

def run_consumer():
    print("Consumer running (optimized)...")

    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=[KAFKA_SERVER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='dashboard-consumer-v10-final-check'
        )
    except Exception as e:
        print(f"Gagal koneksi Kafka: {e}")
        return

    # In-memory store for dashboard (latest items)
    dashboard_api_items = {}  # komoditas -> latest_record
    dashboard_rss_items = []  # list of latest news

    # Pre-load existing dashboard files so a restart doesn't blank the dashboard
    try:
        existing_api = os.path.join(DASHBOARD_PATH, "live_api.json")
        if os.path.exists(existing_api):
            with open(existing_api, 'r', encoding='utf-8') as f:
                saved = json.load(f)
                for item in saved.get('items', []):
                    name = item.get('nama_komoditas', '')
                    prov = item.get('provinsi', 'Nasional')
                    slug = name.lower().strip().replace(' ', '_')
                    if slug:
                        dashboard_api_items[f"{slug}_{prov}"] = item
            print(f"Pre-loaded {len(dashboard_api_items)} API items from existing dashboard file")
    except Exception as e:
        print(f"Could not pre-load API items: {e}")

    try:
        existing_rss = os.path.join(DASHBOARD_PATH, "live_rss.json")
        if os.path.exists(existing_rss):
            with open(existing_rss, 'r', encoding='utf-8') as f:
                saved = json.load(f)
                dashboard_rss_items = saved.get('items', [])[:20]
            print(f"Pre-loaded {len(dashboard_rss_items)} RSS items from existing dashboard file")
    except Exception as e:
        print(f"Could not pre-load RSS items: {e}")

    last_save_time = time.time()

    for message in consumer:
        topic = message.topic
        data = message.value

        if topic == "pangan-api":
            kom = data.get("komoditas", "unknown")
            prov = data.get("provinsi", "Nasional")
            name = data.get("nama_komoditas")

            # Normalize and Filter for Dashboard
            if name and name != "undefined":
                # Only keep records from the last 24 hours on the dashboard
                try:
                    msg_ts = datetime.fromisoformat(data.get("timestamp"))
                    if (datetime.now(timezone.utc) - msg_ts).total_seconds() < 86400:
                        # Use a consistent slug for the dashboard key
                        slug = name.lower().strip().replace(" ", "_")
                        dashboard_api_items[f"{slug}_{prov}"] = data
                        print(f"API Saved: {slug} - {prov}")
                except Exception as e:
                    # Fallback for old/malformed timestamps
                    print(f"Filter Error: {e}")
                    pass

            # Append to data lake (all data)
            lake_file = os.path.join(DATA_LAKE_API, "api_data.json")
            with open(lake_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(data) + "\n")
            print(f"API: {kom}")

        elif topic == "pangan-rss":
            # Filter for specific sources to avoid old news from other feeds
            valid_sources = [
                "https://www.cnnindonesia.com/ekonomi/rss",
                "https://finance.detik.com/rss"
            ]
            if data.get("source") not in valid_sources:
                continue

            dashboard_rss_items.insert(0, data)
            dashboard_rss_items = dashboard_rss_items[:20]  # Keep last 20
            # Append to data lake
            lake_file = os.path.join(DATA_LAKE_RSS, "rss_data.json")
            with open(lake_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(data) + "\n")
            print(f"RSS: {data.get('judul', 'news')}")

        # Save to dashboard files every 2 seconds or on change
        if time.time() - last_save_time >= 2:
            print("Updating dashboard files...")
            
            # Save API (all commodities)
            api_payload = {"items": list(dashboard_api_items.values())}
            atomic_write_json(os.path.join(DASHBOARD_PATH, "live_api.json"), api_payload)

            # Save RSS
            rss_payload = {"items": dashboard_rss_items}
            atomic_write_json(os.path.join(DASHBOARD_PATH, "live_rss.json"), rss_payload)

            last_save_time = time.time()

if __name__ == "__main__":
    run_consumer()