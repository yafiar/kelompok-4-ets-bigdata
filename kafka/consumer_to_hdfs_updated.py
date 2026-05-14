import json
import os
import time
from datetime import datetime, timezone
import tempfile
import subprocess
from kafka import KafkaConsumer
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("consumer_to_hdfs")

KAFKA_SERVER = "localhost:9092"
TOPICS = ["pangan-api", "pangan-rss"]

# AUTO PATH
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Dashboard (local)
DASHBOARD_PATH = os.path.join(BASE_DIR, "dashboard", "data")

# HDFS Paths
HDFS_NAMENODE = "hdfs://localhost:9000"
HDFS_API_PATH = f"{HDFS_NAMENODE}/data/pangan/api"
HDFS_RSS_PATH = f"{HDFS_NAMENODE}/data/pangan/rss"
HDFS_CHECKPOINT_PATH = f"{HDFS_NAMENODE}/data/pangan/checkpoints"

# Local temp directories for staging
LOCAL_STAGING_API = os.path.join(BASE_DIR, "data", "pangan", "api_staging")
LOCAL_STAGING_RSS = os.path.join(BASE_DIR, "data", "pangan", "rss_staging")

# Ensure directories exist
os.makedirs(DASHBOARD_PATH, exist_ok=True)
os.makedirs(LOCAL_STAGING_API, exist_ok=True)
os.makedirs(LOCAL_STAGING_RSS, exist_ok=True)

def init_hdfs_directories():
    """Initialize HDFS directories if they don't exist."""
    try:
        for path in [HDFS_API_PATH, HDFS_RSS_PATH, HDFS_CHECKPOINT_PATH]:
            subprocess.run(
                ["docker", "exec", "-i", "namenode", "hdfs", "dfs", "-mkdir", "-p", path],
                check=False,
                capture_output=True,
            )
        log.info("HDFS directories initialized")
    except Exception as e:
        log.warning("Could not initialize HDFS directories: %s", e)

def put_file_to_hdfs(local_path, hdfs_path):
    """Upload a file to HDFS using namenode container."""
    try:
        # Copy file into container and then to HDFS
        filename = os.path.basename(local_path)
        container_path = f"/tmp/{filename}"
        
        # Copy to container
        subprocess.run(
            ["docker", "cp", local_path, f"namenode:{container_path}"],
            check=True,
            capture_output=True,
        )
        
        # Put to HDFS
        subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", container_path, f"{hdfs_path}/"],
            check=True,
            capture_output=True,
        )
        
        log.info(f"Successfully uploaded {filename} to HDFS: {hdfs_path}")
        return True
    except Exception as e:
        log.error(f"Failed to upload to HDFS: %s", e)
        return False

def atomic_write_json(path, data):
    """Writes JSON atomically using a temporary file."""
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
                return True
            except PermissionError:
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except Exception:
                    pass
                if attempt < retries - 1:
                    time.sleep(delay * (2 ** attempt))
                    continue
                raise
        except Exception as e:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass
            log.error(f"Error writing JSON: %s", e)
            raise
    return False


def flush_buffer_to_hdfs(buffer, staging_dir, filename_prefix, hdfs_path, label):
    """Persist a buffered batch to local staging and HDFS."""
    if not buffer:
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    local_file = os.path.join(staging_dir, f"{filename_prefix}_{timestamp}.json")
    atomic_write_json(local_file, buffer)
    put_file_to_hdfs(local_file, hdfs_path)
    buffer.clear()
    log.info("Flushed %s data to HDFS", label)

def run_consumer():
    log.info("Consumer running (HDFS-enabled)...")
    
    # Initialize HDFS directories
    init_hdfs_directories()

    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=[KAFKA_SERVER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='hdfs-consumer-v1'
        )
    except Exception as e:
        log.error(f"Failed to connect to Kafka: {e}")
        return

    # In-memory stores
    dashboard_api_items = {}
    dashboard_rss_items = []
    hdfs_api_buffer = []
    hdfs_rss_buffer = []
    
    BUFFER_SIZE = 100  # Flush to HDFS every N messages
    DASHBOARD_SYNC_INTERVAL = 30  # Sync dashboard data every 30 seconds

    last_dashboard_sync = time.time()

    log.info("Listening to Kafka topics...")

    try:
        for message in consumer:
            topic = message.topic
            value = message.value

            if topic == "pangan-api":
                # Add to HDFS buffer
                hdfs_api_buffer.append(value)
                
                # Update dashboard
                name = value.get('nama_komoditas', '')
                prov = value.get('provinsi', 'Nasional')
                slug = name.lower().strip().replace(' ', '_')
                if slug:
                    dashboard_api_items[f"{slug}_{prov}"] = value

            elif topic == "pangan-rss":
                # Add to HDFS buffer
                hdfs_rss_buffer.append(value)
                
                # Update dashboard
                dashboard_rss_items.insert(0, value)
                if len(dashboard_rss_items) > 50:
                    dashboard_rss_items.pop()

                # RSS is low-volume, so persist it immediately instead of waiting for the batch window.
                flush_buffer_to_hdfs(hdfs_rss_buffer, LOCAL_STAGING_RSS, "rss_data", HDFS_RSS_PATH, "RSS")

            # Flush HDFS buffers periodically
            if len(hdfs_api_buffer) >= BUFFER_SIZE:
                flush_buffer_to_hdfs(hdfs_api_buffer, LOCAL_STAGING_API, "api_data", HDFS_API_PATH, "API")

            if len(hdfs_rss_buffer) >= BUFFER_SIZE:
                flush_buffer_to_hdfs(hdfs_rss_buffer, LOCAL_STAGING_RSS, "rss_data", HDFS_RSS_PATH, "RSS")

            # Sync dashboard data periodically
            current_time = time.time()
            if current_time - last_dashboard_sync >= DASHBOARD_SYNC_INTERVAL:
                # Make sure small batches are persisted even when the buffer is below the size threshold.
                flush_buffer_to_hdfs(hdfs_api_buffer, LOCAL_STAGING_API, "api_data", HDFS_API_PATH, "API")
                flush_buffer_to_hdfs(hdfs_rss_buffer, LOCAL_STAGING_RSS, "rss_data", HDFS_RSS_PATH, "RSS")

                # Write live API data
                api_output = os.path.join(DASHBOARD_PATH, "live_api.json")
                api_data = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'items': list(dashboard_api_items.values()),
                    'total': len(dashboard_api_items)
                }
                atomic_write_json(api_output, api_data)

                # Write live RSS data
                rss_output = os.path.join(DASHBOARD_PATH, "live_rss.json")
                rss_data = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'items': dashboard_rss_items,
                    'total': len(dashboard_rss_items)
                }
                atomic_write_json(rss_output, rss_data)

                log.info(f"Dashboard synced: {len(dashboard_api_items)} API items, {len(dashboard_rss_items)} RSS items")
                last_dashboard_sync = current_time

    except KeyboardInterrupt:
        log.info("Consumer interrupted by user")
        # Flush remaining data to HDFS before exiting
        flush_buffer_to_hdfs(hdfs_api_buffer, LOCAL_STAGING_API, "api_data_final", HDFS_API_PATH, "API")
        flush_buffer_to_hdfs(hdfs_rss_buffer, LOCAL_STAGING_RSS, "rss_data_final", HDFS_RSS_PATH, "RSS")
    except Exception as e:
        log.error(f"Error in consumer: %s", e)
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumer()
