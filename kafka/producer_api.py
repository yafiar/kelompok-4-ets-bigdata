# [Ahmad Yafi Ar Rizq/5027241066]: National Scraper (All Provinces)
import json
import logging
import re
import time
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "pangan-api"

BI_BASE_URL = "https://www.bi.go.id/hargapangan"
BI_GRID_ENDPOINT = f"{BI_BASE_URL}/WebSite/Home/GetGridData1"

# List of Province IDs in PIHPS
PROVINCES = [
    {"id": "1", "name": "Aceh"}, {"id": "2", "name": "Sumatera Utara"}, {"id": "3", "name": "Sumatera Barat"},
    {"id": "4", "name": "Riau"}, {"id": "5", "name": "Kepulauan Riau"}, {"id": "6", "name": "Jambi"},
    {"id": "7", "name": "Sumatera Selatan"}, {"id": "8", "name": "Bangka Belitung"}, {"id": "9", "name": "Bengkulu"},
    {"id": "10", "name": "Lampung"}, {"id": "11", "name": "DKI Jakarta"}, {"id": "12", "name": "Jawa Barat"},
    {"id": "13", "name": "Banten"}, {"id": "14", "name": "Jawa Tengah"}, {"id": "15", "name": "DI Yogyakarta"},
    {"id": "16", "name": "Jawa Timur"}, {"id": "17", "name": "Bali"}, {"id": "18", "name": "Nusa Tenggara Barat"},
    {"id": "19", "name": "Nusa Tenggara Timur"}, {"id": "20", "name": "Kalimantan Barat"}, {"id": "21", "name": "Kalimantan Tengah"},
    {"id": "22", "name": "Kalimantan Selatan"}, {"id": "23", "name": "Kalimantan Timur"}, {"id": "24", "name": "Kalimantan Utara"},
    {"id": "25", "name": "Sulawesi Utara"}, {"id": "26", "name": "Gorontalo"}, {"id": "27", "name": "Sulawesi Tengah"},
    {"id": "28", "name": "Sulawesi Barat"}, {"id": "29", "name": "Sulawesi Selatan"}, {"id": "30", "name": "Sulawesi Tenggara"},
    {"id": "31", "name": "Maluku"}, {"id": "32", "name": "Maluku Utara"}, {"id": "33", "name": "Papua Barat"}, {"id": "34", "name": "Papua"}
]

BI_SUB_COMMODITIES = [
    {"name": "Bawang Merah Ukuran Sedang", "id": "5_11"},
    {"name": "Bawang Putih Ukuran Sedang", "id": "6_12"},
    {"name": "Beras Kualitas Bawah I", "id": "1_1"},
    {"name": "Beras Kualitas Medium I", "id": "1_3"},
    {"name": "Cabai Merah Besar", "id": "7_13"},
    {"name": "Cabai Merah Keriting ", "id": "7_14"},
    {"name": "Cabai Rawit Merah", "id": "8_16"},
    {"name": "Daging Ayam Ras Segar", "id": "2_7"},
    {"name": "Daging Sapi Kualitas 1", "id": "3_8"},
    {"name": "Minyak Goreng Curah", "id": "9_17"},
    {"name": "Telur Ayam Ras Segar", "id": "4_10"},
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("producer_all_provinces")

def _fetch_data(session, tree_id, prov_id, date_str):
    params = {"tanggal": date_str, "commodity": tree_id, "priceType": "1", "provId": prov_id}
    try:
        res = session.get(BI_GRID_ENDPOINT, params=params, timeout=15)
        return res.json().get("data", [])
    except: return []

def run_producer():
    producer = KafkaProducer(bootstrap_servers=[KAFKA_SERVER], value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    session = requests.Session()
    date_str = datetime.now().strftime("%d %b %Y")
    
    while True:
        log.info("Starting Full Sweep...")
        # 1. First, get National Averages
        for item in BI_SUB_COMMODITIES:
            rows = _fetch_data(session, item["id"], "0", date_str)
            for row in rows:
                record = {
                    "komoditas": item["name"].lower().replace(" ", "_"),
                    "nama_komoditas": item["name"],
                    "harga": int(float(str(row.get("Nilai", 0)).replace(",", ""))),
                    "provinsi": "Nasional",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "percentage": float(row.get("Percentage", 0))
                }
                if record["harga"] > 0:
                    producer.send(TOPIC_NAME, record)
        
        # 2. Then, get Province Details (Selective to avoid blocking)
        # We'll fetch 5 random provinces each cycle to eventually cover everything
        import random
        sample_provinces = random.sample(PROVINCES, 5)
        
        for prov in sample_provinces:
            log.info(f"Fetching data for province: {prov['name']}")
            for item in BI_SUB_COMMODITIES:
                rows = _fetch_data(session, item["id"], prov["id"], date_str)
                for row in rows:
                    record = {
                        "komoditas": item["name"].lower().replace(" ", "_"),
                        "nama_komoditas": item["name"],
                        "harga": int(float(str(row.get("Nilai", 0)).replace(",", ""))),
                        "provinsi": prov["name"],
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "percentage": float(row.get("Percentage", 0))
                    }
                    if record["harga"] > 0:
                        producer.send(TOPIC_NAME, record)
                time.sleep(0.5)
        
        log.info("Cycle finished.")
        producer.flush()
        time.sleep(10)

if __name__ == "__main__":
    run_producer()