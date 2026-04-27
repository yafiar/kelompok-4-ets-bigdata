"""
producer_api.py — Kafka Producer: Harga Pangan
Anggota 2 — ETS Big Data Kelompok 4

Strategi pengambilan data:
  1. (Utama)  Simulator realistis berbasis harga pasar Indonesia
              → aman dipakai kapan saja, tidak bergantung koneksi eksternal
  2. (Bonus)  Bisa diganti ke Panel Harga / World Bank API cukup dengan
              mengubah USE_SIMULATOR = False dan mengisi API_URL

Topic Kafka  : pangan-api
Key          : nama komoditas (beras, cabai_merah, dll)
Interval     : DEFAULT_INTERVAL_SEC (default 30 detik, ubah jadi 1800 untuk produksi)
"""

import json
import logging
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ─────────────────────────────────────────────
# KONFIGURASI — edit bagian ini sesuai kebutuhan
# ─────────────────────────────────────────────
KAFKA_SERVER       = "localhost:9092"
TOPIC_NAME         = "pangan-api"
DEFAULT_INTERVAL_SEC = 30          # 30 detik untuk demo; ganti 1800 untuk produksi
USE_SIMULATOR      = True          # False → pakai Panel Harga API (lihat bagian bawah)
# API_URL          = "https://panelharga.badanpangan.go.id/chart/komoditas-json" # (opsional)

# ─────────────────────────────────────────────
# DATA REFERENSI HARGA (Rp, per satuan)
# Sumber acuan: rata-rata harga pasar Indonesia 2025-2026
# ─────────────────────────────────────────────
KOMODITAS_CONFIG = {
    "beras": {
        "satuan"    : "kg",
        "harga_base": 14_500,
        "volatilitas": 0.02,   # ±2% fluktuasi wajar
    },
    "cabai_merah": {
        "satuan"    : "kg",
        "harga_base": 40_000,
        "volatilitas": 0.15,   # cabai sangat fluktuatif
    },
    "cabai_rawit": {
        "satuan"    : "kg",
        "harga_base": 55_000,
        "volatilitas": 0.18,
    },
    "bawang_merah": {
        "satuan"    : "kg",
        "harga_base": 35_000,
        "volatilitas": 0.10,
    },
    "bawang_putih": {
        "satuan"    : "kg",
        "harga_base": 38_000,
        "volatilitas": 0.08,
    },
    "minyak_goreng": {
        "satuan"    : "liter",
        "harga_base": 17_500,
        "volatilitas": 0.03,
    },
    "gula_pasir": {
        "satuan"    : "kg",
        "harga_base": 16_000,
        "volatilitas": 0.02,
    },
    "telur_ayam": {
        "satuan"    : "kg",
        "harga_base": 28_000,
        "volatilitas": 0.05,
    },
    "daging_ayam": {
        "satuan"    : "kg",
        "harga_base": 35_000,
        "volatilitas": 0.06,
    },
    "tepung_terigu": {
        "satuan"    : "kg",
        "harga_base": 12_000,
        "volatilitas": 0.02,
    },
}

# ─────────────────────────────────────────────
# STATE: simpan harga sebelumnya agar simulasi
# terasa "bergerak" dan tidak acak total
# ─────────────────────────────────────────────
_harga_state: dict[str, float] = {
    k: float(v["harga_base"]) for k, v in KOMODITAS_CONFIG.items()
}


# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("producer_api")


# ─────────────────────────────────────────────
# SIMULATOR — menghasilkan data harga realistis
# ─────────────────────────────────────────────
def simulate_harga(komoditas: str) -> dict:
    """
    Menghasilkan satu record harga dengan random-walk kecil dari harga sebelumnya.
    Model: harga_baru = harga_lama * (1 + N(0, volatilitas))
    Clamped agar tidak keluar ±40% dari harga base.
    """
    cfg   = KOMODITAS_CONFIG[komoditas]
    prev  = _harga_state[komoditas]
    delta = random.gauss(0, cfg["volatilitas"])      # distribusi normal
    harga = prev * (1 + delta)

    # Clamp supaya tidak terlalu jauh dari realita
    batas_bawah = cfg["harga_base"] * 0.60
    batas_atas  = cfg["harga_base"] * 1.40
    harga = max(batas_bawah, min(batas_atas, harga))

    _harga_state[komoditas] = harga

    return {
        "komoditas" : komoditas,
        "harga"     : round(harga),           # Rp bulat
        "satuan"    : cfg["satuan"],
        "timestamp" : datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "sumber"    : "simulator",
    }


# ─────────────────────────────────────────────
# (OPSIONAL) PANEL HARGA API
# Aktifkan dengan USE_SIMULATOR = False
# ─────────────────────────────────────────────
def fetch_panel_harga() -> list[dict]:
    """
    Contoh integrasi Panel Harga Badan Pangan Nasional.
    Jika API berubah atau tidak tersedia, balik ke simulator otomatis.
    """
    import requests  # noqa: import inside function — hanya dipanggil jika dibutuhkan

    try:
        resp = requests.get(
            "https://panelharga.badanpangan.go.id/chart/komoditas-json",
            timeout=10,
        )
        resp.raise_for_status()
        raw = resp.json()

        records = []
        for item in raw.get("data", []):
            records.append({
                "komoditas" : item.get("komoditas_nama", "unknown").lower().replace(" ", "_"),
                "harga"     : int(item.get("harga", 0)),
                "satuan"    : "kg",
                "timestamp" : datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
                "sumber"    : "panel_harga_api",
            })
        return records

    except Exception as exc:
        log.warning(f"Panel Harga API gagal ({exc}), fallback ke simulator.")
        return []


# ─────────────────────────────────────────────
# FUNGSI UTAMA PENGIRIMAN
# ─────────────────────────────────────────────
def get_all_data() -> list[dict]:
    """
    Kumpulkan data dari semua komoditas.
    Pakai simulator atau API tergantung USE_SIMULATOR.
    """
    if USE_SIMULATOR:
        return [simulate_harga(k) for k in KOMODITAS_CONFIG]

    records = fetch_panel_harga()
    if not records:                         # fallback kalau API gagal
        log.info("Fallback ke simulator karena API kosong.")
        return [simulate_harga(k) for k in KOMODITAS_CONFIG]
    return records


def build_producer(retries: int = 5, delay: int = 5) -> KafkaProducer:
    """
    Buat KafkaProducer dengan retry — berguna saat container Kafka
    belum sepenuhnya ready waktu script pertama kali dijalankan.
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",               # tunggu konfirmasi dari broker
                retries=3,
            )
            log.info(f"✅  Terhubung ke Kafka: {KAFKA_SERVER}")
            return producer
        except NoBrokersAvailable:
            log.warning(f"⏳  Kafka belum siap (percobaan {attempt}/{retries}), tunggu {delay}s...")
            time.sleep(delay)

    raise RuntimeError(f"❌  Tidak bisa terhubung ke Kafka setelah {retries} percobaan.")


def run_producer(interval: int = DEFAULT_INTERVAL_SEC):
    """
    Loop utama: ambil data → kirim ke Kafka → tunggu → ulangi.
    """
    producer = build_producer()

    log.info(f"🚀  Producer mulai — interval: {interval}s — topic: {TOPIC_NAME}")
    log.info(f"    Mode: {'SIMULATOR' if USE_SIMULATOR else 'PANEL HARGA API'}")
    log.info(f"    Komoditas: {', '.join(KOMODITAS_CONFIG.keys())}")
    print("-" * 60)

    iteration = 0
    while True:
        iteration += 1
        records = get_all_data()

        log.info(f"📦  Iterasi #{iteration} — mengirim {len(records)} record...")

        for record in records:
            key = record["komoditas"]
            future = producer.send(TOPIC_NAME, key=key, value=record)

            # Logging detail per komoditas
            log.info(
                f"   ✉  [{key}] Rp {record['harga']:,} / {record['satuan']} "
                f"→ topic={TOPIC_NAME}"
            )

        producer.flush()   # pastikan semua pesan terkirim sebelum sleep
        log.info(f"✅  Semua data terkirim. Menunggu {interval}s hingga pengiriman berikutnya...\n")
        time.sleep(interval)


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Kafka Producer: Harga Pangan")
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SEC,
        help=f"Interval pengiriman dalam detik (default: {DEFAULT_INTERVAL_SEC})",
    )
    args = parser.parse_args()

    run_producer(interval=args.interval)