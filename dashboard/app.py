from datetime import datetime
import json
from pathlib import Path

from flask import Flask, jsonify, render_template

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent


def _read_json_if_exists(*relative_paths):
    for relative_path in relative_paths:
        file_path = BASE_DIR / relative_path
        if not file_path.exists():
            continue
        try:
            with file_path.open("r", encoding="utf-8") as file_handle:
                data = json.load(file_handle)
            updated_at = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(timespec="seconds")
            return data, str(file_path), updated_at
        except (OSError, json.JSONDecodeError):
            continue
    return None, None, None


def _first_list(payload, *keys):
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def _normalize_live_payload(data, source_file, updated_at, fallback_message, label):
    if isinstance(data, list):
        items = data
        payload = {}
    elif isinstance(data, dict):
        payload = dict(data)
        items = _first_list(payload, "items", "data")
        if not items and source_file:
            items = [payload]
    else:
        payload = {}
        items = []

    payload["items"] = items
    payload.setdefault("status", "ok" if source_file else "placeholder")
    payload.setdefault("updated_at", updated_at or datetime.now().isoformat(timespec="seconds"))
    payload.setdefault("message", fallback_message if not source_file else f"Data {label} terakhir disinkronkan.")
    payload["source_file"] = source_file
    return payload


def _normalize_spark_payload(data, source_file, updated_at):
    if isinstance(data, list):
        payload = {"volatility_ranking": data}
    elif isinstance(data, dict):
        payload = dict(data)
    else:
        payload = {}

    payload["volatility_ranking"] = _first_list(payload, "volatility_ranking", "ranking", "results", "data")
    payload["price_trends"] = _first_list(payload, "price_trends", "trends")
    payload["news_correlation"] = _first_list(payload, "news_correlation", "correlation")
    payload.setdefault("status", "ok" if source_file else "placeholder")
    payload.setdefault("updated_at", updated_at or datetime.now().isoformat(timespec="seconds"))
    payload.setdefault("message", "Hasil Spark tersedia." if source_file else "Task Spark belum selesai.")
    payload.setdefault("interpretation", payload.get("summary") or "Narasi interpretasi belum diisi.")
    payload["source_file"] = source_file
    return payload


def _placeholder_payloads():
    now_iso = datetime.now().isoformat(timespec="seconds")
    spark_placeholder = {
        "status": "placeholder",
        "updated_at": now_iso,
        "source_file": None,
        "message": "Task Spark (anggota 4) belum selesai. Hasil analisis akan tampil setelah spark_results.json tersedia.",
        "volatility_ranking": [],
        "price_trends": [],
        "news_correlation": [],
        "interpretation": "Placeholder: belum ada narasi interpretasi dari Spark.",
    }
    live_api_placeholder = {
        "status": "placeholder",
        "updated_at": now_iso,
        "source_file": None,
        "message": "Task producer API / consumer belum sinkron ke dashboard/data/live_api.json.",
        "items": [],
    }
    live_rss_placeholder = {
        "status": "placeholder",
        "updated_at": now_iso,
        "source_file": None,
        "message": "Task producer RSS / consumer belum sinkron ke dashboard/data/live_rss.json.",
        "items": [],
    }
    return spark_placeholder, live_api_placeholder, live_rss_placeholder


def _normalize_items(data):
    if not isinstance(data, dict):
        return {"items": []}
    if "items" not in data and isinstance(data.get("data"), list):
        data["items"] = data["data"]
    data.setdefault("items", [])
    return data


def get_dashboard_payload():
    spark_placeholder, api_placeholder, rss_placeholder = _placeholder_payloads()

    spark_data, spark_source, spark_updated_at = _read_json_if_exists(
        "data/spark_results.json",
        "../data/pangan/hasil/spark_results.json",
    )
    api_data, api_source, api_updated_at = _read_json_if_exists(
        "data/live_api.json",
        "../data/pangan/api/live_api.json",
    )
    rss_data, rss_source, rss_updated_at = _read_json_if_exists(
        "data/live_rss.json",
        "../data/pangan/rss/live_rss.json",
    )

    spark_payload = _normalize_spark_payload(
        spark_data if spark_data is not None else spark_placeholder,
        spark_source,
        spark_updated_at,
    )

    api_payload = _normalize_live_payload(
        api_data if api_data is not None else api_placeholder,
        api_source,
        api_updated_at,
        api_placeholder["message"],
        "API",
    )

    rss_payload = _normalize_live_payload(
        rss_data if rss_data is not None else rss_placeholder,
        rss_source,
        rss_updated_at,
        rss_placeholder["message"],
        "RSS",
    )

    return {
        "spark": spark_payload,
        "live_api": api_payload,
        "live_rss": rss_payload,
    }

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_dashboard_data():
    return jsonify(get_dashboard_payload())

@app.route('/api/spark')
def get_spark_results():
    return jsonify(get_dashboard_payload()["spark"])

@app.route('/api/live-api')
def get_live_api():
    return jsonify(get_dashboard_payload()["live_api"])

@app.route('/api/live-rss')
def get_live_rss():
    return jsonify(get_dashboard_payload()["live_rss"])

if __name__ == '__main__':
    app.run(debug=True, port=5000)
