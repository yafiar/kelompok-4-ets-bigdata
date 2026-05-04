# [Ahmad Yafi Ar Rizq/5027241066]: Dashboard Backend Logic
from datetime import datetime
import json
from pathlib import Path

from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent


def _read_json_if_exists(*relative_paths):
    for relative_path in relative_paths:
        file_path = BASE_DIR / relative_path
        if not file_path.exists():
            continue
        try:
            with file_path.open("r", encoding="utf-8") as file_handle:
                return json.load(file_handle), str(file_path)
        except (OSError, json.JSONDecodeError):
            continue
    return None, None


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

    spark_data, spark_source = _read_json_if_exists(
        "data/spark_results.json",
        "../data/pangan/hasil/spark_results.json",
    )
    api_data, api_source = _read_json_if_exists(
        "data/live_api.json",
        "../data/pangan/api/live_api.json",
    )
    rss_data, rss_source = _read_json_if_exists(
        "data/live_rss.json",
        "../data/pangan/rss/live_rss.json",
    )

    spark_payload = spark_data if isinstance(spark_data, dict) else spark_placeholder
    if spark_payload is not spark_placeholder:
        spark_payload.setdefault("status", "ok")
        spark_payload.setdefault("updated_at", datetime.now().isoformat(timespec="seconds"))
        spark_payload.setdefault("volatility_ranking", spark_payload.get("ranking", []))
        spark_payload.setdefault("price_trends", spark_payload.get("trends", []))
        spark_payload.setdefault("news_correlation", spark_payload.get("correlation", []))
        spark_payload.setdefault("interpretation", "Narasi interpretasi belum diisi.")
    spark_payload["source_file"] = spark_source

    api_payload = _normalize_items(api_data if isinstance(api_data, dict) else api_placeholder)
    
    # [Ahmad Yafi Ar Rizq/5027241066]: Historical Fallback Logic
    # If live items are empty or missing certain commodities, try to pull from data lake
    live_items = api_payload.get("items", [])
    present_commodities = {item.get("nama_komoditas") for item in live_items}
    
    lake_file = BASE_DIR / "../data/pangan/api/api_data.json"
    if lake_file.exists():
        try:
            with open(lake_file, "rb") as f:
                # Seek to end and read last 50KB to get recent history without loading GBs
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - 50000))
                chunk = f.read().decode('utf-8', errors='ignore')
                lines = chunk.splitlines()
                
                for line in reversed(lines):
                    try:
                        item = json.loads(line)
                        name = item.get("nama_komoditas")
                        if not name: continue
                        if name not in present_commodities:
                            item["is_historical"] = True
                            live_items.append(item)
                            present_commodities.add(name)
                    except: continue
        except: pass

    api_payload["items"] = live_items
    api_payload.setdefault("status", "ok" if api_source or live_items else "placeholder")
    api_payload.setdefault("updated_at", datetime.now().isoformat(timespec="seconds"))
    api_payload.setdefault("message", "Showing historical data for some items." if any(i.get("is_historical") for i in live_items) else "")
    api_payload["source_file"] = api_source

    rss_payload = _normalize_items(rss_data if isinstance(rss_data, dict) else rss_placeholder)

    # [Ahmad Yafi Ar Rizq/5027241066]: RSS Data Lake Fallback
    # If live_rss.json is empty (e.g. consumer just restarted), fall back to the data lake
    if not rss_payload.get("items"):
        rss_lake = BASE_DIR / "../data/pangan/rss/rss_data.json"
        if rss_lake.exists():
            try:
                with open(rss_lake, "rb") as f:
                    f.seek(0, 2)
                    size = f.tell()
                    f.seek(max(0, size - 100000))
                    chunk = f.read().decode('utf-8', errors='ignore')
                seen_links = set()
                fallback_items = []
                for line in reversed(chunk.splitlines()):
                    try:
                        item = json.loads(line)
                        link = item.get("link") or item.get("url") or ""
                        if link and link not in seen_links:
                            seen_links.add(link)
                            fallback_items.append(item)
                    except Exception:
                        continue
                if fallback_items:
                    rss_payload["items"] = fallback_items
            except Exception:
                pass

    rss_payload.setdefault("status", "ok" if rss_source or rss_payload.get("items") else "placeholder")
    rss_payload.setdefault("updated_at", datetime.now().isoformat(timespec="seconds"))
    rss_payload.setdefault("message", "")
    rss_payload["source_file"] = rss_source


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

# [Ahmad Yafi Ar Rizq/5027241066]: Price History Endpoint — province x date matrix for modal
@app.route('/api/price-history')
def get_price_history():
    """Returns historical prices for a commodity grouped by province and date.
    
    Query params:
      - commodity: nama_komoditas to filter by (case-insensitive substring match)
    
    Returns:
      {
        "commodity": "...",
        "dates": ["04/05/2026", ...],            # sorted ascending
        "provinces": [
          { "name": "Nasional", "prices": {"04/05/2026": 45000, ...} },
          ...
        ]
      }
    """
    commodity_q = request.args.get('commodity', '').strip().lower()
    if not commodity_q:
        return jsonify({"error": "commodity param required"}), 400

    # Build date→province→price from the data lake (full scan in chunks)
    lake_file = BASE_DIR / "../data/pangan/api/api_data.json"
    live_file  = BASE_DIR / "data/live_api.json"

    # province → date → price
    matrix: dict = {}
    commodity_display = commodity_q

    def _process_line(line: str):
        nonlocal commodity_display
        try:
            item = json.loads(line)
        except Exception:
            return
        name = item.get("nama_komoditas") or ""
        if commodity_q not in name.lower():
            return
        commodity_display = name  # capture display name

        prov  = item.get("provinsi") or "Nasional"
        harga = item.get("harga")
        if harga is None:
            return

        # Normalise date: prefer tanggal_data, fallback to timestamp
        raw_date = item.get("tanggal_data") or item.get("timestamp") or ""
        try:
            raw_date = raw_date.strip()
            if "T" in raw_date or (len(raw_date) >= 19 and raw_date[10] == " "):
                # ISO-ish timestamp: "2026-05-04T01:25:35..."
                dt = datetime.fromisoformat(raw_date.replace(" ", "T").split("+")[0].split("Z")[0])
                date_key = dt.strftime("%d/%m/%Y")
            elif raw_date[:4].isdigit() and "-" in raw_date:
                # "2026-05-04"
                date_key = datetime.strptime(raw_date[:10], "%Y-%m-%d").strftime("%d/%m/%Y")
            elif len(raw_date) >= 11 and not raw_date[:2].isdigit() is False:
                # "01 May 2026" → parse as strptime
                try:
                    date_key = datetime.strptime(raw_date[:11], "%d %b %Y").strftime("%d/%m/%Y")
                except Exception:
                    date_key = raw_date[:10]
            else:
                date_key = raw_date[:10]
        except Exception:
            date_key = raw_date[:10] if raw_date else "Unknown"

        if prov not in matrix:
            matrix[prov] = {}
        # Keep the latest price for this province+date
        matrix[prov][date_key] = int(harga)

    # --- Read live_api.json first (small, fast) ---
    if live_file.exists():
        try:
            with live_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
                for item in data.get("items", []):
                    line = json.dumps(item)
                    _process_line(line)
        except Exception:
            pass

    # --- Read full data lake line-by-line (NDJSON) ---
    if lake_file.exists():
        try:
            with lake_file.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        _process_line(line)
        except Exception:
            pass

    # Collect and sort all unique dates across all provinces
    all_dates = sorted(
        {d for prov_data in matrix.values() for d in prov_data.keys()},
        key=lambda d: (
            datetime.strptime(d, "%d/%m/%Y")
            if len(d) == 10 and d.count("/") == 2
            else datetime.min
        )
    )

    # Sort provinces: Nasional first, then alphabetical
    def _prov_sort(p):
        return (0, p) if p == "Nasional" else (1, p)

    provinces = [
        {"name": prov, "prices": matrix[prov]}
        for prov in sorted(matrix.keys(), key=_prov_sort)
    ]

    return jsonify({
        "commodity": commodity_display,
        "dates": all_dates,
        "provinces": provinces,
    })


if __name__ == '__main__':
    app.run(debug=True, port=5000)
