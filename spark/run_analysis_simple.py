import json
import os
from datetime import datetime

def run_analysis_simple():
    print("Starting Analysis (Zero-Dependency Fallback)...")

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    API_DATA_PATH = os.path.join(BASE_DIR, "data", "pangan", "api", "api_data.json")
    RSS_DATA_PATH = os.path.join(BASE_DIR, "data", "pangan", "rss", "rss_data.json")
    OUTPUT_PATH = os.path.join(BASE_DIR, "dashboard", "data", "spark_results.json")

    if not os.path.exists(API_DATA_PATH):
        print(f"Data not found at {API_DATA_PATH}")
        return

    # 1. Load Data
    data_by_kom = {}
    with open(API_DATA_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                kom = record.get('komoditas')
                if not kom: continue
                if kom not in data_by_kom:
                    data_by_kom[kom] = []
                data_by_kom[kom].append(record)
            except: continue

    # 2. Calculate Volatility and Trends
    ranking = []
    trends = []
    
    for kom, records in data_by_kom.items():
        # Sort by timestamp
        records.sort(key=lambda x: x.get('timestamp', ''))
        
        prices = [r.get('harga', 0) for r in records if r.get('harga', 0) > 0]
        if not prices: continue
        
        h_max = max(prices)
        h_min = min(prices)
        h_avg = sum(prices) / len(prices)
        vol = ((h_max - h_min) / h_avg * 100) if h_avg > 0 else 0
        
        ranking.append({
            "komoditas": records[0].get('nama_komoditas', kom),
            "harga_max": h_max,
            "harga_min": h_min,
            "harga_avg": round(h_avg, 2),
            "volatilitas": round(vol, 2),
            "volatility": round(vol, 2)
        })
        
        # Simple trend (latest vs previous)
        if len(records) > 1:
            latest = records[-1]
            prev = records[-2]
            status = "Naik" if latest['harga'] > prev['harga'] else "Turun" if latest['harga'] < prev['harga'] else "Stabil"
            trends.append({
                "komoditas": latest.get('nama_komoditas', kom),
                "waktu_jam": latest.get('timestamp', '').replace('T', ' '),
                "harga_rata_rata": latest['harga'],
                "status_tren": status
            })
        else:
            trends.append({
                "komoditas": records[0].get('nama_komoditas', kom),
                "waktu_jam": records[0].get('timestamp', '').replace('T', ' '),
                "harga_rata_rata": records[0]['harga'],
                "status_tren": "Awal"
            })

    # Sort ranking
    ranking.sort(key=lambda x: x['volatilitas'], reverse=True)

    # 3. Final Export
    result = {
        "volatility_ranking": ranking[:10],
        "volatilitas": ranking[:10],
        "ranking": ranking[:10],
        "trends": trends[-50:],
        "correlation": trends[-50:],
        "updated_at": datetime.now().isoformat(),
        "status": "ok",
        "interpretation": "Analisis volatilitas harga berhasil diproses. Komoditas dengan fluktuasi tertinggi dihitung dari pergerakan harga pasar terbaru."
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4, ensure_ascii=False)
    
    print(f"Analysis successful. Data exported to {OUTPUT_PATH}")

if __name__ == "__main__":
    run_analysis_simple()
