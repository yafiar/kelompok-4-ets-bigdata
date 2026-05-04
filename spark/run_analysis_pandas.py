import os
import json
import pandas as pd
from datetime import datetime

def run_analysis_pandas():
    print("Starting Analysis (Pandas Fallback)...")

    # Paths
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    API_DATA_PATH = os.path.join(BASE_DIR, "data", "pangan", "api", "api_data.json")
    RSS_DATA_PATH = os.path.join(BASE_DIR, "data", "pangan", "rss", "rss_data.json")
    OUTPUT_PATH = os.path.join(BASE_DIR, "dashboard", "data", "spark_results.json")

    # 1. Load API Data
    if not os.path.exists(API_DATA_PATH):
        print(f"File not found: {API_DATA_PATH}")
        return

    # Read line-delimited JSON
    try:
        df_api = pd.read_json(API_DATA_PATH, lines=True)
    except Exception as e:
        print(f"Error reading API data: {e}")
        return

    if df_api.empty:
        print("API data is empty.")
        return

    # Convert timestamp
    df_api['timestamp'] = pd.to_datetime(df_api['timestamp'])

    # 2. Volatility Calculation
    volatilitas = df_api.groupby('komoditas')['harga'].agg(['max', 'min', 'mean']).reset_index()
    volatilitas.columns = ['komoditas', 'harga_max', 'harga_min', 'harga_avg']
    volatilitas['volatilitas_persen'] = ((volatilitas['harga_max'] - volatilitas['harga_min']) / volatilitas['harga_avg']) * 100
    volatilitas = volatilitas.sort_values('volatilitas_persen', ascending=False)

    # 3. Trend Calculation
    df_api['waktu_jam'] = df_api['timestamp'].dt.strftime('%Y-%m-%d %H:00:00')
    tren = df_api.groupby(['komoditas', 'waktu_jam'])['harga'].mean().reset_index()
    tren.columns = ['komoditas', 'waktu_jam', 'harga_rata_rata']
    
    tren = tren.sort_values(['komoditas', 'waktu_jam'])
    tren['harga_sebelumnya'] = tren.groupby('komoditas')['harga_rata_rata'].shift(1)
    
    def get_status(row):
        if pd.isna(row['harga_sebelumnya']): return "Awal"
        if row['harga_rata_rata'] > row['harga_sebelumnya']: return "Naik"
        if row['harga_rata_rata'] < row['harga_sebelumnya']: return "Turun"
        return "Stabil"
    
    tren['status_tren'] = tren.apply(get_status, axis=1)

    # 4. RSS Correlation (Simple count)
    korelasi = tren.copy()
    korelasi['jumlah_berita'] = 0
    
    if os.path.exists(RSS_DATA_PATH):
        try:
            df_rss = pd.read_json(RSS_DATA_PATH, lines=True)
            if not df_rss.empty:
                df_rss['ts'] = pd.to_datetime(df_rss['published'], errors='coerce').fillna(pd.to_datetime(df_rss['timestamp'], errors='coerce'))
                df_rss['waktu_jam'] = df_rss['ts'].dt.strftime('%Y-%m-%d %H:00:00')
                
                list_keywords = ["beras", "cabai", "bawang", "daging", "telur", "ayam", "minyak", "gula"]
                for k in list_keywords:
                    news_count = df_rss[df_rss['judul'].str.contains(k, case=False, na=False)].groupby('waktu_jam').size().reset_index(name='jumlah_berita')
                    # Merge with korelasi for this commodity
                    idx = (korelasi['komoditas'].str.contains(k, case=False))
                    # This is a bit simplified, but works for the dashboard
                    for _, row in news_count.iterrows():
                        match = (korelasi['waktu_jam'] == row['waktu_jam']) & idx
                        korelasi.loc[match, 'jumlah_berita'] = row['jumlah_berita']
        except Exception as e:
            print(f"RSS correlation failed: {e}")

    # 5. Export
    result = {
        "ranking": volatilitas.head(20).to_dict(orient="records"),
        "trends": tren.tail(100).to_dict(orient="records"),
        "correlation": korelasi.tail(100).to_dict(orient="records"),
        "updated_at": datetime.now().isoformat(),
        "status": "ok",
        "interpretation": "Analisis volatilitas dan tren harga berhasil diperbarui menggunakan Pandas (Spark Fallback)."
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4, ensure_ascii=False)
    
    print(f"Analysis completed and exported to {OUTPUT_PATH}")

if __name__ == "__main__":
    run_analysis_pandas()
