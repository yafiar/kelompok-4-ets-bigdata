import os
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, min as spark_min, avg, window, to_timestamp, lower, when, lag, date_format, lit
from pyspark.sql.window import Window

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("SparkAnalysis")

def run_analysis():
    log.info("Starting Spark Analysis...")

    # 1. Initialize Spark Session
    try:
        spark = SparkSession.builder \
            .appName("HargaPangan_Analysis") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        log.info("Spark Session created successfully.")
    except Exception as e:
        log.error("Failed to create Spark Session: %s", e)
        return

    # 2. Paths (Local paths where consumer saves data)
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    API_DATA_PATH = os.path.join(BASE_DIR, "data", "pangan", "api", "api_data.json")
    RSS_DATA_PATH = os.path.join(BASE_DIR, "data", "pangan", "rss", "rss_data.json")
    OUTPUT_PATH = os.path.join(BASE_DIR, "dashboard", "data", "spark_results.json")

    # 3. Load Data
    def load_json_df(path):
        if not os.path.exists(path):
            log.warning("File not found: %s", path)
            return None
        try:
            # Spark can read line-delimited JSON or standard JSON
            # Our consumer writes line-delimited JSON to api_data.json
            return spark.read.json(path)
        except Exception as e:
            log.error("Error loading %s: %s", path, e)
            return None

    df_api = load_json_df(API_DATA_PATH)
    df_rss = load_json_df(RSS_DATA_PATH)

    if df_api is None:
        log.error("Data API missing. Cannot proceed.")
        spark.stop()
        return

    # 4. API Data Processing
    log.info("Processing API data...")
    # Convert timestamp string to actual timestamp
    df_api = df_api.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
    
    # a. Volatility Ranking
    df_volatilitas = df_api.groupBy("komoditas").agg(
        spark_max("harga").alias("harga_max"),
        spark_min("harga").alias("harga_min"),
        avg("harga").alias("harga_avg")
    ).withColumn(
        "volatilitas_persen",
        ((col("harga_max") - col("harga_min")) / col("harga_avg")) * 100
    ).orderBy(col("volatilitas_persen").desc())

    # b. Trends
    df_api_waktu = df_api.withColumn("waktu_jam", date_format(col("timestamp"), "yyyy-MM-dd HH:00:00"))
    df_tren = df_api_waktu.groupBy("komoditas", "waktu_jam").agg(
        avg("harga").alias("harga_rata_rata")
    )
    windowSpec = Window.partitionBy("komoditas").orderBy("waktu_jam")
    df_tren = df_tren.withColumn("harga_sebelumnya", lag("harga_rata_rata", 1).over(windowSpec))
    df_tren = df_tren.withColumn(
        "status_tren",
        when(col("harga_sebelumnya").isNull(), "Awal")
        .when(col("harga_rata_rata") > col("harga_sebelumnya"), "Naik")
        .when(col("harga_rata_rata") < col("harga_sebelumnya"), "Turun")
        .otherwise("Stabil")
    )

    # 5. RSS Correlation
    log.info("Processing RSS data correlation...")
    if df_rss:
        # Normalize RSS timestamp
        if "timestamp" in df_rss.columns:
            df_rss = df_rss.withColumn("ts", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
        else:
            df_rss = df_rss.withColumn("ts", to_timestamp(col("published"), "yyyy-MM-dd'T'HH:mm:ss"))
        
        df_rss_waktu = df_rss.withColumn("waktu_jam", date_format(col("ts"), "yyyy-MM-dd HH:00:00"))
        df_rss_lower = df_rss_waktu.withColumn("judul_lower", lower(col("judul")))

        list_keywords = ["beras", "cabai", "bawang", "daging", "telur", "ayam", "minyak", "gula"]
        korelasi_list = []
        
        for k in list_keywords:
            df_k = df_rss_lower.filter(col("judul_lower").contains(k)) \
                               .groupBy("waktu_jam") \
                               .count() \
                               .withColumnRenamed("count", "jumlah_berita") \
                               .withColumn("komoditas", lit(k))
            korelasi_list.append(df_k)
        
        if korelasi_list:
            from functools import reduce
            df_korelasi_all = reduce(lambda x, y: x.union(y), korelasi_list)
            df_final_korelasi = df_tren.join(df_korelasi_all, on=["komoditas", "waktu_jam"], how="left").fillna(0)
        else:
            df_final_korelasi = df_tren.withColumn("jumlah_berita", lit(0))
    else:
        df_final_korelasi = df_tren.withColumn("jumlah_berita", lit(0))

    # 6. Export Results
    log.info("Exporting results to %s", OUTPUT_PATH)
    try:
        # Convert Spark to Pandas to JSON
        # Note: limit to avoid huge JSON files
        result = {
            "ranking": df_volatilitas.limit(20).toPandas().to_dict(orient="records"),
            "trends": df_tren.orderBy("waktu_jam", ascending=False).limit(100).toPandas().to_dict(orient="records"),
            "correlation": df_final_korelasi.orderBy("waktu_jam", ascending=False).limit(100).toPandas().to_dict(orient="records"),
            "updated_at": datetime.now().isoformat(),
            "status": "success",
            "interpretation": "Analisis volatilitas dan tren harga berhasil diperbarui dari data API terbaru."
        }
        
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=4, ensure_ascii=False)
        log.info("Analysis completed and exported.")
    except Exception as e:
        log.error("Export failed: %s", e)

    spark.stop()

if __name__ == "__main__":
    run_analysis()
