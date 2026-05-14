import os
import json
import logging
import time
import schedule
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, min as spark_min, avg, window, to_timestamp, lower, when, lag, date_format, lit, row_number
from pyspark.sql.window import Window

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("SparkScheduler")

def run_analysis():
    """Execute Spark analysis on current data."""
    log.info("="*60)
    log.info("Starting Spark Analysis...")
    log.info("="*60)

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

    # 2. Paths (Read from HDFS)
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Read from HDFS (latest data)
    HDFS_NAMENODE = "hdfs://localhost:9000"
    HDFS_API_PATH = f"{HDFS_NAMENODE}/data/pangan/api"
    HDFS_RSS_PATH = f"{HDFS_NAMENODE}/data/pangan/rss"
    
    # Output to dashboard (local filesystem)
    OUTPUT_PATH = os.path.join(BASE_DIR, "dashboard", "data", "spark_results.json")
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    # 3. Load Data from HDFS
    def load_json_from_hdfs(hdfs_path):
        """Load JSON data from HDFS directory."""
        try:
            # Read all JSON files from the HDFS directory
            df = spark.read.json(f"{hdfs_path}/*.json")
            if df.count() == 0:
                log.warning(f"No data found in {hdfs_path}")
                return None
            log.info(f"Loaded {df.count()} records from {hdfs_path}")
            return df
        except Exception as e:
            log.warning(f"Could not load data from {hdfs_path}: %s", e)
            return None

    df_api = load_json_from_hdfs(HDFS_API_PATH)
    df_rss = load_json_from_hdfs(HDFS_RSS_PATH)

    if df_api is None:
        log.error("Data API missing in HDFS. Cannot proceed.")
        # Fallback: try to read from local storage
        local_api_path = os.path.join(BASE_DIR, "data", "pangan", "api_staging")
        if os.path.exists(local_api_path):
            try:
                df_api = spark.read.json(f"{local_api_path}/*.json")
                if df_api.count() > 0:
                    log.info(f"Fallback: Loaded {df_api.count()} records from local staging")
                else:
                    log.error("No data in fallback location either")
                    spark.stop()
                    return
            except Exception as e:
                log.error(f"Fallback also failed: %s", e)
                spark.stop()
                return
        else:
            spark.stop()
            return

    # 4. API Data Processing
    log.info("Processing API data...")
    try:
        # Ensure timestamp is properly formatted
        if "timestamp" in df_api.columns:
            df_api = df_api.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
        
        # a. Volatility Ranking (Top 10 most volatile commodities)
        df_volatilitas = df_api.groupBy("komoditas").agg(
            spark_max("harga").alias("harga_max"),
            spark_min("harga").alias("harga_min"),
            avg("harga").alias("harga_avg"),
            col("harga").cast("long").getItem(0).alias("count")
        ).withColumn(
            "volatilitas_persen",
            ((col("harga_max") - col("harga_min")) / col("harga_avg")) * 100
        ).orderBy(col("volatilitas_persen").desc()).limit(10)

        volatilitas_list = [row.asDict() for row in df_volatilitas.collect()]
        log.info(f"Calculated volatility for {len(volatilitas_list)} commodities")

        # b. Trends (Hourly changes)
        df_api_waktu = df_api.withColumn("waktu_jam", date_format(col("timestamp"), "yyyy-MM-dd HH:00:00"))
        df_tren = df_api_waktu.groupBy("komoditas", "waktu_jam").agg(
            avg("harga").alias("harga_rata_rata"),
            col("provinsi").cast("long").getItem(0).alias("count")
        )
        windowSpec = Window.partitionBy("komoditas").orderBy("waktu_jam")
        df_tren = df_tren.withColumn("harga_sebelumnya", lag("harga_rata_rata", 1).over(windowSpec))
        df_tren = df_tren.withColumn(
            "status_tren",
            when(col("harga_sebelumnya").isNull(), "Awal")
            .when(col("harga_rata_rata") > col("harga_sebelumnya"), "Naik")
            .otherwise("Turun")
        )
        
        tren_list = [row.asDict() for row in df_tren.collect()]
        log.info(f"Calculated trends for {len(tren_list)} entries")

        # c. Latest Prices by Commodity
        df_latest = df_api_waktu.withColumn(
            "rn", row_number().over(Window.partitionBy("komoditas").orderBy(col("timestamp").desc()))
        ).filter(col("rn") == 1).drop("rn")
        
        latest_prices = [row.asDict() for row in df_latest.collect()]
        log.info(f"Found latest prices for {len(latest_prices)} commodities")

    except Exception as e:
        log.error("Error processing API data: %s", e)
        volatilitas_list = []
        tren_list = []
        latest_prices = []

    # 5. RSS Data Processing (if available)
    news_summary = []
    if df_rss is not None:
        try:
            log.info("Processing RSS data...")
            rss_records = [row.asDict() for row in df_rss.limit(20).collect()]
            news_summary = rss_records
            log.info(f"Collected {len(rss_records)} news articles")
        except Exception as e:
            log.warning("Error processing RSS data: %s", e)

    # 6. Generate Output
    results = {
        "timestamp": datetime.now().isoformat(),
        "data_sources": {
            "api_records": df_api.count() if df_api else 0,
            "rss_records": df_rss.count() if df_rss else 0
        },
        "analysis": {
            "volatilitas": volatilitas_list,
            "tren": tren_list[:50],  # Limit to 50 latest trends
            "harga_terkini": latest_prices
        },
        "berita_terkini": news_summary[:10]  # Top 10 latest news
    }

    # 7. Write Results
    try:
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        log.info(f"Results written to {OUTPUT_PATH}")
    except Exception as e:
        log.error(f"Failed to write results: %s", e)

    # 8. Stop Spark Session
    spark.stop()
    log.info("Spark Session stopped")
    log.info("="*60)

def schedule_job():
    """Schedule the analysis job to run every 10 minutes."""
    log.info("Spark Scheduler initialized")
    log.info("Analysis will run every 10 minutes")
    
    # Schedule job every 10 minutes
    schedule.every(10).minutes.do(run_analysis)
    
    # Run immediately on startup
    log.info("Running initial analysis...")
    run_analysis()
    
    # Keep scheduler running
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    try:
        schedule_job()
    except KeyboardInterrupt:
        log.info("Scheduler stopped by user")
    except Exception as e:
        log.error(f"Fatal error in scheduler: %s", e)
