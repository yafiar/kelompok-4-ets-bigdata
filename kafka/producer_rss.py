import feedparser
from kafka import KafkaProducer
import json
import time

KAFKA_SERVER = "localhost:9092"
TOPIC = "pangan-rss"

def run_producer():
    print("🔥 FILE BARU SEDANG DIJALANKAN 🔥")

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    urls = [
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best"
    ]

    seen = set()

    while True:
        print("\n🔄 Ambil data RSS...")

        for url in urls:
            feed = feedparser.parse(url)
            print(f"📡 Sumber: {url} | Jumlah berita: {len(feed.entries)}")

            for entry in feed.entries:
                if entry.link in seen:
                    continue

                data = {
                    "judul": entry.title,
                    "link": entry.link,
                    "published": entry.published
                }

                # kirim ke Kafka
                producer.send(TOPIC, value=data)

                # print biar kelihatan jalan
                print("✅ Kirim RSS:", data["judul"])

                seen.add(entry.link)

        print("⏳ Menunggu 10 detik...\n")
        time.sleep(10)   # 🔥 dipercepat untuk demo

if __name__ == "__main__":
    run_producer()