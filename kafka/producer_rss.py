import json
import logging
import time

import feedparser
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC = "pangan-rss"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("producer_rss")


def _entry_text(entry, *keys, default=""):
    for key in keys:
        if hasattr(entry, key):
            value = getattr(entry, key)
            if value:
                return value
    if hasattr(entry, "get"):
        for key in keys:
            value = entry.get(key)
            if value:
                return value
    return default

def run_producer():
    log.info("RSS producer started")

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )

    urls = [
        "https://feeds.bbci.co.uk/news/business/rss.xml",
        "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
        "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best",
    ]

    seen = set()

    while True:
        log.info("Fetching RSS feeds")

        for url in urls:
            try:
                feed = feedparser.parse(url)
            except Exception as exc:
                log.warning("Failed to parse RSS feed %s: %s", url, exc)
                continue

            if getattr(feed, "bozo", False) and getattr(feed, "bozo_exception", None):
                log.warning("RSS feed parse warning for %s: %s", url, feed.bozo_exception)

            log.info("Source: %s | Entries: %s", url, len(feed.entries))

            for entry in feed.entries:
                link = _entry_text(entry, "link", default="")
                title = _entry_text(entry, "title", default="Tanpa judul")
                published = _entry_text(entry, "published", "updated", "published_parsed", default=time.strftime("%Y-%m-%dT%H:%M:%S"))

                if not link:
                    continue

                if link in seen:
                    continue

                data = {
                    "judul": title,
                    "title": title,
                    "link": link,
                    "published": published,
                    "source": url,
                    "summary": _entry_text(entry, "summary", "description", default=""),
                    "description": _entry_text(entry, "description", "summary", default=""),
                }

                producer.send(TOPIC, value=data)

                log.info("Sent RSS: %s", data["judul"])

                seen.add(entry.link)

        log.info("Waiting 10 seconds before next fetch")
        time.sleep(10)

if __name__ == "__main__":
    run_producer()