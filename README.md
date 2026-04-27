# ETS BigData (C) Kelompok 4 - HargaPangan

| Anggota                        | NRP        | Tanggung Jawab                                                                                                                                                                   |
| ------------------------------ | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Maritza Adelia Sucipto         | 5027241111 | Setup Docker **Hadoop (HDFS)** & **Kafka (Zookeeper + Broker)**, buat topic `pangan-api` dan `pangan-rss`, konfigurasi network/port, testing awal, troubleshooting infrastruktur |
| Adinda Cahya Pramesti          | 5027241117 | `producer_api.py` — ambil data harga komoditas, bentuk JSON konsisten, kirim ke `pangan-api`, logging & interval polling                                                         |
| Alnico Virendra Kitaro Diaz    | 5027241081 | `producer_rss.py` (feedparser, anti duplikat) + `consumer_to_hdfs.py` baca `pangan-api` & `pangan-rss`, buffer 2–5 menit, simpan ke HDFS dan salinan lokal untuk dashboard       |
| Afriza Tristan Calendra Rajasa | 5027241104 | `spark/analysis.ipynb` — baca dari HDFS, 3 analisis wajib (volatilitas, tren harga, korelasi berita), simpan hasil ke HDFS & `spark_results.json`, narasi interpretasi           |
| Ahmad Yafi Ar Rizq             | 5027241066 | `dashboard/app.py` + `index.html` — 3 panel (hasil Spark, live harga, berita), indikator naik/turun, auto-refresh, opsional grafik Chart.js                                      |
