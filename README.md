# ETS BigData (C) Kelompok 4 - HargaPangan

## Anggota Kelompok
| Anggota                        | NRP        | Kontribusi & Tanggung Jawab                                                                                                                                                      |
| ------------------------------ | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Maritza Adelia Sucipto         | 5027241111 | Setup Infrastruktur: Docker (Hadoop/HDFS & Kafka), konfigurasi network/port, troubleshooting awal.                                                                              |
| Adinda Cahya Pramesti          | 5027241117 | Data Ingestion (API): `producer_api.py` untuk mengambil data harga komoditas dan mengirim ke Kafka topic `pangan-api`.                                                         |
| Alnico Virendra Kitaro Diaz    | 5027241081 | Data Ingestion (RSS): `producer_rss.py` untuk berita ekonomi dan `consumer_to_hdfs.py` untuk sinkronisasi data dari Kafka ke HDFS.                                             |
| Afriza Tristan Calendra Rajasa | 5027241104 | Data Processing: `spark/analysis.ipynb` untuk analisis volatilitas, tren harga, dan korelasi berita menggunakan Spark.                                                          |
| Ahmad Yafi Ar Rizq             | 5027241066 | Dashboard & Visualization: Flask app untuk menampilkan hasil analisis Spark, harga live, dan berita ekonomi secara real-time.                                                  |

---

## Deskripsi Proyek
**HargaPangan** adalah sistem monitoring harga komoditas bahan pokok secara real-time. Sistem ini dirancang untuk tim riset (seperti Bulog) sebagai alat bantu *early warning* dalam memantau fluktuasi harga bahan pokok dan menghubungkannya dengan berita ekonomi terkini.

### Justifikasi Pemilihan Topik
Topik ini dipilih karena harga bahan pokok memiliki dampak langsung terhadap stabilitas ekonomi masyarakat. Dengan menggabungkan data harga (kuantitatif) dan berita ekonomi (kualitatif), sistem ini dapat memberikan wawasan lebih mendalam tentang *mengapa* harga suatu komoditas bergejolak, bukan sekadar *apa* yang berubah.

### Pertanyaan Bisnis Utama
> "Komoditas mana yang paling bergejolak harganya hari ini, dan apakah ada berita ekonomi yang menjelaskan penyebabnya?"

---

## Arsitektur Sistem
Sistem ini menggunakan stack Big Data modern untuk menangani aliran data dari ingestion hingga visualisasi.


---

## Cara Menjalankan Sistem

### 1. Prasyarat
- Docker & Docker Compose
- Python 3.x
- Library Python: `kafka-python`, `feedparser`, `requests`, `flask`, `pyspark`

### 2. Setup Infrastruktur (Hadoop & Kafka)
Jalankan kontainer Docker untuk Hadoop dan Kafka:
```bash
docker-compose -f docker-compose-hadoop.yml up -d
docker-compose -f docker-compose-kafka.yml up -d
```

### 3. Inisialisasi Sistem (Wajib bagi Anggota 1)
Setelah kontainer jalan, jalankan perintah ini satu kali untuk membuat topic dan folder:
```bash
# Membuat Kafka Topics
docker exec -it kafka kafka-topics --create --topic pangan-api --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic pangan-rss --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Membuat Folder HDFS
docker exec -it namenode hdfs dfs -mkdir -p /data/pangan/api
docker exec -it namenode hdfs dfs -mkdir -p /data/pangan/rss
```

Untuk detail lebih teknis mengenai infrastruktur, silakan cek [INFRASTRUCTURE.md](./INFRASTRUCTURE.md).

### 4. Menjalankan Ingestion (Kafka Producers)
Buka terminal baru dan jalankan producer:
```bash
python kafka/producer_api.py
python kafka/producer_rss.py
```

### 4. Menjalankan Consumer (HDFS Sync)
Simpan data dari Kafka ke HDFS:
```bash
python kafka/consumer_to_hdfs.py
```

### 5. Analisis Data (Spark)
Buka dan jalankan semua cell di `spark/analysis.ipynb` untuk memproses data dari HDFS.

### 6. Menjalankan Dashboard
Jalankan aplikasi Flask untuk melihat visualisasi:
```bash
python dashboard/app.py
```
Akses di: `http://localhost:5000`

---

## Screenshots
*(Akan ditambahkan setelah sistem berjalan sepenuhnya)*
- **HDFS Web UI**: 

- **Kafka Consumer Output**: Log terminal dari consumer.
- **Dashboard**: Tampilan visualisasi web.

---

## Tantangan & Solusi
*(Akan diperbarui selama proses pengerjaan)*
---

## Struktur Repository
```
.
├── docker-compose-hadoop.yml    ← Konfigurasi HDFS
├── docker-compose-kafka.yml     ← Konfigurasi Kafka
├── kafka/                       ← Layer Ingestion
│   ├── producer_api.py
│   ├── producer_rss.py
│   └── consumer_to_hdfs.py
├── spark/                       ← Layer Processing
│   └── analysis.ipynb
└── dashboard/                   ← Layer Visualisasi
    ├── app.py
    └── templates/index.html
```
