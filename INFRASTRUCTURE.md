# Panduan Infrastruktur & Orkestrasi
**PIC: Maritza Adelia Sucipto (5027241111)**

Dokumen ini berisi panduan untuk menyiapkan infrastruktur dasar (Hadoop & Kafka) menggunakan Docker.

## 🚀 Langkah-langkah Persiapan

### 1. Menjalankan Kontainer
Pastikan Anda berada di root folder proyek, lalu jalankan:

```bash
# Menjalankan Hadoop (HDFS)
docker-compose -f docker-compose-hadoop.yml up -d

# Menjalankan Kafka (Zookeeper + Broker)
docker-compose -f docker-compose-kafka.yml up -d
```

### 2. Membuat Kafka Topics
Setelah Kafka berjalan, buat dua topic utama yang dibutuhkan tim:

```bash
# Membuat topic pangan-api
docker exec -it kafka kafka-topics --create --topic pangan-api --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Membuat topic pangan-rss
docker exec -it kafka kafka-topics --create --topic pangan-rss --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

## 🔍 Verifikasi & Testing
Gunakan perintah ini untuk memastikan semua layanan berjalan dengan benar.

### A. Testing Kafka
Cek apakah topic sudah terdaftar:
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

### B. Testing HDFS
Cek isi root direktori HDFS:
```bash
docker exec -it namenode hdfs dfs -ls /
```
*(Opsional)* Buat folder data awal:
```bash
docker exec -it namenode hdfs dfs -mkdir -p /data/pangan/api
docker exec -it namenode hdfs dfs -mkdir -p /data/pangan/rss
```

### C. Web UI Access
Buka di browser Anda:
- **HDFS Namenode**: [http://localhost:9870](http://localhost:9870) (Cek kapasitas penyimpanan & Datanode)
- **HDFS Datanode**: [http://localhost:9864](http://localhost:9864)

---

## 🛠️ Troubleshooting Common Issues

1. **Error: `Connection refused` pada Kafka**
   - Pastikan kontainer `kafka` sudah status `running` (`docker ps`).
   - Cek log dengan `docker logs kafka`.
2. **Datanode tidak muncul di Web UI**
   - Biasanya karena masalah `cluster ID` tidak cocok. Solusi tercepat: Hapus volume dan restart.
   ```bash
   docker-compose -f docker-compose-hadoop.yml down -v
   docker-compose -f docker-compose-hadoop.yml up -d
   ```
3. **Port conflict**
   - Pastikan port 9870, 9000, 9092, dan 2181 tidak sedang digunakan oleh aplikasi lain di komputer Anda.

---

## Penjelasan File Konfigurasi
- **`docker-compose-hadoop.yml`**: Mengatur `namenode` dan `datanode`. Menggunakan `bigdata_network` untuk isolasi.
- **`hadoop.env`**: Berisi variabel lingkungan seperti `CORE_CONF_fs_defaultFS` agar Hadoop tahu di mana filesystem utamanya.
- **`docker-compose-kafka.yml`**: Mengatur `zookeeper` dan `kafka broker`. Menggunakan `KAFKA_CFG_ADVERTISED_LISTENERS` agar producer di luar Docker bisa mengirim data.
