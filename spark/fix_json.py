import json
import os

# Data dummy yang tadi kita buat
data = {
    "volatilitas": [
        {"komoditas": "cabai", "harga_max": 45000, "harga_min": 40000, "harga_avg": 42333.3, "volatilitas_persen": 11.8},
        {"komoditas": "beras", "harga_max": 15000, "harga_min": 14500, "harga_avg": 14766.6, "volatilitas_persen": 3.3}
    ],
    "tren_harga": [
        {"komoditas": "beras", "waktu_jam": "2026-04-25 10:00:00", "harga_rata_rata": 14500.0, "status_tren": "Awal"},
        {"komoditas": "beras", "waktu_jam": "2026-04-25 11:00:00", "harga_rata_rata": 15000.0, "status_tren": "Naik"},
        {"komoditas": "cabai", "waktu_jam": "2026-04-25 10:00:00", "harga_rata_rata": 45000.0, "status_tren": "Awal"}
    ],
    "korelasi_berita": [
        {"komoditas": "beras", "waktu_jam": "2026-04-25 10:00:00", "jumlah_berita": 1, "status_tren": "Awal"},
        {"komoditas": "cabai", "waktu_jam": "2026-04-25 11:00:00", "jumlah_berita": 1, "status_tren": "Turun"}
    ]
}

# Simpan ke folder yang benar
path = "dashboard/data/spark_results.json"
os.makedirs("dashboard/data", exist_ok=True)

with open(path, "w") as f:
    json.dump(data, f, indent=4)

print(f"✅ BERHASIL! File terbuat di: {os.path.abspath(path)}")