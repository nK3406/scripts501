import pandas as pd
import argparse
import sqlite3
import sys

# Argparse ile dışarıdan dosya adı alma
parser = argparse.ArgumentParser(description="SWIRTS veri setini yükler.")
parser.add_argument("data_path", type=str, help="SQLite veri tabanı dosyasının yolu (örn: swirts.sqlite)")
args = parser.parse_args()

# SQLite dosyasına bağlan ve collisions tablosunu oku
try:
    conn = sqlite3.connect(args.data_path)
    df = pd.read_sql_query("SELECT * FROM collisions", conn)
    conn.close()
    print("Veri başarıyla yüklendi!\n")
except Exception as e:
    print("Veri yüklenirken hata oluştu:", e)
    sys.exit(1)

# Veri setinin ilk 5 satırına bakalım
print("İlk 5 satır:")
print(df.head())

# Sütun isimlerine bakalım
print("\nSütunlar:")
print(df.columns)
