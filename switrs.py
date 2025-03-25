import pandas as pd
import argparse

# Argparse ile dışarıdan dosya adı alma
parser = argparse.ArgumentParser(description="SWIRTS veri setini yükler.")
parser.add_argument("data_path", type=str, help="Veri dosyasının yolu (örn: swirts.csv)")
args = parser.parse_args()

# Dosyayı oku
try:
    df = pd.read_csv(args.data_path)
    print("Veri başarıyla yüklendi!\n")
except Exception as e:
    print("Veri yüklenirken hata oluştu:", e)
    exit()

# Veri setinin ilk 5 satırına bakalım
print("İlk 5 satır:")
print(df.head())

# Sütun isimlerine bakalım
print("\nSütunlar:")
print(df.columns)
