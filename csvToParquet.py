
import pandas as pd
import os


def main():
   try:
       # CSV dosya yolunu belirt
       csv_path = '/mnt/data/joined_output/part-00000-72f44f60-9e62-4a58-85d3-f7ad93b08285-c000.csv'
       parquet_path = '/mnt/data/joined_output/veriler-konum-bilgili.parquet'
      
       print(f"CSV dosyası yükleniyor: {csv_path}")
      
       # CSV dosyasının varlığını kontrol et
       if not os.path.exists(csv_path):
           raise FileNotFoundError(f"CSV dosyası bulunamadı: {csv_path}")
      
       # CSV dosyasını oku
       df = pd.read_csv(csv_path, engine='python')
       print(f"CSV dosyası başarıyla yüklendi. Satır sayısı: {len(df)}")
      
       # Parquet formatına dönüştür ve kaydet
       print(f"Parquet formatına dönüştürülüyor: {parquet_path}")
       df.to_parquet(parquet_path, index=False)
       print("Dönüştürme tamamlandı.")
      
       # Dosya boyutlarını karşılaştır
       csv_size = os.path.getsize(csv_path) / (1024 * 1024)  # MB
       parquet_size = os.path.getsize(parquet_path) / (1024 * 1024)  # MB
      
       print(f"\nDosya boyutları:")
       print(f"CSV: {csv_size:.2f} MB")
       print(f"Parquet: {parquet_size:.2f} MB")
       print(f"Boyut azalması: {((csv_size - parquet_size) / csv_size * 100):.2f}%")
      
   except Exception as e:
       print(f"\nHata oluştu: {str(e)}")
       print("\nHata detayı:")
       import traceback
       traceback.print_exc()


if __name__ == "__main__":
   main()
