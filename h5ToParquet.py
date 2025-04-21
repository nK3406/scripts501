import argparse
import pandas as pd
import os

def process_and_split_h5(h5_path, key='t', output_prefix='output'):
    try:
        # HDF5 dosyasını oku
        df = pd.read_hdf(h5_path, key=key)

        # Zaman bilgisini indeks olarak kullandığını varsayarak:
        df = df.reset_index()  # index -> ayrı 'timestamp' sütunu

        # Yeni index sütunu oluştur (1'den başlayarak)
        df.insert(0, 'index', range(1, len(df) + 1))

        # Satır sayısına göre veriyi ikiye böl
        total_rows = len(df)
        midpoint = total_rows // 2

        df1 = df.iloc[:midpoint].copy()
        df2 = df.iloc[midpoint:].copy()

        # df2'deki index'leri devam ettir
        df2['index'] = range(midpoint + 1, total_rows + 1)

        # Çıktı dosya isimlerini oluştur
        parquet_file1 = f"{output_prefix}_part1.parquet"
        parquet_file2 = f"{output_prefix}_part2.parquet"

        # Parquet olarak kaydet
        df1.to_parquet(parquet_file1, index=False)
        df2.to_parquet(parquet_file2, index=False)

        print(f"\n✅ Başarılı!")
        print(f" - {parquet_file1} ({len(df1)} satır)")
        print(f" - {parquet_file2} ({len(df2)} satır)\n")

    except Exception as e:
        print(f"Hata oluştu: {e}")

def main():
    parser = argparse.ArgumentParser(description='HDF5 dosyasını iki parçalı Parquet dosyalarına dönüştür.')
    parser.add_argument('--file', type=str, required=True, help='Girdi HDF5 dosyası (.h5)')
    parser.add_argument('--key', type=str, default='t', help='HDF5 içindeki dataset anahtarı (varsayılan: t)')
    parser.add_argument('--output_prefix', type=str, default='output', help='Parquet dosya adı ön eki (varsayılan: output)')
    args = parser.parse_args()

    process_and_split_h5(args.file, args.key, args.output_prefix)

if __name__ == '__main__':
    main()
