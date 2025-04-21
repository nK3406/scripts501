import argparse
import pandas as pd

def preview_parquet(file_path, rows=5):
    try:
        df = pd.read_parquet(file_path)
        print(f"\n'{file_path}' dosyasından ilk {rows} satır:\n")
        print(df.head(rows))
    except Exception as e:
        print(f"Hata oluştu: {e}")

def main():
    parser = argparse.ArgumentParser(description='Parquet dosyasının ilk birkaç satırını önizle.')
    parser.add_argument('file', type=str, help='Parquet dosyasının yolu')
    parser.add_argument('--rows', type=int, default=5, help='Gösterilecek satır sayısı (varsayılan: 5)')
    args = parser.parse_args()

    preview_parquet(args.file, args.rows)

if __name__ == '__main__':
    main()
