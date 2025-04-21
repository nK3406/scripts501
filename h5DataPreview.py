import argparse
import pandas as pd

def preview_h5(h5_file_path, key='t', rows=5):
    try:
        # Veriyi oku
        df = pd.read_hdf(h5_file_path, key=key)
        print(f"\n'{key}' anahtarındaki ilk {rows} satır:\n")
        print(df.head(rows))
    except Exception as e:
        print(f"Hata oluştu: {e}")

def main():
    parser = argparse.ArgumentParser(description='HDF5 (.h5) dosyasındaki veriyi önizle (head).')
    parser.add_argument('file', type=str, help='HDF5 dosyasının yolu (.h5 uzantılı)')
    parser.add_argument('--key', type=str, default='t', help='Okunacak dataset anahtarı (varsayılan: t)')
    parser.add_argument('--rows', type=int, default=5, help='Kaç satır gösterilsin (varsayılan: 5)')
    args = parser.parse_args()

    preview_h5(args.file, key=args.key, rows=args.rows)

if __name__ == '__main__':
    main()
