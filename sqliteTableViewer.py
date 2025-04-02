import argparse
import sqlite3
import pandas as pd

def preview_sqlite_table(db_path, table_name, rows=5):
    try:
        # SQLite bağlantısını kur
        conn = sqlite3.connect(db_path)
        
        # Veriyi oku
        query = f"SELECT * FROM {table_name} LIMIT {rows}"
        df = pd.read_sql_query(query, conn)

        print(f"\n'{table_name}' tablosundan ilk {rows} satır:\n")
        print(df)

        conn.close()

    except Exception as e:
        print(f"Hata oluştu: {e}")

def main():
    parser = argparse.ArgumentParser(description='SQLite veritabanındaki bir tabloyu önizle.')
    parser.add_argument('--db', type=str, required=True, help='SQLite veritabanı dosyasının yolu (.sqlite/.db)')
    parser.add_argument('--table', type=str, required=True, help='Görüntülenecek tablo adı')
    parser.add_argument('--rows', type=int, default=5, help='Gösterilecek satır sayısı (varsayılan: 5)')
    args = parser.parse_args()

    preview_sqlite_table(args.db, args.table, args.rows)

if __name__ == '__main__':
    main()
