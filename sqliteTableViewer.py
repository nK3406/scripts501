import argparse
import sqlite3
import pandas as pd

def list_columns(db_path, table_name):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = cursor.fetchall()
        if columns_info:
            print(f"\n'{table_name}' tablosunun sütunları:")
            for col in columns_info:
                print(f" - {col[1]}")  # col[1] sütun adını temsil eder
        else:
            print(f"\nTablo '{table_name}' bulunamadı veya sütun bilgisi alınamadı.")
        conn.close()
    except Exception as e:
        print(f"Hata oluştu: {e}")

def preview_table(db_path, table_name, rows):
    try:
        conn = sqlite3.connect(db_path)
        query = f"SELECT * FROM {table_name} LIMIT {rows}"
        df = pd.read_sql_query(query, conn)
        print(f"\n'{table_name}' tablosundan ilk {rows} satır:\n")
        print(df)
        conn.close()
    except Exception as e:
        print(f"Hata oluştu: {e}")

def main():
    parser = argparse.ArgumentParser(description='SQLite veritabanındaki bir tabloyu önizle veya sütunlarını listele.')
    parser.add_argument('--db', type=str, required=True, help='SQLite veritabanı dosyasının yolu (.sqlite/.db)')
    parser.add_argument('--table', type=str, required=True, help='Görüntülenecek tablo adı')
    parser.add_argument('--rows', type=int, default=5, help='Gösterilecek satır sayısı (varsayılan: 5)')
    parser.add_argument('--columns', action='store_true', help='Sadece sütun adlarını listele')

    args = parser.parse_args()

    if args.columns:
        list_columns(args.db, args.table)
    else:
        preview_table(args.db, args.table, args.rows)

if __name__ == '__main__':
    main()
