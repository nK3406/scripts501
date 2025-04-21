import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    parser = argparse.ArgumentParser(description="Extract collision times from SWITRS using Spark")
    parser.add_argument("--latest_csv", type=str, required=True, help="Path to latest.csv file")
    parser.add_argument("--sqlite", type=str, required=True, help="Path to SWITRS SQLite file")
    parser.add_argument("--table", type=str, default="collisions", help="SQLite table name")
    parser.add_argument("--save", type=str, help="Optional output path for CSV export")

    args = parser.parse_args()

    # Spark Session
    spark = SparkSession.builder \
        .appName("Extract Collision Times") \
        .config("spark.driver.extraClassPath", "/home/orhankocak_0233/501-main/sqlite-jdbc-3.49.1.0.jar") \
        .config("spark.jars", "/home/orhankocak_0233/501-main/sqlite-jdbc-3.49.1.0.jar") \
        .getOrCreate()

    # Load latest.csv
    print("latest.csv dosyası yükleniyor...")
    latest_df = spark.read.option("header", True).csv(args.latest_csv)

    if "case_id" not in latest_df.columns:
        raise ValueError("latest.csv dosyasında 'case_id' sütunu bulunamadı.")

    latest_case_ids = latest_df.select("ID","case_id").dropna().distinct()

    # Load SQLite table
    print("SWITRS SQLite dosyası yükleniyor...")
    collisions_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:sqlite:{args.sqlite}") \
        .option("dbtable", args.table) \
        .option("driver", "org.sqlite.JDBC") \
        .load()

    # Select necessary columns
    collisions_subset = collisions_df.select("case_id", "collision_date", "collision_time")

    # Join with latest case_ids
    print("Join işlemi yapılıyor...")
    joined_df = latest_case_ids.join(collisions_subset, on="case_id", how="inner")

    print(f"Eşleşen kayıt sayısı: {joined_df.count()}")
    joined_df.show(10, truncate=False)

    if args.save:
        print(f"CSV olarak dışa aktarılıyor: {args.save}")
        joined_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(args.save)


    spark.stop()

if __name__ == "__main__":
    main()
