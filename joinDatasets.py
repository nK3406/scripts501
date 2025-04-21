import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, radians, sin, cos, sqrt, asin, when
from pyspark.sql import functions as F

def haversine_expr(lat1, lon1, lat2, lon2):
    """
    Spark SQL için Haversine mesafe hesaplama ifadesi (metre cinsinden)
    """
    return 6371000 * 2 * asin(sqrt(
        sin((radians(lat2) - radians(lat1)) / 2) ** 2 +
        cos(radians(lat1)) * cos(radians(lat2)) *
        sin((radians(lon2) - radians(lon1)) / 2) ** 2
    ))

def convert_direction(direction):
    """
    Kısa yön harflerini tam yön isimlerine dönüştüren fonksiyon (N -> north, S -> south, vb.)
    """
    return when(direction == "N", "north") \
           .when(direction == "S", "south") \
           .when(direction == "E", "east") \
           .when(direction == "W", "west") \
           .otherwise(None)

def get_opposite_direction(direction):
    """
    Yönün tersini döndüren fonksiyon (north -> south, east -> west, vb.)
    """
    return when(direction == "north", "south") \
           .when(direction == "south", "north") \
           .when(direction == "east", "west") \
           .when(direction == "west", "east") \
           .otherwise(None)

def main():
    parser = argparse.ArgumentParser(description="Join SWITRS (SQLite) and LargeST (CSV) datasets using Spark")
    parser.add_argument("--sqlite", type=str, default="/home/orhankocak_0233/501-main/swit", help="Path to SWITRS SQLite file")
    parser.add_argument("--table", type=str, default="collisions", help="Table name inside SQLite file")
    parser.add_argument("--meta_csv", type=str, default="/home/orhankocak_0233/501-main/ca_meta.csv", help="Path to LargeST metadata CSV file")
    parser.add_argument("--output", type=str, help="Output path for joined data (CSV format)")
    parser.add_argument("--distance", type=int, default=500, help="Maximum distance in meters for spatial join")
    parser.add_argument("--preview", action="store_true", help="Flag to preview only the first 100 rows")


    args = parser.parse_args()

    # Spark session
    spark = SparkSession.builder \
        .appName("Join SWITRS and LargeST metadata") \
        .config("spark.driver.extraClassPath", "/home/orhankocak_0233/501-main/sqlite-jdbc-3.49.1.0.jar") \
        .config("spark.jars", "/home/orhankocak_0233/501-main/sqlite-jdbc-3.49.1.0.jar") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    # Read SWITRS collisions from SQLite (only first 100 rows)
    print("SWITRS SQLite verisi okunuyor (ilk 100 satır)...")
    switrs_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:sqlite:{args.sqlite}") \
        .option("dbtable", args.table) \
        .option("driver", "org.sqlite.JDBC") \
        .load()

    # Only select the necessary columns and limit to 100 rows
    switrs_df = switrs_df.select("case_id", "latitude", "longitude", "direction")
    
    print(f"SWITRS SQLite sütunları: {switrs_df.columns}")
    print("SWITRS verisi önizlemesi (ilk 5 satır):")
    

    # Read LargeST metadata from CSV (no limit)
    print("LargeST CSV verisi okunuyor...")
    meta_df = spark.read.csv(args.meta_csv, header=True, inferSchema=True)

    # Only select the necessary columns (no limit)
    meta_df = meta_df.select("ID", "Lat", "Lng", "Fwy", "Direction")
    
    print(f"LargeST CSV sütunları: {meta_df.columns}")
    print("LargeST verisi önizlemesi (ilk 5 satır):")
    

    # Rename for clarity
    print("Sütun isimleri düzenleniyor...")
    switrs_df = switrs_df.withColumnRenamed("latitude", "sw_lat") \
                         .withColumnRenamed("longitude", "sw_lng")

    meta_df = meta_df.withColumnRenamed("Lat", "lg_lat") \
                     .withColumnRenamed("Lng", "lg_lng") \
                     .withColumnRenamed("Fwy", "lg_road") \
                     .withColumnRenamed("Direction", "lg_direction")

    print(f"SWITRS verisi sonrası sütunlar: {switrs_df.columns}")
    print(f"LargeST verisi sonrası sütunlar: {meta_df.columns}")

    # Convert direction to full form in both datasets
    meta_df = meta_df.withColumn("lg_direction_full", convert_direction(col("lg_direction")))
    switrs_df = switrs_df.withColumn("direction_full", convert_direction(col("direction")))

    # Get opposite direction
    meta_df = meta_df.withColumn("opposite_direction", get_opposite_direction(col("lg_direction_full")))
    
    # Konum eşleşmesini yön bilgisiyle birlikte yapıyoruz
    print("Veri setleri eşleştiriliyor... (Mesafe, yön ve konum)")

    meta_df.show(50, truncate=False)
    switrs_df.show(50, truncate=False)

    # Yön ve konum eşleşmesi
    joined_df = meta_df.join(switrs_df, 
        (F.abs(meta_df['lg_lat'] - switrs_df['sw_lat']) < 0.05) & 
        (F.abs(meta_df['lg_lng'] - switrs_df['sw_lng']) < 0.05) &
        (meta_df['lg_direction_full'] == switrs_df['direction'])
    )

    joined_df.show(50, truncate=False)

    # Yönlere göre mesafe filtrelemesi
    joined_df = joined_df.withColumn("direction_match", when(
        (switrs_df['direction'] == "north") & (meta_df['lg_lat'] < switrs_df['sw_lat']), True
    ).when(
        (switrs_df['direction'] == "south") & (meta_df['lg_lat'] > switrs_df['sw_lat']), True
    ).when(
        (switrs_df['direction'] == "east") & (meta_df['lg_lng'] < switrs_df['sw_lng']), True
    ).when(
        (switrs_df['direction'] == "west") & (meta_df['lg_lng'] > switrs_df['sw_lng']), True
    ).otherwise(False))

    # Filtreyi uygula
    joined_df = joined_df.filter(col("direction_match") == True)

    # Mesafeyi hesapla
    print("Mesafe hesaplanıyor...")
    joined_df = joined_df.withColumn("distance_m", haversine_expr(
        col("lg_lat"), col("lg_lng"),
        col("sw_lat"), col("sw_lng")
    ))

    # Mesafeye göre filtreleme
    joined_df = joined_df.filter(col("distance_m") <= args.distance)

    # Sonuçları kontrol et
    print(f"Eşleşen kayıt sayısı: {joined_df.count()}")

    # Veriyi kontrol et
    if args.output:
        print(f"Veri CSV formatında dışa aktarılıyor: {args.output}")
        joined_df.coalesce(1).write.mode("overwrite").csv(args.output, header=True)
        print(f"Veri başarıyla {args.output} yoluna kaydedildi.")
    else:
        selected_columns = joined_df.select(
            "ID", "lg_lat", "lg_lng", "sw_county", "distance_m", 
        )

        selected_columns.show(50, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
