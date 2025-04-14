import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, radians, sin, cos, sqrt, asin

def haversine_expr(lat1, lon1, lat2, lon2):
    """
    Spark SQL için Haversine mesafe hesaplama ifadesi (metre cinsinden)
    """
    return 6371000 * 2 * asin(sqrt(
        sin((radians(lat2) - radians(lat1)) / 2) ** 2 +
        cos(radians(lat1)) * cos(radians(lat2)) *
        sin((radians(lon2) - radians(lon1)) / 2) ** 2
    ))

def main():
    parser = argparse.ArgumentParser(description="Join SWITRS (SQLite) and LargeST (CSV) datasets using Spark")
    parser.add_argument("--sqlite", type=str, required=True, help="Path to SWITRS SQLite file")
    parser.add_argument("--table", type=str, default="collisions", help="Table name inside SQLite file")
    parser.add_argument("--meta_csv", type=str, required=True, help="Path to LargeST metadata CSV file")
    parser.add_argument("--output", type=str, help="Output path for joined data (Parquet format)")
    parser.add_argument("--distance", type=int, default=100, help="Maximum distance in meters for spatial join")

    args = parser.parse_args()

    # Spark session
    spark = SparkSession.builder \
        .appName("Join SWITRS and LargeST metadata") \
        .config("spark.driver.extraClassPath", "/usr/share/java/sqlite-jdbc.jar") \
        .getOrCreate()

    # Read SWITRS collisions from SQLite
    switrs_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:sqlite:{args.sqlite}") \
        .option("dbtable", args.table) \
        .option("driver", "org.sqlite.JDBC") \
        .load()

    # Read LargeST metadata from CSV
    meta_df = spark.read.csv(args.meta_csv, header=True, inferSchema=True)

    # Rename for clarity
    switrs_df = switrs_df.withColumnRenamed("latitude", "sw_lat") \
                         .withColumnRenamed("longitude", "sw_lng") \
                         .withColumnRenamed("primary_road", "sw_road") \
                         .withColumnRenamed("direction", "sw_direction") \
                         .withColumnRenamed("county_location", "sw_county")

    meta_df = meta_df.withColumnRenamed("Lat", "lg_lat") \
                     .withColumnRenamed("Lng", "lg_lng") \
                     .withColumnRenamed("Fwy", "lg_road") \
                     .withColumnRenamed("Direction", "lg_direction") \
                     .withColumnRenamed("County", "lg_county")

    # Cross join + filter for proximity and semantics
    joined_df = meta_df.crossJoin(switrs_df) \
        .withColumn("distance_m", haversine_expr(
            col("lg_lat"), col("lg_lng"),
            col("sw_lat"), col("sw_lng")
        )) \
        .filter(col("distance_m") <= args.distance) \
        .filter(col("lg_road") == col("sw_road")) \
        .filter(col("lg_direction") == col("sw_direction")) \
        .filter(col("lg_county") == col("sw_county"))

    # Output
    if args.output:
        joined_df.write.mode("overwrite").parquet(args.output)
        print(f"Joined data written to {args.output}")
    else:
        joined_df.show(truncate=False, n=50)

    spark.stop()

if __name__ == "__main__":
    main()
