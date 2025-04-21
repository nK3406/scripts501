from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_timestamp

# Spark oturumu (JDBC JAR ile başlatıldıktan sonra çalıştırılmalı)
spark = SparkSession.builder.appName("JoinTrafficAndCollisionData").getOrCreate()

# 1. Trafik zaman serisi CSV verisini oku
traffic_df = spark.read.option("header", True).option("inferSchema", True).csv("parquets_output_pandas/merged_output.csv")

# 2. SQLite içindeki 'collisions' tablosunu JDBC ile oku
collisions_df = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlite:/home/orhankocak_0233/501-main/swit") \
    .option("dbtable", "collisions") \
    .option("driver", "org.sqlite.JDBC") \
    .load()

# 3. collision_date + collision_time → event_time oluştur
collisions_df = collisions_df.withColumn(
    "event_time",
    to_timestamp(concat_ws(" ", col("collision_date"), col("collision_time")))
)

# 4. Yalnızca gerekli sütunları al
selected_columns = [
    "case_id",
    "population",
    "distance",
    "intersection",
    "weather_1",
    "ramp_intersection",
    "tow_away",
    "collision_severity",
    "killed_victims",
    "party_count",
    "road_surface",
    "road_condition_1",
    "lighting",
    "truck_collision",
    "primary_ramp",
    "event_time"
]
filtered_collisions = collisions_df.select(*selected_columns)

# 5. traffic_df ile joinle (case_id üzerinden)
joined_df = traffic_df.join(filtered_collisions, on="case_id", how="inner")

# 6. ML eğitimine uygun veri setini kaydet
joined_df.write.mode("overwrite").option("header", True).parquet("/mnt/data/ml_ready_dataset")
