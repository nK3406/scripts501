from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Spark oturumu
spark = SparkSession.builder.appName("CollisionComparison").getOrCreate()

# Veri setlerini oku
df_a = spark.read.option("header", True).csv("veri_a.csv")
df_b = spark.read.option("header", True).csv("veri_b.csv")

# Zaman sütunlarını uygun formata çevir
df_a = df_a.withColumn("Time", to_timestamp("Time", "yyyy-MM-dd HH:mm:ss")) \
           .withColumn("value", col("value").cast("double")) \
           .withColumn("case_id", col("case_id").cast("long")) \
           .withColumn("sensor_id", col("sensor_id").cast("long"))

df_b = df_b.withColumn("collision_timestamp", to_timestamp(concat_ws(" ", "collision_date", "collision_time"), "yyyy-MM-dd HH:mm:ss")) \
           .withColumn("case_id", col("case_id").cast("long")) \
           .withColumn("ID", col("ID").cast("long"))

# Veri setlerini birleştir
df_merged = df_a.join(df_b, (df_a.case_id == df_b.case_id) & (df_a.sensor_id == df_b.ID), how="inner")

# Zaman ilişkisini belirle
df_labeled = df_merged.withColumn(
    "time_relation",
    when(col("Time") < col("collision_timestamp"), "before")
    .when(col("Time") > col("collision_timestamp"), "after")
    .otherwise("collision")
)

# Window tanımları
window_before = Window.partitionBy("case_id", "sensor_id", "collision_timestamp").orderBy(col("Time").desc())
window_after = Window.partitionBy("case_id", "sensor_id", "collision_timestamp").orderBy(col("Time").asc())

# Önceki ve sonraki değerleri seç
df_before = df_labeled.filter(col("time_relation") == "before") \
                      .withColumn("rn", row_number().over(window_before)) \
                      .filter(col("rn") == 1) \
                      .select("case_id", "sensor_id", "collision_timestamp", col("value").alias("value_before"))

df_after = df_labeled.filter(col("time_relation") == "after") \
                     .withColumn("rn", row_number().over(window_after)) \
                     .filter(col("rn") == 1) \
                     .select("case_id", "sensor_id", "collision_timestamp", col("value").alias("value_after"))

# Sonuçları birleştir
df_result = df_b \
    .join(df_before, on=["case_id", "collision_timestamp"], how="left") \
    .join(df_after, on=["case_id", "collision_timestamp"], how="left") \
    .select("case_id", "ID", "collision_timestamp", "value_before", "value_after")

# Sonucu göster
df_result.show(truncate=False)