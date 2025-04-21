

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, avg, min as spark_min, expr
from pyspark.sql.window import Window

# Spark oturumu
spark = SparkSession.builder.appName("TimeToRecovery").getOrCreate()

# 1. Veriyi oku
df = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/data/flagged_output/part-00000-215f95ee-b116-4dbd-9ec8-ceeb9e3cd3b5-c000.csv")

# 2. Olay zamanı (isCaseTime == 1 olan satırlar)
event_times = df.filter(col("isCaseTime") == 1).select(
    "case_id", "sensor_id", col("Time").alias("event_time")
)

# 3. Olay zamanını tüm satırlara joinle
df_with_event = df.join(event_times, on=["case_id", "sensor_id"], how="inner")

# 4. minute_diff hesapla
df_minute = df_with_event.withColumn(
    "minute_diff",
    (unix_timestamp("Time") - unix_timestamp("event_time")) / 60
)

# 5. Pre-event ortalamasını hesapla: -30dk ila -5dk arası
pre_event = df_minute.filter((col("minute_diff") >= -30) & (col("minute_diff") <= -5)) \
    .groupBy("case_id", "sensor_id") \
    .agg(avg("value").alias("pre_mean"))

# 6. Pre-mean ile main tabloyu birleştir
df_joined = df_minute.join(pre_event, on=["case_id", "sensor_id"], how="left")

# 7. Olay sonrası (minute_diff > 0) ve değeri pre_mean ±%10 aralığına giren ilk satır
df_filtered = df_joined.filter(
    (col("minute_diff") > 0) &
    (col("value") >= col("pre_mean") * 0.9) &
    (col("value") <= col("pre_mean") * 1.1)
)

# 8. Her olay için minimum minute_diff (yani ilk recovery anı)
window_spec = Window.partitionBy("case_id", "sensor_id")
df_ttr = df_filtered.withColumn("ttr_minutes", spark_min("minute_diff").over(window_spec)) \
    .select("case_id", "sensor_id", "pre_mean", "ttr_minutes") \
    .dropDuplicates(["case_id", "sensor_id"])

# 9. Sonuçları göster veya kaydet
df_ttr.orderBy("ttr_minutes").show(50, truncate=False)

# (Opsiyonel) CSV olarak kaydet
df_ttr.write.mode("overwrite").option("header", True).csv("/mnt/data/time_to_recovery_output")

# Spark'tan Pandas'a geçir
ttr_pd = df_ttr.toPandas()

# Histogram çiz
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
plt.hist(ttr_pd["ttr_minutes"].dropna(), bins=30, edgecolor='black')
plt.title("Time to Recovery (TTR) Distribution")
plt.xlabel("Minutes to Recovery")
plt.ylabel("Number of Accidents")
plt.grid(True)
plt.tight_layout()
plt.savefig("time_to_recovery_histogram.png")
plt.close()
