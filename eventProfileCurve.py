from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, avg
import matplotlib.pyplot as plt
import pandas as pd

# 1. Spark başlat
spark = SparkSession.builder.appName("EventProfileFull").getOrCreate()

# 2. Kazalı veri dosyasını oku
df = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/data/flagged_output/part-00000-215f95ee-b116-4dbd-9ec8-ceeb9e3cd3b5-c000.csv")

# 3. isCaseTime == 1 olan olay zamanlarını al
event_times = df.filter(col("isCaseTime") == 1).select(
    "case_id", "sensor_id", col("Time").alias("event_time")
)

# 4. Olay zamanını diğer satırlara ekle (join)
df_with_event = df.join(event_times, on=["case_id", "sensor_id"], how="inner")

# 5. Dakika farkını hesapla
df_final = df_with_event.withColumn(
    "minute_diff",
    (unix_timestamp(col("Time")) - unix_timestamp(col("event_time"))) / 60
)

# 6. Her minute_diff için ortalama trafik değeri (value)
profile_df = df_final.groupBy("minute_diff").agg(avg("value").alias("avg_value")).orderBy("minute_diff")

# 7. CSV olarak kaydet (hem tüm olay bazlı veriyi, hem profili)
df_final.write.mode("overwrite").option("header", True).csv("/mnt/data/event_aligned_output")
profile_df.write.mode("overwrite").option("header", True).csv("/mnt/data/event_profile_output")

# 8. Grafik için Pandas'a al
profile_pd = profile_df.toPandas()

# 9. Grafik çiz
plt.figure(figsize=(12, 6))
plt.plot(profile_pd["minute_diff"], profile_pd["avg_value"], marker='o', linestyle='-')
plt.axvline(0, color='red', linestyle='--', label='Accident Time')
plt.title("Event Profile Curve: Average Traffic Value Around Accidents")
plt.xlabel("Minutes from Accident")
plt.ylabel("Average Traffic Value")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("event_profile_curve.png")
plt.close()
