from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp

# Spark oturumunu başlat
spark = SparkSession.builder.appName("DeltaValueAnalysis").getOrCreate()

# Veriyi CSV'den oku
df = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/data/flagged_output/part-00000-215f95ee-b116-4dbd-9ec8-ceeb9e3cd3b5-c000.csv")

# Olay zamanı: isCaseTime == 1 olan satırlar
event_times = df.filter(col("isCaseTime") == 1) \
    .select("case_id", "sensor_id", col("Time").alias("event_time"))

# event_time ile joinle (aynı case_id + sensor_id'li satırlarla)
df_with_event = df.join(event_times, on=["case_id", "sensor_id"], how="inner")

# Zaman farkını dakika olarak hesapla
df_final = df_with_event.withColumn(
    "minute_diff",
    (unix_timestamp(col("Time")) - unix_timestamp(col("event_time"))) / 60
)

# Olay öncesi 30dk-5dk arası
pre_event_df = df_final.filter((col("minute_diff") >= -30) & (col("minute_diff") <= -5))

# Olay sonrası 0-30dk arası
post_event_df = df_final.filter((col("minute_diff") >= 0) & (col("minute_diff") <= 30))

# Her case_id + sensor_id için ortalama value hesapla
from pyspark.sql.functions import avg

pre_agg = pre_event_df.groupBy("case_id", "sensor_id").agg(avg("value").alias("pre_mean"))
post_agg = post_event_df.groupBy("case_id", "sensor_id").agg(avg("value").alias("post_mean"))

# Join edip delta_value hesapla
delta_df = pre_agg.join(post_agg, on=["case_id", "sensor_id"], how="inner") \
    .withColumn("delta_value", col("post_mean") - col("pre_mean"))

# Sonuçları CSV olarak kaydet
delta_df.write.mode("overwrite").option("header", True).csv("delta_value_output")

# Spark DataFrame'den Pandas'a geç
delta_pd = delta_df.toPandas()

# Grafik çizimi (delta_value dağılımı histogramı)
import matplotlib.pyplot as plt


plt.figure(figsize=(10, 6))
plt.hist(delta_pd["delta_value"], bins=50, edgecolor='black')
plt.axvline(0, color='red', linestyle='--')
plt.title("Distribution of Delta Traffic Values (Post - Pre)")
plt.xlabel("Delta Value")
plt.ylabel("Frequency")
plt.grid(True)
plt.tight_layout()
plt.savefig("delta_value_histogram.png")
plt.close()