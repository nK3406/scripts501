from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_timestamp, unix_timestamp, abs
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, when

# Spark oturumu başlat
spark = SparkSession.builder.appName("Unique CaseTime Flag").getOrCreate()

# CSV dosyalarını oku
traffic_df = spark.read.option("header", "true").csv("parquets_output_pandas/merged_output.csv")

# İkinci CSV'yi oku (lokasyon verisi)
location_df = spark.read.option("header", "true").csv("/home/orhankocak_0233/501-main/scripts501/case_id_timestamps.csv")

# Zaman dönüşümleri
traffic_df = traffic_df.withColumn("Time", to_timestamp(col("Time")))
location_df = location_df.withColumn(
    "case_time",
    to_timestamp(concat_ws(" ", col("collision_date"), col("collision_time")))
).select("case_id", "case_time")

# Join işlemi
joined_df = traffic_df.join(location_df, on="case_id", how="left")

# Zaman farkını hesapla (saniye cinsinden)
joined_df = joined_df.withColumn(
    "time_diff_sec",
    abs(unix_timestamp(col("Time")) - unix_timestamp(col("case_time")))
)

# Sadece 4 dakikadan (240 saniye) az fark olanları al
filtered_df = joined_df.filter(col("time_diff_sec") <= 150)

# Her case_id için en küçük zaman farkına sahip olanı seç
window_spec = Window.partitionBy("case_id").orderBy(col("time_diff_sec").asc())

ranked_df = filtered_df.withColumn("row_num", row_number().over(window_spec))

# Şimdi orijinal `traffic_df` ile eşleşen satıra flag atacağız
flagged_df = traffic_df.join(
    ranked_df.filter(col("row_num") == 1)
             .select("case_id", "Time")
             .withColumn("isCaseTime", when(col("Time").isNotNull(), 1)),
    on=["case_id", "Time"],
    how="left"
).withColumn("isCaseTime", when(col("isCaseTime").isNull(), 0).otherwise(1))

# Sonucu göster
flagged_df.select("Time", "value", "case_id", "sensor_id", "isCaseTime").show()

# (İsteğe bağlı) dışa aktar
flagged_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("/mnt/data/flagged_output")
