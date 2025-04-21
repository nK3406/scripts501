import pandas as pd
from datetime import datetime, timedelta
import os
import gc  # Garbage collector

# Dosya yolları
csv_path = "./case_id_timestamps.csv"
parquet_dir = "/mnt/data/parquets"
output_file = "./parquets_output_pandas/merged_output.csv"
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# Veriyi oku ve zaman oluştur
df = pd.read_csv(csv_path)
df["timestamp"] = pd.to_datetime(df["collision_date"] + " " + df["collision_time"])
df = df[df["timestamp"] >= pd.Timestamp("2017-01-01 00:00:00")]

# 📌 1. ZAMAN SIRASINA GÖRE SIRALA
df = df.sort_values("timestamp").reset_index(drop=True)

merged_data = []
last_parquet_key = None
cached_df = None

for _, row in df.iterrows():
    case_id = row["case_id"]
    sensor_id = str(row["ID"])
    time = row["timestamp"]
    year = time.year
    part = "part1" if time < datetime(year, 7, 2, 12, 0) else "part2"
    parquet_key = f"{year}_{part}"
    parquet_file = f"{parquet_dir}/{parquet_key}.parquet"

    print(f"\n🔎 İşleniyor: case_id={case_id}, sensor_id={sensor_id}, dosya={parquet_file}")

    try:
        # ... loop içinde, dosya değişimi sırasında:
        if parquet_key != last_parquet_key:
            print(f"📥 {parquet_key} yükleniyor...")

            # cached_df varsa sil
            if 'cached_df' in locals():
                del cached_df
                gc.collect()

            cached_df = pd.read_parquet(parquet_file, engine="pyarrow")
            cached_df["Time"] = pd.to_datetime(cached_df["Time"])
            last_parquet_key = parquet_key
            print(f"✅ {parquet_key} yüklendi. {cached_df.shape[0]} satır.")


        if sensor_id not in cached_df.columns:
            print(f"⚠️ Sensor {sensor_id} bu dosyada yok.")
            continue

        start = time - timedelta(minutes=30)
        end = time + timedelta(hours=3)
        filtered = cached_df[(cached_df["Time"] >= start) & (cached_df["Time"] <= end)][["Time", sensor_id]]

        if filtered.empty:
            print(f"⚠️ Zaman aralığında ölçüm yok.")
            continue

        print(f"✅ {len(filtered)} satır bulundu.")
        filtered = filtered.rename(columns={sensor_id: "value"})
        filtered["case_id"] = case_id
        filtered["sensor_id"] = sensor_id
        merged_data.append(filtered)

    except Exception as e:
        print(f"❌ HATA: {e}")
        continue

# CSV'ye yaz
if merged_data:
    final_df = pd.concat(merged_data, ignore_index=True)
    final_df.to_csv(output_file, index=False)
    print(f"\n🎉 Toplam {len(final_df)} satır kaydedildi: {output_file}")
else:
    print("\n🚫 Hiçbir veri eklenmedi.")
