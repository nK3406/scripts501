import pandas as pd
import matplotlib.pyplot as plt
import glob

# Tüm part-*.csv dosyalarını birleştir
csv_files = glob.glob("delta_value_output/part-*.csv")
delta_pd = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

# Delta'yı en düşük 20 kazaya göre sırala
top_negative = delta_pd.sort_values("delta_value").head(20)

# case_id + sensor_id birlikte gösterilecek etiket olsun
top_negative["label"] = top_negative["case_id"].astype(str) + "_" + top_negative["sensor_id"].astype(str)

# Grafik
plt.figure(figsize=(12, 6))
plt.barh(top_negative["label"], top_negative["delta_value"], color="crimson")
plt.xlabel("Delta Value (Post - Pre)")
plt.title("Top 20 Accidents with Most Negative Impact on Traffic")
plt.grid(True, axis='x')
plt.tight_layout()
plt.savefig("top20_negative_impact_accidents.png")
plt.close()