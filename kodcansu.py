import pandas as pd
import numpy as np
from scipy import stats
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns
import os


def main():
   try:
       # Veri seti yolunu kontrol et
       data_path = 'parquets_output_pandas/merged_output.csv'
       print(f"Veri seti yolu: {data_path}")
      
       if not os.path.exists(data_path):
           raise FileNotFoundError(f"Veri seti bulunamadı: {data_path}")
      
       # Veri setini yükle
       print("Veri seti yükleniyor...")
       df = pd.read_csv(data_path, engine='python')
       print(f"Veri seti başarıyla yüklendi. Satır sayısı: {len(df)}")
      
       # Veri temizleme
       print("\nVeri temizleme yapılıyor...")
       # Sonsuz değerleri NaN ile değiştir
       df = df.replace([np.inf, -np.inf], np.nan)
       # NaN değerleri kaldır
       df = df.dropna()
       print(f"Temizleme sonrası satır sayısı: {len(df)}")
      
       # Zaman sütununu datetime formatına dönüştür
       print("\nZaman verileri dönüştürülüyor...")
       df['Time'] = pd.to_datetime(df['Time'])
       df['Yıl'] = df['Time'].dt.year
       df['Hafta'] = df['Time'].dt.isocalendar().week
       df['Saat'] = df['Time'].dt.hour
       print("Zaman verileri dönüştürüldü.")
      
       # Sütun isimlerini kontrol et
       print("\nVeri seti sütunları:")
       print(df.columns.tolist())
      
       # 1. Pearson/Spearman Korelasyon Analizi
       print("\n1. Korelasyon Analizi")
       print("-" * 50)
      
       # Trafik değeri ve kaza sayısı arasındaki korelasyon
       korelasyon_df = df.groupby('Time').agg({
           'value': 'mean',
           'case_id': 'nunique'
       }).reset_index()
      
       # Korelasyon hesaplamadan önce veri kontrolü
       if len(korelasyon_df) > 0:
           pearson_corr, pearson_p = stats.pearsonr(korelasyon_df['value'], korelasyon_df['case_id'])
           spearman_corr, spearman_p = stats.spearmanr(korelasyon_df['value'], korelasyon_df['case_id'])
          
           print(f"Pearson Korelasyon: {pearson_corr:.3f} (p-value: {pearson_p:.3f})")
           print(f"Spearman Korelasyon: {spearman_corr:.3f} (p-value: {spearman_p:.3f})")
          
           # Korelasyon görselleştirmesi
           plt.figure(figsize=(10, 6))
           sns.scatterplot(data=korelasyon_df, x='value', y='case_id')
           plt.title('Trafik Değeri ve Kaza Sayısı Arasındaki İlişki')
           plt.xlabel('Ortalama Trafik Değeri')
           plt.ylabel('Kaza Sayısı')
           plt.savefig('korelasyon_analizi.png')
           plt.close()
       else:
           print("Korelasyon analizi için yeterli veri bulunamadı.")
      
       # 2. Event Study ve Heatmap Analizi
       print("\n2. Event Study ve Heatmap Analizi")
       print("-" * 50)
      
       if len(df['case_id'].unique()) > 0:
           # Event Study için zaman penceresi
           event_window = 24  # 24 saatlik pencere
           time_steps = np.arange(-event_window, event_window + 1)
          
           # Her kaza için zaman serisi verilerini topla
           event_data = []
           for case_id in df['case_id'].unique():
               case_data = df[df['case_id'] == case_id]
               accident_time = case_data['Time'].min()
              
               # Kaza öncesi ve sonrası trafik değerleri
               for t in time_steps:
                   time_point = accident_time + pd.Timedelta(hours=t)
                   traffic_value = df[
                       (df['Time'] >= time_point) &
                       (df['Time'] < time_point + pd.Timedelta(hours=1))
                   ]['value'].mean()
                  
                   if not np.isnan(traffic_value):
                       event_data.append({
                           'case_id': case_id,
                           'time_step': t,
                           'traffic_value': traffic_value
                       })
          
           if event_data:
               event_df = pd.DataFrame(event_data)
              
               # Event Study görselleştirmesi
               plt.figure(figsize=(12, 6))
               sns.lineplot(data=event_df, x='time_step', y='traffic_value')
               plt.axvline(x=0, color='r', linestyle='--', label='Kaza Zamanı')
               plt.title('Kaza Öncesi ve Sonrası Trafik Değerleri')
               plt.xlabel('Saat (Kaza Zamanına Göre)')
               plt.ylabel('Ortalama Trafik Değeri')
               plt.legend()
               plt.savefig('event_study.png')
               plt.close()
              
               # Heatmap görselleştirmesi
               heatmap_data = event_df.pivot_table(
                   index='case_id',
                   columns='time_step',
                   values='traffic_value',
                   aggfunc='mean'
               )
              
               plt.figure(figsize=(15, 8))
               sns.heatmap(heatmap_data, cmap='YlOrRd', center=heatmap_data.mean().mean())
               plt.title('Kaza Öncesi ve Sonrası Trafik Değerleri Heatmap')
               plt.xlabel('Saat (Kaza Zamanına Göre)')
               plt.ylabel('Kaza ID')
               plt.savefig('event_heatmap.png')
               plt.close()
           else:
               print("Event study ve heatmap analizi için yeterli veri bulunamadı.")
       else:
           print("Event study ve heatmap analizi için kaza verisi bulunamadı.")
      
       # 3. Difference-in-Differences (DiD) Analizi
       print("\n3. Difference-in-Differences Analizi")
       print("-" * 50)
      
       if len(df) > 0:
           # 2020 yılı öncesi ve sonrası karşılaştırması
           df['treatment'] = (df['Yıl'] >= 2020).astype(int)
           df['post'] = (df['Time'] >= pd.Timestamp('2020-01-01')).astype(int)
          
           # DiD analizi
           did_results = df.groupby(['treatment', 'post']).agg({
               'value': 'mean',
               'case_id': 'nunique'
           }).unstack()
          
           # DiD görselleştirmesi
           plt.figure(figsize=(12, 6))
           did_results['value'].plot(kind='bar')
           plt.title('2020 Öncesi ve Sonrası Trafik Değerleri')
           plt.ylabel('Ortalama Trafik Değeri')
           plt.savefig('did_analysis.png')
           plt.close()
       else:
           print("DiD analizi için yeterli veri bulunamadı.")
      
       # 4. K-Means Kümeleme Analizi
       print("\n4. K-Means Kümeleme Analizi")
       print("-" * 50)
      
       if len(df) > 0:
           # Risk faktörlerini hazırla
           risk_features = df.groupby('case_id').agg({
               'value': ['mean', 'std', 'max'],
               'sensor_id': 'nunique'
           }).reset_index()
          
           # NaN değerleri temizle
           risk_features = risk_features.dropna()
          
           if len(risk_features) > 0:
               # Özellikleri normalize et
               X = risk_features[['value', 'sensor_id']].values
               X = (X - X.mean(axis=0)) / X.std(axis=0)
              
               # K-Means kümeleme
               kmeans = KMeans(n_clusters=3, random_state=42)
               risk_features['risk_cluster'] = kmeans.fit_predict(X)
              
               # Kümeleme sonuçlarını görselleştir
               plt.figure(figsize=(10, 6))
               sns.scatterplot(data=risk_features, x='value', y='sensor_id',
                             hue='risk_cluster', palette='viridis')
               plt.title('Risk Seviyelerine Göre Kümelenmiş Kazalar')
               plt.xlabel('Ortalama Trafik Değeri')
               plt.ylabel('Sensör Sayısı')
               plt.savefig('kmeans_clustering.png')
               plt.close()
           else:
               print("K-means analizi için yeterli veri bulunamadı.")
       else:
           print("K-means analizi için veri bulunamadı.")
      
       print("\nAnalizler tamamlandı ve görselleştirmeler kaydedildi:")
       print("1. korelasyon_analizi.png")
       print("2. event_study.png")
       print("3. event_heatmap.png")
       print("4. did_analysis.png")
       print("5. kmeans_clustering.png")
  
   except Exception as e:
       print(f"\nHata oluştu: {str(e)}")
       print("\nHata detayı:")
       import traceback
       traceback.print_exc()


if __name__ == "__main__":
   main()
