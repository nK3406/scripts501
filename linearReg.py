from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

# Spark oturumunu başlat
spark = SparkSession.builder.appName("LinearRegressionTraffic").getOrCreate()

# 1. ML-ready veri setini oku
df = spark.read.option("header", True).option("inferSchema", True).csv("ml_ready_dataset")

# 2. Kullanılacak input ve target kolonları
label_col = "value"
categorical_cols = ["weather_1", "road_surface", "road_condition_1", "lighting", "truck_collision", "primary_ramp", "ramp_intersection", "intersection", "tow_away"]
numerical_cols = ["population", "distance", "killed_victims", "party_count", "collision_severity"]

# 3. Kategorik kolonları indexle + one-hot encode et
indexers = [StringIndexer(inputCol=col, outputCol=col+"_idx", handleInvalid="keep") for col in categorical_cols]
encoders = [OneHotEncoder(inputCol=col+"_idx", outputCol=col+"_vec") for col in categorical_cols]

# 4. Tüm feature'ları birleştir
feature_cols = [col+"_vec" for col in categorical_cols] + numerical_cols
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# 5. Model: Linear Regression
lr = LinearRegression(featuresCol="features", labelCol=label_col, predictionCol="prediction")

# 6. Pipeline oluştur
pipeline = Pipeline(stages=indexers + encoders + [assembler, lr])

# 7. Eğitim-veri setine uygula
model = pipeline.fit(df)

# 8. Tahmin yap
predictions = model.transform(df)

# 9. Performans değerlendirme
from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})

print(f"Linear Regression RMSE: {rmse}")
print(f"Linear Regression R2: {r2}")

# (İsteğe bağlı) tahminleri CSV olarak kaydet
# predictions.select("case_id", "sensor_id", "value", "prediction").write.mode("overwrite").option("header", True).csv("linear_regression_predictions")
