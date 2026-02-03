# =========================
# STEP 4: CLUSTERING USING SPARK ML (FIXED + IMPROVED)
# =========================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

# -------------------------
# 1. Start Spark WITH HIVE
# -------------------------
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("AirPollutionClustering") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------
# 2. Read GOLD data from Hive
# -------------------------
spark.sql("USE air_pollution_db")

df = spark.sql("""
    SELECT
        city,
        date,
        aqi,
        pm25,
        pm10,
        no2,
        so2,
        co,
        o3
    FROM aqi_data
""")

print("Total rows from Gold table:", df.count())

# -------------------------
# 3. Drop rows with missing values
# -------------------------
df = df.dropna(subset=["aqi", "pm25", "pm10"])

print("Rows after null filtering:", df.count())

# -------------------------
# 4. Assemble numeric features
# -------------------------
feature_cols = ["aqi", "pm25", "pm10", "no2", "so2", "co", "o3"]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="raw_features",
    handleInvalid="skip"
)

df_features = assembler.transform(df)

# -------------------------
# 5. Scale features (VERY IMPORTANT for KMeans)
# -------------------------
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True
)

scaler_model = scaler.fit(df_features)
df_scaled = scaler_model.transform(df_features)

print("Rows ready for clustering:", df_scaled.count())

# -------------------------
# 6. Choose k safely
# -------------------------
row_count = df_scaled.count()
k = min(3, row_count)

print("Using k =", k)

# -------------------------
# 7. Train KMeans model
# -------------------------
kmeans = KMeans(
    k=k,
    seed=42,
    featuresCol="features"
)

model = kmeans.fit(df_scaled)

# -------------------------
# 8. Predict clusters
# -------------------------
df_clustered = model.transform(df_scaled)

# -------------------------
# 9. Final output
# -------------------------
df_final = df_clustered.select(
    "city",
    "date",
    "aqi",
    "pm25",
    "pm10",
    "no2",
    "so2",
    "co",
    "o3",
    col("prediction").alias("cluster_id")
)

# -------------------------
# 10. Save clustered data to Hive
# -------------------------
spark.sql("DROP TABLE IF EXISTS air_pollution_db.aqi_clustered")

df_final.write \
    .mode("overwrite") \
    .saveAsTable("air_pollution_db.aqi_clustered")

print("✅ Clustered data saved to Hive table: air_pollution_db.aqi_clustered")

# -------------------------
# 11. Verification
# -------------------------
df_final.show(truncate=False)

print("🎉 ML clustering completed successfully")
