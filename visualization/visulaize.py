# =========================
# STEP 5: VISUALIZATION – AQI CLUSTERS
# =========================

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# -------------------------
# 1. Start Spark WITH HIVE
# -------------------------
spark = SparkSession.builder \
    .appName("AQI_Cluster_Visualization") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------
# 2. Load Clustered Data
# -------------------------
spark.sql("USE air_pollution_db")

df = spark.sql("""
    SELECT
        city,
        date,
        aqi,
        pm25,
        pm10,
        cluster_id
    FROM aqi_clustered
""")

print("Total rows for visualization:", df.count())

# -------------------------
# 3. Convert to Pandas (SAFE: SMALL DATA)
# -------------------------
pdf = df.toPandas()

# -------------------------
# 4. Scatter Plot: PM2.5 vs PM10
# -------------------------
plt.figure(figsize=(8, 6))
plt.scatter(
    pdf["pm25"],
    pdf["pm10"],
    c=pdf["cluster_id"],
    cmap="viridis",
    s=80,
    alpha=0.8
)

plt.xlabel("PM2.5")
plt.ylabel("PM10")
plt.title("AQI Clustering: PM2.5 vs PM10")
plt.colorbar(label="Cluster ID")
plt.grid(True)
plt.tight_layout()
plt.show()

# -------------------------
# 5. AQI Distribution per Cluster
# -------------------------
plt.figure(figsize=(8, 6))
plt.scatter(
    pdf["cluster_id"],
    pdf["aqi"],
    c=pdf["cluster_id"],
    cmap="viridis",
    s=80,
    alpha=0.8
)

plt.xlabel("Cluster ID")
plt.ylabel("AQI")
plt.title("AQI Distribution Across Clusters")
plt.grid(True)
plt.tight_layout()
plt.show()

print("✅ Visualization completed successfully")
