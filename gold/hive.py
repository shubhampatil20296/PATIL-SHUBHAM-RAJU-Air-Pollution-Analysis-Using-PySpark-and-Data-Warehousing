# =========================
# GOLD LAYER – HIVE TABLE
# =========================

from pyspark.sql import SparkSession

# -------------------------
# 1. Start Spark WITH HIVE
# -------------------------
spark = SparkSession.builder \
    .appName("AQI_Gold_Hive") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------
# 2. ABSOLUTE PATH TO SILVER DATA (AIRFLOW SAFE)
# -------------------------
SILVER_PATH = "/home/sunbeam/Downloads/BD_Pro_draft/silver/aqi_cleaned"

df_silver = spark.read.parquet(SILVER_PATH)

print("Rows in silver:", df_silver.count())

# -------------------------
# 3. Prepare Hive Database
# -------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS air_pollution_db")
spark.sql("USE air_pollution_db")

# -------------------------
# 4. Write Gold Table
# -------------------------
df_silver.select(
    "city",
    "date",
    "aqi",
    "pm25",
    "pm10",
    "no2",
    "so2",
    "co",
    "o3"
).write \
 .mode("overwrite") \
 .saveAsTable("aqi_data")

print("✅ Gold table air_pollution_db.aqi_data created")

# -------------------------
# 5. Verify
# -------------------------
spark.sql("""
    SELECT city, aqi, pm25, pm10, date
    FROM aqi_data
""").show(truncate=False)
