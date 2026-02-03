# =========================
# SILVER LAYER – CLEAN & FLATTEN AQI DATA
# =========================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType

# -------------------------
# 1. Start Spark Session
# -------------------------
spark = SparkSession.builder \
    .appName("AQI_Silver_Cleaning") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------
# 2. Absolute Path (IMPORTANT for Airflow)
# -------------------------
BRONZE_PATH = "/home/sunbeam/Downloads/BD_Pro_draft/bronze/raw_aqi_data.json"
SILVER_PATH = "/home/sunbeam/Downloads/BD_Pro_draft/silver/aqi_cleaned"

print(f"📥 Reading Bronze data from: {BRONZE_PATH}")

df_raw = spark.read.option("multiline", "true").json(BRONZE_PATH)

print("📄 RAW SCHEMA")
df_raw.printSchema()

# -------------------------
# 3. Filter successful responses
# -------------------------
df_ok = df_raw.filter(col("response.status") == "ok")

print("✅ Rows after status filter:", df_ok.count())

# -------------------------
# 4. Define AQI DATA SCHEMA (CRITICAL FIX)
# -------------------------
aqi_schema = StructType([
    StructField("aqi", LongType()),
    StructField("city", StructType([
        StructField("name", StringType()),
        StructField("geo", ArrayType(DoubleType()))
    ])),
    StructField("iaqi", StructType([
        StructField("pm25", StructType([StructField("v", DoubleType())])),
        StructField("pm10", StructType([StructField("v", DoubleType())])),
        StructField("no2", StructType([StructField("v", DoubleType())])),
        StructField("so2", StructType([StructField("v", DoubleType())])),
        StructField("co", StructType([StructField("v", DoubleType())])),
        StructField("o3", StructType([StructField("v", DoubleType())]))
    ])),
    StructField("time", StructType([
        StructField("iso", StringType())
    ]))
])

# -------------------------
# 5. Parse response.data STRING → STRUCT
# -------------------------
df_parsed = df_ok.withColumn(
    "aqi_data",
    from_json(col("response.data"), aqi_schema)
)

# -------------------------
# 6. Flatten AQI payload
# -------------------------
df_clean = df_parsed.select(
    col("city").alias("city"),
    col("fetched_at"),
    col("aqi_data.city.name").alias("station_name"),
    col("aqi_data.city.geo")[0].alias("latitude"),
    col("aqi_data.city.geo")[1].alias("longitude"),
    col("aqi_data.aqi").alias("aqi"),
    col("aqi_data.iaqi.pm25.v").alias("pm25"),
    col("aqi_data.iaqi.pm10.v").alias("pm10"),
    col("aqi_data.iaqi.no2.v").alias("no2"),
    col("aqi_data.iaqi.so2.v").alias("so2"),
    col("aqi_data.iaqi.co.v").alias("co"),
    col("aqi_data.iaqi.o3.v").alias("o3"),
    col("aqi_data.time.iso").alias("timestamp")
)

# -------------------------
# 7. Data quality checks
# -------------------------
df_clean = df_clean.dropna(subset=["aqi", "pm25", "pm10"])

# -------------------------
# 8. Add date column
# -------------------------
df_clean = df_clean.withColumn("date", to_date(col("timestamp")))

# -------------------------
# 9. Write SILVER output
# -------------------------
df_clean.write.mode("overwrite").parquet(SILVER_PATH)

print(f"✅ Silver layer written successfully to: {SILVER_PATH}")

# -------------------------
# 10. Sample output
# -------------------------
df_clean.show(5, truncate=False)
