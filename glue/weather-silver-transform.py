from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    to_date,
    date_format,
    row_number,
)
from pyspark.sql.window import Window


BUCKET = "weather-data-pro"
SOURCE_PATH = f"s3://{BUCKET}/raw/weather/"
TARGET_PATH = f"s3://{BUCKET}/silver/weather_observations/"

spark = SparkSession.builder.appName("WeatherSilverTransform").getOrCreate()

print(f"Reading Bronze data from {SOURCE_PATH}")
df = spark.read.parquet(SOURCE_PATH)

required_columns = [
    "id",
    "city",
    "observed_at",
    "ingested_at",
    "temperature",
    "humidity",
]

missing_columns = [name for name in required_columns if name not in df.columns]
if missing_columns:
    raise ValueError(f"Bronze data is missing required columns: {missing_columns}")

df = (
    df
    .withColumn("id", col("id").cast("long"))
    .withColumn("city", trim(col("city")))
    .withColumn("observed_at", to_timestamp(col("observed_at")))
    .withColumn("ingested_at", to_timestamp(col("ingested_at")))
    .withColumn("temperature", col("temperature").cast("double"))
    .withColumn("humidity", col("humidity").cast("double"))
)

if "wind_speed" in df.columns:
    df = df.withColumn("wind_speed", col("wind_speed").cast("double"))

df = df.filter(
    col("id").isNotNull()
    & col("city").isNotNull()
    & (col("city") != "")
    & col("observed_at").isNotNull()
    & col("ingested_at").isNotNull()
    & col("temperature").isNotNull()
    & col("humidity").isNotNull()
)

df = df.filter(
    col("temperature").between(-100, 70)
    & col("humidity").between(0, 100)
)

if "wind_speed" in df.columns:
    df = df.filter(col("wind_speed").isNull() | (col("wind_speed") >= 0))

latest_record = Window.partitionBy("id").orderBy(
    col("ingested_at").desc_nulls_last()
)

df = (
    df
    .withColumn("_row_number", row_number().over(latest_record))
    .filter(col("_row_number") == 1)
    .drop("_row_number")
)

df = (
    df
    .withColumn("observation_date", to_date(col("observed_at")))
    .withColumn("observation_hour", date_format(col("observed_at"), "HH"))
)

record_count = df.count()
print(f"Validated Silver records: {record_count}")
print(f"Writing Silver data to {TARGET_PATH}")

(
    df.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("observation_date")
    .save(TARGET_PATH)
)

print("Silver transformation completed successfully.")
spark.stop()
