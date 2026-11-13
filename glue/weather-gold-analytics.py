from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    avg,
    min,
    max,
    round,
    sum as spark_sum,
    date_trunc,
    to_date,
    when,
)


BUCKET = "weather-data-pro"
SOURCE_PATH = f"s3://{BUCKET}/silver/weather_observations/"
GOLD_PATH = f"s3://{BUCKET}/gold/"

spark = SparkSession.builder.appName("WeatherGoldAnalytics").getOrCreate()

print(f"Reading Silver data from {SOURCE_PATH}")
df = spark.read.parquet(SOURCE_PATH)

required_columns = [
    "id",
    "city",
    "observed_at",
    "temperature",
    "humidity",
]

missing_columns = [name for name in required_columns if name not in df.columns]
if missing_columns:
    raise ValueError(f"Silver data is missing required columns: {missing_columns}")

df = df.withColumn("observation_date", to_date(col("observed_at")))

has_wind_speed = "wind_speed" in df.columns

# City-level weather summary
city_aggregations = [
    count("*").alias("total_observations"),
    round(avg("temperature"), 2).alias("avg_temperature"),
    round(min("temperature"), 2).alias("min_temperature"),
    round(max("temperature"), 2).alias("max_temperature"),
    round(avg("humidity"), 2).alias("avg_humidity"),
    round(max("humidity"), 2).alias("max_humidity"),
]

if has_wind_speed:
    city_aggregations.append(
        round(avg("wind_speed"), 2).alias("avg_wind_speed")
    )

city_summary = df.groupBy("city").agg(*city_aggregations)

city_path = f"{GOLD_PATH}city_weather_summary/"
print(f"Writing city summary to {city_path}")
city_summary.write.mode("overwrite").parquet(city_path)

# Hourly weather trends
hourly_aggregations = [
    count("*").alias("total_observations"),
    round(avg("temperature"), 2).alias("avg_temperature"),
    round(avg("humidity"), 2).alias("avg_humidity"),
]

if has_wind_speed:
    hourly_aggregations.append(
        round(avg("wind_speed"), 2).alias("avg_wind_speed")
    )

hourly_trends = (
    df
    .withColumn("observation_hour", date_trunc("hour", col("observed_at")))
    .groupBy("city", "observation_date", "observation_hour")
    .agg(*hourly_aggregations)
)

hourly_path = f"{GOLD_PATH}hourly_weather_trends/"
print(f"Writing hourly trends to {hourly_path}")
(
    hourly_trends.write
    .mode("overwrite")
    .partitionBy("observation_date")
    .parquet(hourly_path)
)

# Daily weather summary
daily_aggregations = [
    count("*").alias("total_observations"),
    round(avg("temperature"), 2).alias("avg_temperature"),
    round(min("temperature"), 2).alias("min_temperature"),
    round(max("temperature"), 2).alias("max_temperature"),
    round(avg("humidity"), 2).alias("avg_humidity"),
]

if has_wind_speed:
    daily_aggregations.append(
        round(avg("wind_speed"), 2).alias("avg_wind_speed")
    )

daily_summary = (
    df
    .groupBy("city", "observation_date")
    .agg(*daily_aggregations)
)

daily_path = f"{GOLD_PATH}daily_weather_summary/"
print(f"Writing daily summary to {daily_path}")
(
    daily_summary.write
    .mode("overwrite")
    .partitionBy("observation_date")
    .parquet(daily_path)
)

# Data-quality metrics on the Silver dataset
quality_metrics = (
    df
    .groupBy("observation_date")
    .agg(
        count("*").alias("total_records"),
        countDistinct("id").alias("unique_records"),
        spark_sum(when(col("city").isNull(), 1).otherwise(0)).alias("missing_city"),
        spark_sum(when(col("temperature").isNull(), 1).otherwise(0)).alias("missing_temperature"),
        spark_sum(when(col("humidity").isNull(), 1).otherwise(0)).alias("missing_humidity"),
        spark_sum(
            when(
                col("humidity").isNotNull()
                & ~col("humidity").between(0, 100),
                1,
            ).otherwise(0)
        ).alias("invalid_humidity"),
        spark_sum(
            when(
                col("temperature").isNotNull()
                & ~col("temperature").between(-100, 70),
                1,
            ).otherwise(0)
        ).alias("invalid_temperature"),
    )
)

quality_path = f"{GOLD_PATH}weather_quality_metrics/"
print(f"Writing quality metrics to {quality_path}")
(
    quality_metrics.write
    .mode("overwrite")
    .partitionBy("observation_date")
    .parquet(quality_path)
)

print("Gold analytics transformation completed successfully.")
spark.stop()
