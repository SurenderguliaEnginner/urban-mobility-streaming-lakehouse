"""
Databricks Serverless PySpark Medallion Lakehouse Pipeline
Source: GCS Bucket (Raw JSON Telemetry) -> Target: BigQuery Serving Layer
"""

from pyspark.sql.functions import col, current_timestamp, to_timestamp, round as spark_round, unix_timestamp, count, sum, avg

# ---------------------------------------------------------
# 1. BRONZE LAYER: Raw Data Ingestion from GCS Lake
# ---------------------------------------------------------
# Note: Authenticated via GCP Service Account in Databricks Workspace
print("🥉 Ingesting Raw Telemetry into Bronze Delta Table...")
# Ingestion logic & timestamp tagging
# df_bronze = df_raw.withColumn("ingested_at", current_timestamp())
# df_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze_taxi_trips")

# ---------------------------------------------------------
# 2. SILVER LAYER: Data Quality & Cleansing
# ---------------------------------------------------------
print("🥈 Cleaning Anomalies and Deriving Metrics for Silver Layer...")
bronze_data = spark.table("bronze_taxi_trips")

# Quality Checks: Filter out negative/zero amounts and invalid passengers
df_silver = bronze_data.filter(
    (col("fare_amount") > 0) & 
    (col("trip_distance") > 0) & 
    (col("passenger_count") > 0)
)

# Feature Engineering: Cast timestamps & compute duration
df_silver = df_silver.withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"))) \
                     .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime"))) \
                     .withColumn("trip_duration_min", spark_round((unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60, 2)) \
                     .withColumn("tip_percentage", spark_round((col("tip_amount") / col("fare_amount")) * 100, 2))

df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_taxi_trips")

# ---------------------------------------------------------
# 3. GOLD LAYER: Business Aggregations & BigQuery Serving
# ---------------------------------------------------------
print("🥇 Producing Business KPIs for Gold Layer...")
silver_data = spark.table("silver_taxi_trips")

df_gold = silver_data.groupBy("pickup_location_id").agg(
    count("vendor_id").alias("total_trips"),
    spark_round(sum("total_amount"), 2).alias("total_revenue"),
    spark_round(avg("fare_amount"), 2).alias("avg_fare"),
    spark_round(avg("tip_amount"), 2).alias("avg_tip"),
    spark_round(avg("trip_distance"), 2).alias("avg_distance_miles")
).orderBy(col("total_revenue").desc())

df_gold.write.format("delta").mode("overwrite").saveAsTable("gold_taxi_performance")

# Pushed to Google BigQuery `shriom-de-2026.ny_taxi_data.gold_taxi_performance` for Looker Studio BI Serving