import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window, current_timestamp, when
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()

# Schema for incoming JSON telemetry
schema = StructType() \
    .add("event_id", StringType()) \
    .add("train_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("speed_mph", DoubleType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("bearing_temp_f", DoubleType()) \
    .add("fuel_level_pct", DoubleType()) \
    .add("status", StringType())

def main():
    # Kafka Configuration
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    api_key = os.getenv('KAFKA_API_KEY')
    api_secret = os.getenv('KAFKA_API_SECRET')
    topic = "rail_telemetry"

    if not all([bootstrap_servers, api_key, api_secret]):
        print("❌ Missing Kafka credentials in .env. Falling back to debug mode.")
        # In a real scenario, we'd exit. Here we'll just wait for credentials.
        return

    # Initialize Spark with Delta and Kafka support
    builder = SparkSession.builder \
        .appName("AuraRail-Processor") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"📡 Subscribing to Kafka topic: {topic}")

    # Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";') \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON payload
    telemetry_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", col("timestamp").cast(TimestampType()))

    # Processing Logic 1: Maintenance Alerts (Rolling 10-minute Avg Temp > 140)
    alerts_df = telemetry_df \
        .withWatermark("event_time", "5 minutes") \
        .groupBy(
            window(col("event_time"), "10 minutes", "5 minutes"),
            col("train_id")
        ) \
        .agg(avg("bearing_temp_f").alias("avg_temp")) \
        .withColumn("is_alert", when(col("avg_temp") > 140.0, True).otherwise(False)) \
        .withColumn("processed_at", current_timestamp())

    # Write Result 1: Delta Lake Silver Table (Raw Stream)
    silver_query = telemetry_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoint/silver_telemetry") \
        .start("./data/silver/telemetry_raw")

    # Write Result 2: Delta Lake Gold Table (Maintenance Alerts)
    gold_query = alerts_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoint/gold_alerts") \
        .start("./data/gold/telemetry_alerts")

    print("🚀 Streaming queries started. Listening for telemetry...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
