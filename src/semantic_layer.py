import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, current_timestamp, desc, first
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv

# Load configuration
load_dotenv()

def main():
    """
    Apple DSI Semantic Layer:
    Transforms raw 'Silver' telemetry into trusted 'Gold' Data Products.
    Demonstrates 'AI-Readiness' by providing semantically rich features.
    """
    # Initialize Spark with Delta support
    builder = SparkSession.builder \
        .appName("SignalPath-SemanticLayer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    silver_path = "./data/silver/telemetry_raw"
    gold_alerts_path = "./data/gold/telemetry_alerts"
    gold_experiment_path = "./data/gold/experiment_summary"

    if not os.path.exists(gold_alerts_path):
        print(f"⚠️  Gold alerts data not found at {gold_alerts_path}. Run processor first.")
        return

    print(f"📖 Reading Gold Alerts from {gold_alerts_path}...")
    alerts_df = spark.read.format("delta").load(gold_alerts_path)

    # 1. Experiment Performance Report
    # We compare the alerting behavior and predictive risk of Variant A (Control) vs. Variant B (Treatment)
    experiment_summary = alerts_df.groupBy("variant_id").agg(
        count("*").alias("total_windows"),
        avg(col("is_alert").cast("int")).alias("alert_rate"),
        avg("avg_temp").alias("mean_window_temp"),
        avg("predictive_failure_risk").alias("avg_predictive_risk"),
        avg("risk_velocity").alias("avg_risk_acceleration"),
        first("experiment_id").alias("experiment_id")
    )

    # 2. Add AI-Readiness Metadata
    final_df = experiment_summary.withColumn("processed_at", current_timestamp())

    # 3. Persist as a Trusted Data Product
    print(f"💾 Persisting Experiment Summary to {gold_experiment_path}...")
    final_df.write.format("delta").mode("overwrite").save(gold_experiment_path)

    # 4. Show Sample (Verification)
    print("📊 Current Experiment Performance Comparison:")
    final_df.show(truncate=False)
    print("✅ Semantic Layer execution complete.")

if __name__ == "__main__":
    main()
