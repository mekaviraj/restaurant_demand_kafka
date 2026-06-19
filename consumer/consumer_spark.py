# consumer/consumer_spark.py
"""
Kafka Delivery Semantics Demo - Spark Streaming Consumer
=========================================================

This consumer demonstrates how different delivery semantics affect:
1. Message duplication (detected via event_id deduplication)
2. Message loss (detected via counting sent vs received)
3. Processing guarantees

The key is to track:
- Total messages received per mode
- Duplicate messages detected (same event_id received twice)
- Mode-specific metrics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, sum as spark_sum, lit, 
    first, max as spark_max, min as spark_min, approx_count_distinct, to_json, struct, when, greatest
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
import os

# Suppress verbose logging
import logging
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)

print("\n" + "="*70)
print("FOOD DELIVERY ORDER RELIABILITY CONSUMER (Spark Structured Streaming)")
print("="*70)
print("This consumer tracks:")
print("  ✓ Total orders received per mode")
print("  ✓ Duplicate orders (same order_id multiple times)")
print("  ✓ Accuracy and estimated loss")
print("="*70 + "\n")

# ============================================
# Spark Session Setup
# ============================================
spark = SparkSession.builder \
    .appName("KafkaDeliverySemanticsDemo") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================
# Schema for Orders Topic
# ============================================
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("restaurant_name", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price_per_item", IntegerType(), True),
    StructField("total_price", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("kafka_mode", StringType(), True),
    StructField("sent_sequence", IntegerType(), True),
    StructField("producer_sent_at", DoubleType(), True),
    StructField("prep_time_minutes", IntegerType(), True),
    StructField("is_delayed", BooleanType(), True),
    StructField("is_cancelled", BooleanType(), True),
    StructField("incident_mode", StringType(), True),
])

# ============================================
# Read from Kafka "orders" Topic
# ============================================
print("[1/4] Reading from Kafka 'orders' topic...\n")

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON from Kafka value field
df_parsed = df_raw.select(
    col("key").cast("string").alias("event_key"),
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp"),
).select(
    col("event_key"),
    col("kafka_timestamp"),
    col("data.*")
)

# ============================================
# Duplicate Detection
# ============================================
print("[2/4] Setting up duplicate detection via event_id...\n")

# For each event_id, track:
# - How many times it was received
# - Which mode it belongs to
duplicate_tracking = df_parsed \
    .groupBy("order_id", "kafka_mode") \
    .agg(
        count("*").alias("receive_count"),
        first("customer_id").alias("customer_id"),
        first("restaurant_name").alias("restaurant_name"),
        first("item_name").alias("item_name"),
        first("sent_sequence").alias("sent_sequence"),
    ) \
    .withColumn("is_duplicate", col("receive_count") > 1)

# ============================================
# Stream Query 1: Real-time Duplicate Counter
# ============================================
print("[3/4] Starting real-time metrics streams...\n")

duplicate_summary = duplicate_tracking \
    .groupBy("kafka_mode") \
    .agg(
        count("*").alias("unique_orders"),
        spark_sum("receive_count").alias("total_orders_received"),
        spark_max("sent_sequence").alias("sent_orders"),
    ) \
    .withColumn("duplicate_orders", 
                col("total_orders_received") - col("unique_orders")) \
    .withColumn("duplicate_percentage",
                when(col("total_orders_received") > 0,
                     (col("duplicate_orders") * 100.0) / col("total_orders_received")).otherwise(lit(0.0))) \
    .withColumn("estimated_loss",
                greatest(col("sent_orders") - col("total_orders_received"), lit(0))) \
    .withColumn("accuracy",
                when(col("total_orders_received") > 0,
                     (col("unique_orders") * 100.0) / col("total_orders_received")).otherwise(lit(100.0)))

# Console output - Real-time duplicate metrics
query1 = duplicate_summary \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 10) \
    .trigger(processingTime="10 seconds") \
    .start()

# ============================================
# Stream Query 2: Detailed Per-Mode Analysis
# ============================================

per_mode_metrics = df_parsed \
    .withWatermark("kafka_timestamp", "1 minute") \
    .groupBy(
        col("kafka_mode"),
        window(col("kafka_timestamp"), "10 seconds", "5 seconds")
    ) \
    .agg(
        count("*").alias("total_orders_received"),
        approx_count_distinct("order_id").alias("unique_orders"),
        spark_max("producer_sent_at").alias("latest_producer_time"),
    ) \
    .withColumn("duplicate_orders", 
                col("total_orders_received") - col("unique_orders"))

query2 = per_mode_metrics \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 15) \
    .trigger(processingTime="10 seconds") \
    .start()

# ============================================
# Stream Query 3: Write Metrics to Kafka
# ============================================
print("[4/4] Forwarding metrics to 'metrics' topic for dashboard...\n")

metrics_output = df_parsed.select(
    col("order_id").alias("key"),
    to_json(struct(
        col("order_id"),
        col("customer_id"),
        col("restaurant_name"),
        col("item_name"),
        col("quantity"),
        col("price_per_item"),
        col("total_price"),
        col("timestamp"),
        col("kafka_mode"),
        col("sent_sequence"),
        col("prep_time_minutes"),
        col("is_delayed"),
        col("is_cancelled"),
        col("incident_mode"),
        lit("received").alias("type")
    )).alias("value")
)

query3 = metrics_output \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "metrics") \
    .option("checkpointLocation", "/tmp/checkpoint_metrics") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

print("\n" + "="*70)
print("✅ All queries started! Waiting for messages...")
print("="*70)
print("\nWatch for:")
print("  • at_most_once  -> possible order LOSS")
print("  • at_least_once -> possible DUPLICATE orders")
print("  • exactly_once  -> clean order processing")
print("\nPress Ctrl+C to stop.\n")

# Keep queries running
for query in [query1, query2, query3]:
    query.awaitTermination()