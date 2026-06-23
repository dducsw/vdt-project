import os
import time
import base64
import urllib.request
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    when,
    to_timestamp,
    split,
    regexp_replace,
    element_at,
    size,
    lit,
    broadcast
)

# Import shared utility and schemas
from doris_utils import write_to_doris
from schemas import event_schema

# Valid event types for clickstream
VALID_EVENT_TYPES = ["home", "department", "category", "product", "cart", "purchase", "cancel", "return"]

def process_and_write_deadletter_batch(batch_df, epoch_id):
    """
    Processes invalid/malformed events and writes them to the deadletter table.
    """
    try:
        deadletter_df = batch_df.select(
            col("json_str").alias("raw_message"),
            when(col("data").isNull(), "Invalid JSON format")
            .otherwise("Missing required fields or invalid event_type").alias("error_message"),
            current_timestamp().alias("failed_at")
        )
        
        deadletter_cols = ["failed_at", "raw_message", "error_message"]
        write_to_doris(deadletter_df, epoch_id, "ods_clickstream_deadletter", deadletter_cols)
    except Exception as e:
        print(f"Error processing deadletter batch {epoch_id}: {e}")
        raise e

def process_and_write_valid_batch(batch_df, epoch_id):
    """
    Processes validated and deduplicated events and writes them to the DWD clickstream events table.
    Enrichment is offloaded to the OLAP database (Apache Doris) using runtime joins/views.
    """
    try:
        from pyspark.sql.types import StringType
        
        # Add ingested_at timestamp
        processed_df = batch_df.withColumn("ingested_at", current_timestamp())
        
        # Parse URI to extract page_type and product_id
        clean_uri = regexp_replace(col("uri"), "^/|/$", "")
        uri_parts = split(clean_uri, "/")
        
        raw_page_type = when(size(uri_parts) > 0, element_at(uri_parts, 1)).otherwise("home")
        
        enriched_df = processed_df.withColumn(
            "page_type",
            when(raw_page_type.isin("cancel", "return"), "post_purchase")
            .when(col("uri").isNull() | (col("uri") == ""), "unknown")
            .otherwise(raw_page_type)
        ).withColumn(
            "product_id",
            when((raw_page_type == "product") & (size(uri_parts) > 1), element_at(uri_parts, 2).cast("long"))
            .otherwise(lit(None).cast("long"))
        )
        
        # Map to Doris dwd_clickstream_events schema structure
        dwd_df = enriched_df.select(
            col("id").alias("event_id"),
            col("event_timestamp"),
            col("user_id"),
            col("sequence_number"),
            col("session_id"),
            col("ip_address"),
            col("city"),
            col("state"),
            col("postal_code"),
            col("browser"),
            col("traffic_source"),
            col("uri"),
            col("event_type"),
            col("event_timestamp").cast("date").alias("event_date"),
            col("ingested_at"),
            col("page_type"),
            col("product_id"),
            (col("event_type") == "purchase").alias("is_conversion"),
            current_timestamp().alias("processing_time"),
            (col("ingested_at").cast("double") - col("event_timestamp").cast("double")).alias("event_lag_seconds")
        )
        
        dwd_cols = [
            "event_id", "event_timestamp", "user_id", "sequence_number", "session_id", "ip_address", "city", 
            "state", "postal_code", "browser", "traffic_source", "uri", "event_type", "event_date", "ingested_at", 
            "page_type", "product_id", "is_conversion", "processing_time", "event_lag_seconds"
        ]
        
        write_to_doris(dwd_df, epoch_id, "dwd_clickstream_events", dwd_cols)
        
    except Exception as e:
        print(f"Error processing valid clickstream batch {epoch_id}: {e}")
        raise e

def main():
    print("Starting Spark Structured Streaming Clickstream Pipeline with dynamic configurations...")
    
    spark = SparkSession.builder \
        .appName("TheLook-Clickstream-Pipeline") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "4") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "4") \
        .getOrCreate()
        
    # Dynamically distribute doris_utils.py and schemas.py to all worker tasks
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    doris_utils_path = os.path.join(script_dir, "doris_utils.py")
    spark.sparkContext.addPyFile(doris_utils_path)
    
    schemas_path = os.path.join(script_dir, "schemas.py")
    spark.sparkContext.addPyFile(schemas_path)
    
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:29092")
    checkpoint_base = os.getenv("SPARK_CHECKPOINT_DIR", "/opt/spark/data/checkpoints")
    checkpoint_dir = f"{checkpoint_base}/clickstream"
    
    # Read raw stream from Kafka clickstream topic
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", "clickstream-events") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "5000") \
        .option("failOnDataLoss", "true") \
        .load()
        
    # Filter out null payloads early
    raw_stream = raw_stream.filter(col("value").isNotNull())
    
    # Cast value binary to string & parse JSON
    parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
        .withColumn("data", from_json(col("json_str"), event_schema))
        
    # Validation expression
    is_valid_expr = (
        col("data").isNotNull() &
        col("data.id").isNotNull() &
        col("data.session_id").isNotNull() &
        col("data.event_type").isNotNull() &
        col("data.created_at").isNotNull() &
        col("data.event_type").isin(VALID_EVENT_TYPES)
    )
    
    # Stream 1: Deadletter (Invalid Events)
    deadletter_stream = parsed_stream.filter(~is_valid_expr)
    
    # Stream 2: Valid Events with Watermark and Deduplication
    # Convert created_at to timestamp before applying watermark
    valid_stream = parsed_stream.filter(is_valid_expr) \
        .select("data.*") \
        .withColumn("event_timestamp", to_timestamp(col("created_at"))) \
        .withWatermark("event_timestamp", "10 minutes") \
        .dropDuplicates(["id", "event_timestamp"])
        
    # Start Deadletter stream query
    deadletter_query = deadletter_stream.writeStream \
        .foreachBatch(process_and_write_deadletter_batch) \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_dir}/deadletter") \
        .trigger(processingTime="10 seconds") \
        .start()
        
    # Start Valid Events stream query
    valid_query = valid_stream.writeStream \
        .foreachBatch(process_and_write_valid_batch) \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_dir}/valid_events") \
        .trigger(processingTime="5 seconds") \
        .start()
        
    print("Clickstream pipelines (valid & deadletter) are active. Awaiting termination...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()

