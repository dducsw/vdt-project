import os
import time
import base64
import urllib.request
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.functions import col, from_json, when, to_timestamp

# Import shared utility and schemas
from doris_utils import write_to_doris
from schemas import (
    distribution_centers_schema,
    products_schema,
    users_schema,
    inventory_items_schema,
    orders_schema,
    order_items_schema
)

# 2. Debezium JSON Envelope Helper with payload wrapper
def get_debezium_wrapper_schema(table_schema):
    from pyspark.sql.types import StructType, StructField, StringType, LongType
    payload_schema = StructType([
        StructField("before", table_schema, True),
        StructField("after", table_schema, True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True)
    ])
    return StructType([
        StructField("payload", payload_schema, True)
    ])

# Explicitly define timestamp columns for schema validation
TIMESTAMP_COLUMNS = {
    "created_at", "updated_at", "sold_at", "returned_at", 
    "shipped_at", "delivered_at", "event_timestamp"
}

# 3. Process CDC Stream Helper
def process_cdc_stream(df, table_schema, primary_key_renames={}):
    # Parse payload under wrapper.payload
    parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), get_debezium_wrapper_schema(table_schema)).alias("wrapper")) \
        .select("wrapper.payload.*")
    
    # Filter out empty records
    parsed_df = parsed_df.filter(col("before").isNotNull() | col("after").isNotNull())
    
    # If operation is delete ('d'), use before state, else after state
    source_col = when(col("op") == "d", col("before")).otherwise(col("after"))
    
    flat_df = parsed_df.select(
        col("op"),
        source_col.alias("data")
    ).select("op", "data.*")
    
    # Map op = 'd' to __DORIS_DELETE_SIGN__ = 1
    result_df = flat_df.withColumn(
        "__DORIS_DELETE_SIGN__",
        when(col("op") == "d", 1).otherwise(0)
    ).drop("op")
    
    # Apply primary key renames
    for old_name, new_name in primary_key_renames.items():
        if old_name in result_df.columns:
            result_df = result_df.withColumnRenamed(old_name, new_name)
            
    # Convert date/timestamp columns from ISO strings or microsecond longs
    for field in table_schema.fields:
        actual_name = primary_key_renames.get(field.name, field.name)
        if actual_name in result_df.columns and field.name in TIMESTAMP_COLUMNS:
            if isinstance(field.dataType, LongType):
                result_df = result_df.withColumn(
                    actual_name,
                    when(col(actual_name).isNotNull(), (col(actual_name) / 1000000).cast("timestamp")).otherwise(None)
                )
            else:
                result_df = result_df.withColumn(actual_name, to_timestamp(col(actual_name)))
            
    return result_df

def main():
    print("Starting Spark Structured Streaming CDC Pipeline with partitioned checkpoints...")
    
    spark = SparkSession.builder \
        .appName("TheLook-CDC-Pipeline") \
        .getOrCreate()
    
    # Dynamically distribute doris_utils.py and schemas.py to all worker tasks
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    doris_utils_path = os.path.join(script_dir, "doris_utils.py")
    spark.sparkContext.addPyFile(doris_utils_path)
    
    schemas_path = os.path.join(script_dir, "schemas.py")
    spark.sparkContext.addPyFile(schemas_path)
    
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:29092")
    checkpoint_base = os.getenv("SPARK_CHECKPOINT_DIR", "/opt/spark/data/checkpoints")
    checkpoint_dir = f"{checkpoint_base}/cdc"
    
    configs = [
        {
            "topic": "cdc.demo.distribution_centers",
            "schema": distribution_centers_schema,
            "target": "dim_distribution_centers",
            "renames": {},
            "columns": ["id", "name", "latitude", "longitude", "__DORIS_DELETE_SIGN__"]
        },
        {
            "topic": "cdc.demo.products",
            "schema": products_schema,
            "target": "dim_products",
            "renames": {"id": "product_id"},
            "columns": ["product_id", "cost", "category", "name", "brand", "retail_price", "department", "sku", "distribution_center_id", "__DORIS_DELETE_SIGN__"]
        },
        {
            "topic": "cdc.demo.users",
            "schema": users_schema,
            "target": "dim_users",
            "renames": {"id": "user_id"},
            "columns": ["user_id", "first_name", "last_name", "email", "age", "gender", "street_address", "postal_code", "city", "state", "country", "latitude", "longitude", "traffic_source", "created_at", "updated_at", "__DORIS_DELETE_SIGN__"]
        },
        {
            "topic": "cdc.demo.inventory_items",
            "schema": inventory_items_schema,
            "target": "fact_inventory_items",
            "renames": {},
            "columns": ["id", "product_id", "created_at", "sold_at", "cost", "product_category", "product_name", "product_brand", "product_retail_price", "product_department", "product_sku", "product_distribution_center_id", "__DORIS_DELETE_SIGN__"]
        },
        {
            "topic": "cdc.demo.orders",
            "schema": orders_schema,
            "target": "fact_orders",
            "renames": {},
            "columns": ["order_id", "user_id", "status", "gender", "created_at", "updated_at", "returned_at", "shipped_at", "delivered_at", "num_of_item", "__DORIS_DELETE_SIGN__"]
        },
        {
            "topic": "cdc.demo.order_items",
            "schema": order_items_schema,
            "target": "fact_order_items",
            "renames": {},
            "columns": ["id", "order_id", "user_id", "product_id", "inventory_item_id", "status", "created_at", "updated_at", "shipped_at", "delivered_at", "returned_at", "sale_price", "__DORIS_DELETE_SIGN__"]
        }
    ]
    
    queries = []
    
    for conf in configs:
        topic = conf["topic"]
        schema = conf["schema"]
        target = conf["target"]
        renames = conf["renames"]
        cols = conf["columns"]
        
        # Read stream for this specific topic
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", "2000") \
            .option("failOnDataLoss", "true") \
            .load()
            
        # Filter out null (tombstone) values early
        df = df.filter(col("value").isNotNull())
        
        # Parse and process CDC structure
        processed_df = process_cdc_stream(df, schema, renames)
        
        # Use helper generator to avoid loop closure variable binding issues
        def make_writer(t, c):
            return lambda batch_df, epoch_id: write_to_doris(batch_df, epoch_id, t, c)
            
        # Write stream using partitioned checkpoint location per table
        query = processed_df.writeStream \
            .foreachBatch(make_writer(target, cols)) \
            .outputMode("append") \
            .option("checkpointLocation", f"{checkpoint_dir}/{target}") \
            .trigger(processingTime="5 seconds") \
            .start()
            
        queries.append(query)
        print(f"Started CDC streaming query for {topic} -> {target}")
        
    print("CDC streams active. Awaiting termination...")
    for q in queries:
        q.awaitTermination()

if __name__ == "__main__":
    main()
