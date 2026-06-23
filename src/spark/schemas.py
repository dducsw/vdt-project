from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    DoubleType
)

# 1. Distribution Centers CDC Schema
distribution_centers_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
])

# 2. Products CDC Schema
products_schema = StructType([
    StructField("id", LongType(), True),
    StructField("cost", DoubleType(), True),
    StructField("category", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("retail_price", DoubleType(), True),
    StructField("department", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("distribution_center_id", LongType(), True)
])

# 3. Users CDC Schema
users_schema = StructType([
    StructField("id", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("street_address", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("traffic_source", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True)
])

# 4. Inventory Items CDC Schema
inventory_items_schema = StructType([
    StructField("id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("created_at", LongType(), True),
    StructField("sold_at", LongType(), True),
    StructField("cost", DoubleType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_brand", StringType(), True),
    StructField("product_retail_price", DoubleType(), True),
    StructField("product_department", StringType(), True),
    StructField("product_sku", StringType(), True),
    StructField("product_distribution_center_id", LongType(), True)
])

# 5. Orders CDC Schema
orders_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True),
    StructField("returned_at", LongType(), True),
    StructField("shipped_at", LongType(), True),
    StructField("delivered_at", LongType(), True),
    StructField("num_of_item", IntegerType(), True)
])

# 6. Order Items CDC Schema
order_items_schema = StructType([
    StructField("id", LongType(), True),
    StructField("order_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("inventory_item_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True),
    StructField("shipped_at", LongType(), True),
    StructField("delivered_at", LongType(), True),
    StructField("returned_at", LongType(), True),
    StructField("sale_price", DoubleType(), True)
])

# 7. Raw Event Schema for Clickstream
event_schema = StructType([
    StructField("id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("sequence_number", IntegerType(), True),
    StructField("session_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("traffic_source", StringType(), True),
    StructField("uri", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("created_at", StringType(), True)
])
