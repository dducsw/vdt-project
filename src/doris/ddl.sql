CREATE DATABASE IF NOT EXISTS thelook_dw;
USE thelook_dw;

-- 1. ODS Clickstream Deadletter Table
CREATE TABLE IF NOT EXISTS ods_clickstream_deadletter (
    failed_at DATETIME,
    raw_message STRING,
    error_message VARCHAR(1000)
) ENGINE=OLAP
DUPLICATE KEY(failed_at)
DISTRIBUTED BY HASH(failed_at) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

-- 2. DWD Clickstream Events Table
CREATE TABLE IF NOT EXISTS dwd_clickstream_events (
    event_id BIGINT,
    event_timestamp DATETIME,
    user_id BIGINT,
    sequence_number INT,
    session_id VARCHAR(50),
    ip_address VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    browser VARCHAR(50),
    traffic_source VARCHAR(50),
    uri VARCHAR(500),
    event_type VARCHAR(50),
    event_date DATE,
    ingested_at DATETIME,
    page_type VARCHAR(50),
    product_id BIGINT,
    is_conversion BOOLEAN,
    processing_time DATETIME,
    event_lag_seconds DOUBLE,
    INDEX idx_event_type(event_type) USING INVERTED COMMENT 'Inverted index for event_type'
) ENGINE=OLAP
DUPLICATE KEY(event_id, event_timestamp)
PARTITION BY RANGE(event_date) ()
DISTRIBUTED BY HASH(session_id, event_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10",
    "bloom_filter_columns" = "session_id, user_id"
);

-- 3. DIM Users Table
CREATE TABLE IF NOT EXISTS dim_users (
    user_id BIGINT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    age INT,
    gender VARCHAR(10),
    street_address VARCHAR(255),
    postal_code VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    latitude DOUBLE,
    longitude DOUBLE,
    traffic_source VARCHAR(50),
    created_at DATETIME,
    updated_at DATETIME
) ENGINE=OLAP
UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

-- 4. DIM Products Table
CREATE TABLE IF NOT EXISTS dim_products (
    product_id BIGINT,
    cost DOUBLE,
    category VARCHAR(100),
    name VARCHAR(255),
    brand VARCHAR(100),
    retail_price DOUBLE,
    department VARCHAR(100),
    sku VARCHAR(100),
    distribution_center_id BIGINT
) ENGINE=OLAP
UNIQUE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- 5. DIM Distribution Centers Table
CREATE TABLE IF NOT EXISTS dim_distribution_centers (
    id BIGINT,
    name VARCHAR(100),
    latitude DOUBLE,
    longitude DOUBLE
) ENGINE=OLAP
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- 6. FACT Orders Table
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id BIGINT,
    user_id BIGINT,
    status VARCHAR(50),
    gender VARCHAR(10),
    created_at DATETIME,
    updated_at DATETIME,
    returned_at DATETIME,
    shipped_at DATETIME,
    delivered_at DATETIME,
    num_of_item INT
) ENGINE=OLAP
UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "orders_group"
);

-- 7. FACT Order Items Table
CREATE TABLE IF NOT EXISTS fact_order_items (
    order_id BIGINT,
    id BIGINT,
    user_id BIGINT,
    product_id BIGINT,
    inventory_item_id BIGINT,
    status VARCHAR(50),
    created_at DATETIME,
    updated_at DATETIME,
    shipped_at DATETIME,
    delivered_at DATETIME,
    returned_at DATETIME,
    sale_price DOUBLE
) ENGINE=OLAP
UNIQUE KEY(order_id, id)
DISTRIBUTED BY HASH(order_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "orders_group"
);

-- 8. FACT Inventory Items Table
CREATE TABLE IF NOT EXISTS fact_inventory_items (
    id BIGINT,
    product_id BIGINT,
    created_at DATETIME,
    sold_at DATETIME,
    cost DOUBLE,
    product_category VARCHAR(100),
    product_name VARCHAR(255),
    product_brand VARCHAR(100),
    product_retail_price DOUBLE,
    product_department VARCHAR(100),
    product_sku VARCHAR(100),
    product_distribution_center_id BIGINT
) ENGINE=OLAP
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);

-- Note: DWS clickstream window aggregation and session entities are defined as VIEWS in views.sql
