-- =====================================================================
-- DORIS ROUTINE LOAD SCRIPTS FOR CDC INGESTION (REPLACING SPARK CDC)
-- =====================================================================
USE thelook_dw;

-- 1. Routine Load for dim_distribution_centers
CREATE ROUTINE LOAD thelook_dw.routine_load_dim_distribution_centers ON dim_distribution_centers
COLUMNS(
    temp_after_id, temp_before_id, name, latitude, longitude, op,
    id = coalesce(temp_after_id, temp_before_id),
    __DORIS_DELETE_SIGN__ = case when op = 'd' then 1 else 0 end
)
PROPERTIES(
    "format" = "json",
    "jsonpaths" = "[\"$.payload.after.id\", \"$.payload.before.id\", \"$.payload.after.name\", \"$.payload.after.latitude\", \"$.payload.after.longitude\", \"$.payload.op\"]",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5",
    "max_batch_rows" = "100000",
    "max_error_number" = "100"
)
FROM KAFKA(
    "kafka_broker_list" = "kafka-broker-1:29092",
    "kafka_topic" = "cdc.demo.distribution_centers",
    "property.group.id" = "doris_cdc_group_distribution_centers",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

-- 2. Routine Load for dim_products
CREATE ROUTINE LOAD thelook_dw.routine_load_dim_products ON dim_products
COLUMNS(
    temp_after_id, temp_before_id, cost, category, name, brand, retail_price, department, sku, distribution_center_id, op,
    product_id = coalesce(temp_after_id, temp_before_id),
    __DORIS_DELETE_SIGN__ = case when op = 'd' then 1 else 0 end
)
PROPERTIES(
    "format" = "json",
    "jsonpaths" = "[\"$.payload.after.id\", \"$.payload.before.id\", \"$.payload.after.cost\", \"$.payload.after.category\", \"$.payload.after.name\", \"$.payload.after.brand\", \"$.payload.after.retail_price\", \"$.payload.after.department\", \"$.payload.after.sku\", \"$.payload.after.distribution_center_id\", \"$.payload.op\"]",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5",
    "max_batch_rows" = "100000",
    "max_error_number" = "100"
)
FROM KAFKA(
    "kafka_broker_list" = "kafka-broker-1:29092",
    "kafka_topic" = "cdc.demo.products",
    "property.group.id" = "doris_cdc_group_products",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

-- 3. Routine Load for dim_users
CREATE ROUTINE LOAD thelook_dw.routine_load_dim_users ON dim_users
COLUMNS(
    temp_after_id, temp_before_id, first_name, last_name, email, age, gender, street_address, postal_code, city, state, country, latitude, longitude, traffic_source, temp_created_at, temp_updated_at, op,
    user_id = coalesce(temp_after_id, temp_before_id),
    created_at = from_unixtime(temp_created_at / 1000000),
    updated_at = from_unixtime(temp_updated_at / 1000000),
    __DORIS_DELETE_SIGN__ = case when op = 'd' then 1 else 0 end
)
PROPERTIES(
    "format" = "json",
    "jsonpaths" = "[\"$.payload.after.id\", \"$.payload.before.id\", \"$.payload.after.first_name\", \"$.payload.after.last_name\", \"$.payload.after.email\", \"$.payload.after.age\", \"$.payload.after.gender\", \"$.payload.after.street_address\", \"$.payload.after.postal_code\", \"$.payload.after.city\", \"$.payload.after.state\", \"$.payload.after.country\", \"$.payload.after.latitude\", \"$.payload.after.longitude\", \"$.payload.after.traffic_source\", \"$.payload.after.created_at\", \"$.payload.after.updated_at\", \"$.payload.op\"]",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5",
    "max_batch_rows" = "100000",
    "max_error_number" = "100"
)
FROM KAFKA(
    "kafka_broker_list" = "kafka-broker-1:29092",
    "kafka_topic" = "cdc.demo.users",
    "property.group.id" = "doris_cdc_group_users",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

-- 4. Routine Load for fact_inventory_items
CREATE ROUTINE LOAD thelook_dw.routine_load_fact_inventory_items ON fact_inventory_items
COLUMNS(
    temp_after_id, temp_before_id, product_id, temp_created_at, temp_sold_at, cost, product_category, product_name, product_brand, product_retail_price, product_department, product_sku, product_distribution_center_id, op,
    id = coalesce(temp_after_id, temp_before_id),
    created_at = from_unixtime(temp_created_at / 1000000),
    sold_at = from_unixtime(temp_sold_at / 1000000),
    __DORIS_DELETE_SIGN__ = case when op = 'd' then 1 else 0 end
)
PROPERTIES(
    "format" = "json",
    "jsonpaths" = "[\"$.payload.after.id\", \"$.payload.before.id\", \"$.payload.after.product_id\", \"$.payload.after.created_at\", \"$.payload.after.sold_at\", \"$.payload.after.cost\", \"$.payload.after.product_category\", \"$.payload.after.product_name\", \"$.payload.after.product_brand\", \"$.payload.after.product_retail_price\", \"$.payload.after.product_department\", \"$.payload.after.product_sku\", \"$.payload.after.product_distribution_center_id\", \"$.payload.op\"]",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5",
    "max_batch_rows" = "100000",
    "max_error_number" = "100"
)
FROM KAFKA(
    "kafka_broker_list" = "kafka-broker-1:29092",
    "kafka_topic" = "cdc.demo.inventory_items",
    "property.group.id" = "doris_cdc_group_inventory_items",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

-- 5. Routine Load for fact_orders
CREATE ROUTINE LOAD thelook_dw.routine_load_fact_orders ON fact_orders
COLUMNS(
    temp_after_order_id, temp_before_order_id, user_id, status, gender, temp_created_at, temp_updated_at, temp_returned_at, temp_shipped_at, temp_delivered_at, num_of_item, op,
    order_id = coalesce(temp_after_order_id, temp_before_order_id),
    created_at = from_unixtime(temp_created_at / 1000000),
    updated_at = from_unixtime(temp_updated_at / 1000000),
    returned_at = from_unixtime(temp_returned_at / 1000000),
    shipped_at = from_unixtime(temp_shipped_at / 1000000),
    delivered_at = from_unixtime(temp_delivered_at / 1000000),
    __DORIS_DELETE_SIGN__ = case when op = 'd' then 1 else 0 end
)
PROPERTIES(
    "format" = "json",
    "jsonpaths" = "[\"$.payload.after.order_id\", \"$.payload.before.order_id\", \"$.payload.after.user_id\", \"$.payload.after.status\", \"$.payload.after.gender\", \"$.payload.after.created_at\", \"$.payload.after.updated_at\", \"$.payload.after.returned_at\", \"$.payload.after.shipped_at\", \"$.payload.after.delivered_at\", \"$.payload.after.num_of_item\", \"$.payload.op\"]",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5",
    "max_batch_rows" = "100000",
    "max_error_number" = "100"
)
FROM KAFKA(
    "kafka_broker_list" = "kafka-broker-1:29092",
    "kafka_topic" = "cdc.demo.orders",
    "property.group.id" = "doris_cdc_group_orders",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

-- 6. Routine Load for fact_order_items
CREATE ROUTINE LOAD thelook_dw.routine_load_fact_order_items ON fact_order_items
COLUMNS(
    temp_after_id, temp_before_id, order_id, user_id, product_id, inventory_item_id, status, temp_created_at, temp_updated_at, temp_shipped_at, temp_delivered_at, temp_returned_at, sale_price, op,
    id = coalesce(temp_after_id, temp_before_id),
    created_at = from_unixtime(temp_created_at / 1000000),
    updated_at = from_unixtime(temp_updated_at / 1000000),
    shipped_at = from_unixtime(temp_shipped_at / 1000000),
    delivered_at = from_unixtime(temp_delivered_at / 1000000),
    returned_at = from_unixtime(temp_returned_at / 1000000),
    __DORIS_DELETE_SIGN__ = case when op = 'd' then 1 else 0 end
)
PROPERTIES(
    "format" = "json",
    "jsonpaths" = "[\"$.payload.after.id\", \"$.payload.before.id\", \"$.payload.after.order_id\", \"$.payload.after.user_id\", \"$.payload.after.product_id\", \"$.payload.after.inventory_item_id\", \"$.payload.after.status\", \"$.payload.after.created_at\", \"$.payload.after.updated_at\", \"$.payload.after.shipped_at\", \"$.payload.after.delivered_at\", \"$.payload.after.returned_at\", \"$.payload.after.sale_price\", \"$.payload.op\"]",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5",
    "max_batch_rows" = "100000",
    "max_error_number" = "100"
)
FROM KAFKA(
    "kafka_broker_list" = "kafka-broker-1:29092",
    "kafka_topic" = "cdc.demo.order_items",
    "property.group.id" = "doris_cdc_group_order_items",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
