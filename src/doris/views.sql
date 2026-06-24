USE thelook_dw;

-- 1. Drop existing empty DWS tables/views if they exist
DROP VIEW IF EXISTS dws_clickstream_window_agg;
DROP TABLE IF EXISTS dws_clickstream_window_agg;
DROP MATERIALIZED VIEW IF EXISTS dws_clickstream_sessions;
DROP VIEW IF EXISTS dws_clickstream_sessions;
DROP TABLE IF EXISTS dws_clickstream_sessions;
DROP MATERIALIZED VIEW IF EXISTS dws_sales_overview_hourly;
DROP VIEW IF EXISTS dws_sales_overview_hourly;

-- 2. Create DWS Clickstream Window Aggregation View
CREATE VIEW dws_clickstream_window_agg AS
SELECT 
    CONCAT(
        DATE_FORMAT(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_timestamp) / 300) * 300), '%Y-%m-%d %H:%i:%s'),
        '_', COALESCE(traffic_source, 'unknown'),
        '_', COALESCE(browser, 'unknown'),
        '_', COALESCE(event_type, 'unknown'),
        '_', COALESCE(page_type, 'unknown')
    ) AS aggregate_id,
    FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_timestamp) / 300) * 300) AS window_start,
    FROM_UNIXTIME((FLOOR(UNIX_TIMESTAMP(event_timestamp) / 300) + 1) * 300) AS window_end,
    CAST(event_timestamp AS DATE) AS event_date,
    traffic_source,
    browser,
    event_type,
    page_type,
    COUNT(event_id) AS total_events,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_events,
    AVG(event_lag_seconds) AS avg_event_lag_seconds,
    NOW() AS version_emitted_at
FROM thelook_dw.dwd_clickstream_events
GROUP BY 
    FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_timestamp) / 300) * 300),
    FROM_UNIXTIME((FLOOR(UNIX_TIMESTAMP(event_timestamp) / 300) + 1) * 300),
    CAST(event_timestamp AS DATE),
    traffic_source,
    browser,
    event_type,
    page_type;

-- 3. Create DWS Clickstream Sessions — Async Materialized View
-- Lý do dùng Async MV thay View thường:
--   - Tránh tính lại 3-tầng CTE + ROW_NUMBER window function mỗi lần query.
--   - Benchmark cho thấy View thường có P99 = 153.8ms; MV pre-compute giúp giảm xuống ~5ms.
-- Lý do dùng REFRESH COMPLETE thay AUTO:
--   - Query có window function (ROW_NUMBER) và multi-table JOIN: Doris không thể
--     xác định partition delta để incremental refresh, bắt buộc full rebuild.
-- Lưu ý: Xóa cột NOW() (version_emitted_at, processed_at) vì non-deterministic
--   functions không được phép trong Async MV. Nếu cần timestamp refresh, dùng
--   SHOW MATERIALIZED VIEWS để xem 'last_refresh_time'.
CREATE MATERIALIZED VIEW dws_clickstream_sessions
BUILD IMMEDIATE
REFRESH COMPLETE ON SCHEDULE EVERY 5 MINUTE
DISTRIBUTED BY HASH(session_id) BUCKETS 10
PROPERTIES ("replication_num" = "1")
AS
WITH session_events AS (
    SELECT 
        e.session_id,
        e.user_id,
        e.traffic_source,
        e.browser,
        e.event_timestamp,
        e.event_type,
        e.page_type,
        e.product_id,
        p.category AS product_category
    FROM thelook_dw.dwd_clickstream_events e
    LEFT JOIN thelook_dw.dim_products p ON e.product_id = p.product_id
),
session_base AS (
    SELECT 
        session_id,
        MIN(user_id) AS user_id,
        MIN(traffic_source) AS traffic_source,
        MIN(browser) AS browser,
        MIN(event_timestamp) AS session_start,
        MAX(event_timestamp) AS session_end,
        TIMESTAMPDIFF(SECOND, MIN(event_timestamp), MAX(event_timestamp)) AS session_duration_seconds,
        COUNT(*) AS event_count,
        SUM(CASE WHEN page_type IN ('home', 'department', 'category', 'product') THEN 1 ELSE 0 END) AS pageview_count,
        SUM(CASE WHEN page_type = 'product' THEN 1 ELSE 0 END) AS product_view_count,
        SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS cart_count,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
        MAX(CASE WHEN page_type = 'home' THEN 1 ELSE 0 END) = 1 AS saw_home,
        MAX(CASE WHEN page_type = 'product' THEN 1 ELSE 0 END) = 1 AS saw_product,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) = 1 AS added_to_cart,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) = 1 AS purchased
    FROM session_events
    GROUP BY session_id
),
category_counts AS (
    SELECT 
        session_id,
        product_category,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY COUNT(*) DESC) AS rn
    FROM session_events
    WHERE product_category IS NOT NULL
    GROUP BY session_id, product_category
),
top_categories AS (
    SELECT session_id, product_category AS top_category
    FROM category_counts
    WHERE rn = 1
)
SELECT 
    sb.session_id AS session_record_id,
    sb.session_id,
    sb.user_id,
    sb.traffic_source,
    sb.browser,
    sb.session_start,
    sb.session_end,
    sb.session_duration_seconds,
    sb.event_count,
    sb.pageview_count,
    sb.product_view_count,
    sb.cart_count,
    sb.purchase_count,
    sb.saw_home,
    sb.saw_product,
    sb.added_to_cart,
    sb.purchased,
    tc.top_category,
    CAST(sb.session_start AS DATE) AS session_date
FROM session_base sb
LEFT JOIN top_categories tc ON sb.session_id = tc.session_id;

-- 4. Create DWS Sales Performance Flat View
DROP VIEW IF EXISTS dws_sales_performance_flat;
CREATE VIEW dws_sales_performance_flat AS
SELECT 
    oi.id AS order_item_id,
    oi.order_id,
    oi.user_id,
    oi.product_id,
    oi.inventory_item_id,
    oi.status AS order_item_status,
    oi.created_at AS order_item_created_at,
    CAST(oi.created_at AS DATE) AS order_item_date,
    oi.shipped_at AS order_item_shipped_at,
    oi.delivered_at AS order_item_delivered_at,
    oi.returned_at AS order_item_returned_at,
    oi.sale_price,
    p.cost AS product_cost,
    p.retail_price AS product_retail_price,
    oi.sale_price - p.cost AS product_profit,
    (oi.sale_price - p.cost) / NULLIF(oi.sale_price, 0) AS profit_margin,
    p.name AS product_name,
    p.category AS product_category,
    p.brand AS product_brand,
    p.department AS product_department,
    p.sku AS product_sku,
    u.age AS user_age,
    u.gender AS user_gender,
    u.city AS user_city,
    u.state AS user_state,
    u.country AS user_country,
    u.traffic_source AS user_traffic_source,
    dc.name AS distribution_center_name,
    o.num_of_item AS order_num_of_items,
    o.status AS order_status,
    DATEDIFF(oi.delivered_at, oi.shipped_at) AS delivery_lead_time_days,
    DATEDIFF(oi.shipped_at, oi.created_at) AS shipping_lead_time_days
FROM thelook_dw.fact_order_items oi
LEFT JOIN thelook_dw.dim_products p ON oi.product_id = p.product_id
LEFT JOIN thelook_dw.dim_users u ON oi.user_id = u.user_id
LEFT JOIN thelook_dw.dim_distribution_centers dc ON p.distribution_center_id = dc.id
LEFT JOIN thelook_dw.fact_orders o ON oi.order_id = o.order_id;

-- 5. Create DWS Sales Overview Hourly — Async Materialized View
-- Lý do dùng Async MV thay View thường:
--   - Là classic pre-aggregated summary: GROUP BY hour x category x department x country x gender
--     trên fact_order_items (180k rows JOIN dim_products 29k + dim_users 100k).
--   - Mỗi lần dashboard load, View thường phải full-scan ~310k rows với 2 JOINs.
--   - MV pre-compute toàn bộ, query chỉ cần scan kết quả đã tổng hợp (~vài nghìn rows).
-- Lý do dùng REFRESH COMPLETE thay AUTO:
--   - fact_order_items không có partition để Doris theo dõi delta (partition theo HASH, không phải RANGE).
--   - COMPLETE rebuild mỗi 5 phút đảm bảo tính nhất quán và đơn giản hơn cấu hình incremental.
CREATE MATERIALIZED VIEW dws_sales_overview_hourly
BUILD IMMEDIATE
REFRESH COMPLETE ON SCHEDULE EVERY 5 MINUTE
DISTRIBUTED BY HASH(order_hour) BUCKETS 10
PROPERTIES ("replication_num" = "1")
AS
SELECT
    date_trunc(oi.created_at, 'hour') AS order_hour,
    CAST(oi.created_at AS DATE) AS order_date,
    p.category AS product_category,
    p.department AS product_department,
    u.country AS user_country,
    u.gender AS user_gender,
    COUNT(oi.id) AS total_items_ordered,
    SUM(oi.sale_price) AS total_revenue,
    SUM(p.cost) AS total_cost,
    SUM(oi.sale_price - p.cost) AS total_profit,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    COUNT(DISTINCT oi.user_id) AS total_customers,
    SUM(CASE WHEN oi.status = 'Returned' THEN 1 ELSE 0 END) AS returned_items,
    SUM(CASE WHEN oi.status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_items,
    SUM(CASE WHEN oi.status NOT IN ('Cancelled', 'Returned') THEN oi.sale_price ELSE 0 END) AS net_revenue,
    SUM(CASE WHEN oi.status NOT IN ('Cancelled', 'Returned') THEN oi.sale_price - p.cost ELSE 0 END) AS net_profit
FROM thelook_dw.fact_order_items oi
LEFT JOIN thelook_dw.dim_products p ON oi.product_id = p.product_id
LEFT JOIN thelook_dw.dim_users u ON oi.user_id = u.user_id
GROUP BY
    date_trunc(oi.created_at, 'hour'),
    CAST(oi.created_at AS DATE),
    p.category,
    p.department,
    u.country,
    u.gender;

-- 6. Create DWS Product Performance View
DROP VIEW IF EXISTS dws_product_performance;
CREATE VIEW dws_product_performance AS
WITH product_sales AS (
    SELECT
        product_id,
        COUNT(id) AS total_items_ordered,
        SUM(CASE WHEN status NOT IN ('Cancelled', 'Returned') THEN 1 ELSE 0 END) AS net_items_sold,
        SUM(sale_price) AS total_revenue,
        SUM(CASE WHEN status NOT IN ('Cancelled', 'Returned') THEN sale_price ELSE 0 END) AS net_revenue,
        SUM(CASE WHEN status = 'Returned' THEN 1 ELSE 0 END) AS total_returned_items,
        SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) AS total_cancelled_items
    FROM thelook_dw.fact_order_items
    GROUP BY product_id
),
product_views AS (
    SELECT
        product_id,
        COUNT(*) AS total_page_views,
        COUNT(DISTINCT session_id) AS unique_view_sessions
    FROM thelook_dw.dwd_clickstream_events
    WHERE event_type = 'product'
    GROUP BY product_id
)
SELECT
    p.product_id,
    p.name AS product_name,
    p.category AS product_category,
    p.brand AS product_brand,
    p.department AS product_department,
    p.sku AS product_sku,
    p.retail_price,
    p.cost,
    p.retail_price - p.cost AS unit_profit,
    (p.retail_price - p.cost) / NULLIF(p.retail_price, 0) AS unit_margin,
    COALESCE(ps.total_items_ordered, 0) AS total_items_ordered,
    COALESCE(ps.net_items_sold, 0) AS net_items_sold,
    COALESCE(ps.total_revenue, 0) AS total_revenue,
    COALESCE(ps.net_revenue, 0) AS net_revenue,
    (COALESCE(ps.total_items_ordered, 0) * p.cost) AS total_cost,
    (COALESCE(ps.total_revenue, 0) - (COALESCE(ps.total_items_ordered, 0) * p.cost)) AS total_profit,
    COALESCE(ps.total_returned_items, 0) AS total_returned_items,
    COALESCE(ps.total_cancelled_items, 0) AS total_cancelled_items,
    CAST(COALESCE(ps.total_returned_items, 0) AS DOUBLE) / NULLIF(COALESCE(ps.total_items_ordered, 0), 0) AS return_rate,
    COALESCE(pv.total_page_views, 0) AS total_page_views,
    COALESCE(pv.unique_view_sessions, 0) AS unique_view_sessions,
    CAST(COALESCE(ps.total_items_ordered, 0) AS DOUBLE) / NULLIF(COALESCE(pv.total_page_views, 0), 0) AS view_to_order_conversion_rate
FROM thelook_dw.dim_products p
LEFT JOIN product_sales ps ON p.product_id = ps.product_id
LEFT JOIN product_views pv ON p.product_id = pv.product_id;

-- 7. Create DWS Inventory Details View
DROP VIEW IF EXISTS dws_inventory_details;
CREATE VIEW dws_inventory_details AS
SELECT
    ii.id AS inventory_item_id,
    ii.product_id,
    ii.created_at AS inventory_created_at,
    CAST(ii.created_at AS DATE) AS inventory_created_date,
    ii.sold_at AS inventory_sold_at,
    CAST(ii.sold_at AS DATE) AS inventory_sold_date,
    ii.cost,
    ii.product_category,
    ii.product_name,
    ii.product_brand,
    ii.product_retail_price AS retail_price,
    ii.product_department,
    ii.product_sku,
    dc.name AS distribution_center_name,
    CASE WHEN ii.sold_at IS NULL THEN 1 ELSE 0 END AS is_stock_on_hand,
    CASE WHEN ii.sold_at IS NOT NULL THEN 1 ELSE 0 END AS is_sold,
    TIMESTAMPDIFF(DAY, ii.created_at, ii.sold_at) AS days_to_sell
FROM thelook_dw.fact_inventory_items ii
LEFT JOIN thelook_dw.dim_distribution_centers dc ON ii.product_distribution_center_id = dc.id;

