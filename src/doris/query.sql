USE thelook_dw;

-- ============================================================================
-- REAL-TIME ANALYTICS QUERIES FOR APACHE DORIS
-- ============================================================================

-- Query 1: Conversion Funnel Analysis (Last 1 Hour)
-- Tracks the user journey through the funnel: Total Sessions -> Product View -> Add to Cart -> Purchase
-- Computes step-by-step conversion rates.
SELECT 
    COUNT(DISTINCT session_id) AS total_sessions,
    SUM(CASE WHEN saw_product = 1 THEN 1 ELSE 0 END) AS viewed_product_sessions,
    SUM(CASE WHEN added_to_cart = 1 THEN 1 ELSE 0 END) AS add_to_cart_sessions,
    SUM(CASE WHEN purchased = 1 THEN 1 ELSE 0 END) AS purchase_sessions,
    -- Step conversion rates
    CAST(SUM(CASE WHEN saw_product = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(DISTINCT session_id) * 100 AS view_rate_pct,
    CAST(SUM(CASE WHEN added_to_cart = 1 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(SUM(CASE WHEN saw_product = 1 THEN 1 ELSE 0 END), 0) * 100 AS cart_add_rate_pct,
    CAST(SUM(CASE WHEN purchased = 1 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(SUM(CASE WHEN added_to_cart = 1 THEN 1 ELSE 0 END), 0) * 100 AS purchase_rate_pct
FROM thelook_dw.dws_clickstream_sessions
WHERE session_start >= NOW() - INTERVAL 1 HOUR;


-- Query 2: Real-time Ingestion Latency and Throughput (Last 5 Minutes)
-- Measures event lag (end-to-end processing delay from generator to Doris) and average events per second (EPS)
SELECT 
    COUNT(event_id) AS events_ingested,
    COUNT(event_id) / 300.0 AS avg_throughput_eps,
    AVG(event_lag_seconds) AS avg_latency_seconds,
    MIN(event_lag_seconds) AS min_latency_seconds,
    MAX(event_lag_seconds) AS max_latency_seconds
FROM thelook_dw.dwd_clickstream_events
WHERE ingested_at >= NOW() - INTERVAL 5 MINUTE;


-- Query 3: Trending Products and Categories (Last 15 Minutes)
-- Joins clickstream events with the product catalog dimension to identify active top viewed products/categories
SELECT 
    p.category AS product_category,
    p.brand AS product_brand,
    p.name AS product_name,
    COUNT(e.event_id) AS total_views,
    COUNT(DISTINCT e.session_id) AS unique_view_sessions
FROM thelook_dw.dwd_clickstream_events e
JOIN thelook_dw.dim_products p ON e.product_id = p.product_id
WHERE e.event_type = 'product'
  AND e.event_timestamp >= NOW() - INTERVAL 15 MINUTE
GROUP BY p.category, p.brand, p.name
ORDER BY total_views DESC
LIMIT 10;


-- Query 4: Hourly Sales Performance and Profit Margins (Last 24 Hours)
-- Retrieves hourly order counts, gross revenue, net profit, and profit margins from transaction CDC logs
SELECT 
    order_hour,
    total_orders,
    total_revenue,
    total_profit,
    returned_items,
    (total_profit / NULLIF(total_revenue, 0)) * 100 AS profit_margin_percent
FROM thelook_dw.dws_sales_overview_hourly
ORDER BY order_hour DESC
LIMIT 24;
