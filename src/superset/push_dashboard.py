import os
import sys
import json
import shutil
import zipfile
import yaml
import requests

# Configuration
SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USER = os.getenv("SUPERSET_USER", "admin")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD", "admin")

# Database UUID (Doris)
DB_UUID = "b4d1fcd3-1f7b-4aa8-b65a-b2f241931784"

# Dataset UUIDs
DS = {
    "sales_flat":    "96cf1534-118c-4a37-9bd0-f38b251f2bb0",
    "sales_hourly":  "2f28b488-82fa-45b9-91a5-e362fa7a9bd1",
    "product_perf":  "4f6c449f-b98a-4d2d-8e43-1d6cfb332b84",
    "clk_window":    "7e7c3f29-073c-4e06-bdca-a9cfad6aee41",
    "clk_sessions":  "f34d84a2-241f-4b33-8232-4152882b92f2",
}

# Actual data ranges (verified against Doris) # sales_flat / sales_hourly : 2023-01-12 → 2026-06-12
# clk_window / clk_sessions : 2026-06-15 → 2026-06-21
SALES_RANGE    = "2023-01-01 : 2026-12-31"
CLICK_RANGE    = "2026-06-15 : 2026-06-21"

# Dataset column definitions 
DATASET_COLUMNS = {
    "sales_flat": [
        ("order_item_id",          "BIGINT",   False),
        ("order_id",               "BIGINT",   False),
        ("user_id",                "BIGINT",   False),
        ("product_id",             "BIGINT",   False),
        ("order_item_status",      "VARCHAR",  False),
        ("order_item_created_at",  "DATETIME", True),
        ("order_item_date",        "DATE",     True),
        ("order_item_shipped_at",  "DATETIME", True),
        ("order_item_delivered_at","DATETIME", True),
        ("order_item_returned_at", "DATETIME", True),
        ("sale_price",             "DOUBLE",   False),
        ("product_cost",           "DOUBLE",   False),
        ("product_retail_price",   "DOUBLE",   False),
        ("product_profit",         "DOUBLE",   False),
        ("profit_margin",          "DOUBLE",   False),
        ("product_name",           "VARCHAR",  False),
        ("product_category",       "VARCHAR",  False),
        ("product_brand",          "VARCHAR",  False),
        ("product_department",     "VARCHAR",  False),
        ("user_gender",            "VARCHAR",  False),
        ("user_country",           "VARCHAR",  False),
        ("user_state",             "VARCHAR",  False),
        ("order_status",           "VARCHAR",  False),
        ("delivery_lead_time_days","INTEGER",  False),
        ("shipping_lead_time_days","INTEGER",  False),
    ],
    "sales_hourly": [
        ("order_hour",            "DATETIME", True),
        ("order_date",            "DATE",     True),
        ("product_category",      "VARCHAR",  False),
        ("product_department",    "VARCHAR",  False),
        ("user_country",          "VARCHAR",  False),
        ("total_items_ordered",   "BIGINT",   False),
        ("total_revenue",         "DOUBLE",   False),
        ("total_cost",            "DOUBLE",   False),
        ("total_profit",          "DOUBLE",   False),
        ("total_orders",          "BIGINT",   False),
        ("total_customers",       "BIGINT",   False),
        ("returned_items",        "BIGINT",   False),
        ("net_revenue",           "DOUBLE",   False),
        ("net_profit",            "DOUBLE",   False),
    ],
    "product_perf": [
        ("product_id",                    "BIGINT",  False),
        ("product_name",                  "VARCHAR", False),
        ("product_category",              "VARCHAR", False),
        ("product_brand",                 "VARCHAR", False),
        ("product_department",            "VARCHAR", False),
        ("retail_price",                  "DOUBLE",  False),
        ("cost",                          "DOUBLE",  False),
        ("total_items_ordered",           "BIGINT",  False),
        ("net_items_sold",                "BIGINT",  False),
        ("total_revenue",                 "DOUBLE",  False),
        ("net_revenue",                   "DOUBLE",  False),
        ("total_profit",                  "DOUBLE",  False),
        ("total_returned_items",          "BIGINT",  False),
        ("return_rate",                   "DOUBLE",  False),
        ("view_to_order_conversion_rate", "DOUBLE",  False),
    ],
    "clk_window": [
        ("window_start",         "VARCHAR",  False),
        ("event_date",           "DATE",     True),
        ("traffic_source",       "VARCHAR",  False),
        ("browser",              "VARCHAR",  False),
        ("event_type",           "VARCHAR",  False),
        ("page_type",            "VARCHAR",  False),
        ("total_events",         "BIGINT",   False),
        ("unique_sessions",      "BIGINT",   False),
        ("unique_users",         "BIGINT",   False),
        ("avg_event_lag_seconds","DOUBLE",   False),
        ("version_emitted_at",   "DATETIME", True),
    ],
    "clk_sessions": [
        ("session_id",              "VARCHAR",  False),
        ("user_id",                 "BIGINT",   False),
        ("traffic_source",          "VARCHAR",  False),
        ("browser",                 "VARCHAR",  False),
        ("session_start",           "DATETIME", True),
        ("session_end",             "DATETIME", True),
        ("session_duration_seconds","BIGINT",   False),
        ("event_count",             "BIGINT",   False),
        ("pageview_count",          "BIGINT",   False),
        ("cart_count",              "BIGINT",   False),
        ("purchase_count",          "BIGINT",   False),
        ("saw_product",             "BOOL",     False),
        ("added_to_cart",           "BOOL",     False),
        ("purchased",               "BOOL",     False),
        ("top_category",            "VARCHAR",  False),
        ("session_date",            "DATE",     True),
    ],
}

# Table name mapping (dataset key → Doris table name)
TABLE_NAMES = {
    "sales_flat":   "dws_sales_performance_flat",
    "sales_hourly": "dws_sales_overview_hourly",
    "product_perf": "dws_product_performance",
    "clk_window":   "dws_clickstream_window_agg",
    "clk_sessions": "dws_clickstream_sessions",
}

# main_dttm column per dataset
MAIN_DTTM = {
    "sales_flat":   "order_item_created_at",
    "sales_hourly": "order_hour",
    "product_perf": None,
    "clk_window":   "event_date",
    "clk_sessions": "session_start",
}


# 
# YAML generators
# 

def save_yaml(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


def gen_dataset(key):
    cols = DATASET_COLUMNS[key]
    return {
        "table_name":              TABLE_NAMES[key],
        "main_dttm_col":           MAIN_DTTM[key],
        "description":             None,
        "offset":                  0,
        "cache_timeout":           None,
        "catalog":                 None,
        "schema":                  "thelook_dw",
        "sql":                     None,
        "params":                  None,
        "template_params":         None,
        "filter_select_enabled":   True,
        "fetch_values_predicate":  None,
        "extra":                   None,
        "normalize_columns":       False,
        "always_filter_main_dttm": False,
        "folders":                 None,
        "uuid":                    DS[key],
        "metrics": [{
            "metric_name":  "count",
            "verbose_name": "COUNT(*)",
            "metric_type":  "count",
            "expression":   "COUNT(*)",
            "description":  None,
            "d3format":     None,
            "currency":     None,
            "extra":        None,
            "warning_text": None,
        }],
        "columns": [
            {
                "column_name":       name,
                "verbose_name":      None,
                "is_dttm":           is_dttm,
                "is_active":         True,
                "type":              dtype,
                "advanced_data_type": None,
                "groupby":           True,
                "filterable":        True,
                "expression":        None,
                "description":       None,
                "python_date_format": None,
                "extra":             None,
            }
            for name, dtype, is_dttm in cols
        ],
        "version":       "1.0.0",
        "database_uuid": DB_UUID,
    }


def adhoc_filter(col, range_str):
    """Simple temporal range adhoc filter."""
    return {
        "clause":       "WHERE",
        "subject":      col,
        "operator":     "TEMPORAL_RANGE",
        "comparator":   range_str,
        "expressionType": "SIMPLE",
    }


def metric_sql(sql, label):
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


def metric_simple(col, agg, label, col_type="DOUBLE"):
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col, "type": col_type},
        "aggregate": agg,
        "label": label,
    }


def big_number(slice_name, uuid_, ds_key, metric, time_col, time_range, fmt="SMART_NUMBER"):
    return {
        "slice_name":  slice_name,
        "viz_type":    "big_number_total",
        "uuid":        uuid_,
        "dataset_uuid": DS[ds_key],
        "params": {
            "viz_type": "big_number_total",
            "metric":   metric,
            "adhoc_filters": [adhoc_filter(time_col, time_range)],
            "header_font_size":     0.4,
            "subtitle_font_size":   0.15,
            "show_metric_name":     False,
            "y_axis_format":        fmt,
            "time_format":          "smart_date",
            "force_timestamp_formatting": False,
            "conditional_formatting": [],
            "extra_form_data": {},
        },
    }


def line_chart(slice_name, uuid_, ds_key, x_axis, metrics, time_col, time_range, groupby=None):
    return {
        "slice_name":  slice_name,
        "viz_type":    "echarts_timeseries_line",
        "uuid":        uuid_,
        "dataset_uuid": DS[ds_key],
        "params": {
            "viz_type":         "echarts_timeseries_line",
            "x_axis":           x_axis,
            "time_grain_sqla":  "P1M",
            "xAxisForceCategorical": False,
            "metrics":          metrics,
            "groupby":          groupby or [],
            "adhoc_filters":    [adhoc_filter(time_col, time_range)],
            "series_type":      "line",
            "show_legend":      True,
            "legendOrientation": "top",
            "x_axis_time_format": "smart_date",
            "y_axis_format":    "SMART_NUMBER",
            "zoomable":         True,
            "show_value":       False,
            "markerEnabled":    False,
            "row_limit":        50000,
            "order_desc":       True,
            "rich_tooltip":     True,
        },
    }


def bar_chart(slice_name, uuid_, ds_key, x_axis, metrics, time_col, time_range,
              groupby=None, is_horizontal=False, is_stacked=False, force_cat=True):
    return {
        "slice_name":  slice_name,
        "viz_type":    "echarts_timeseries_bar",
        "uuid":        uuid_,
        "dataset_uuid": DS[ds_key],
        "params": {
            "viz_type":              "echarts_timeseries_bar",
            "x_axis":                x_axis,
            "time_grain_sqla":       "P1D",
            "xAxisForceCategorical": force_cat,
            "x_axis_sort_asc":       True,
            "metrics":               metrics,
            "groupby":               groupby or [],
            "adhoc_filters":         [adhoc_filter(time_col, time_range)],
            "orientation":           "horizontal" if is_horizontal else "vertical",
            "stack":                 "Stack" if is_stacked else None,
            "show_legend":           True,
            "legendOrientation":     "top",
            "y_axis_format":         "SMART_NUMBER",
            "series_limit":          20,
            "order_desc":            True,
            "row_limit":             10000,
            "rich_tooltip":          True,
        },
    }


def pie_chart(slice_name, uuid_, ds_key, groupby, metric, time_col, time_range, is_donut=True):
    return {
        "slice_name":  slice_name,
        "viz_type":    "pie",
        "uuid":        uuid_,
        "dataset_uuid": DS[ds_key],
        "params": {
            "viz_type":       "pie",
            "groupby":        [groupby] if isinstance(groupby, str) else groupby,
            "metric":         metric,
            "adhoc_filters":  [adhoc_filter(time_col, time_range)],
            "donut":          is_donut,
            "show_legend":    True,
            "show_labels":    True,
            "labels_outside": True,
            "pie_label_type": "key_value_percent",
            "number_format":  "SMART_NUMBER",
            "row_limit":      20,
        },
    }


def table_chart(slice_name, uuid_, ds_key, groupby, metrics, time_col, time_range):
    return {
        "slice_name":  slice_name,
        "viz_type":    "table",
        "uuid":        uuid_,
        "dataset_uuid": DS[ds_key],
        "params": {
            "viz_type":         "table",
            "groupby":          groupby,
            "metrics":          metrics,
            "adhoc_filters":    [adhoc_filter(time_col, time_range)],
            "page_length":      10,
            "include_search":   True,
            "show_cell_bars":   True,
            "order_desc":       True,
            "row_limit":        50,
            "table_timestamp_format": "smart_date",
        },
    }


def gen_chart_yaml(spec):
    return {
        "slice_name":         spec["slice_name"],
        "description":        None,
        "certified_by":       None,
        "certification_details": None,
        "viz_type":           spec["viz_type"],
        "params":             spec["params"],
        "query_context":      None,
        "cache_timeout":      None,
        "uuid":               spec["uuid"],
        "version":            "1.0.0",
        "dataset_uuid":       spec["dataset_uuid"],
    }


# 
# LAYOUT helpers
# 

def chart_node(chart_id, uuid_, name, row_id, tab_id, width, height=50):
    return {
        "type": "CHART",
        "id":   chart_id,
        "children": [],
        "parents": ["ROOT_ID", "GRID_ID", "TABS-main", tab_id, row_id],
        "meta": {
            "chartId":   0,   # resolved at runtime by Superset
            "height":    height,
            "sliceName": name,
            "uuid":      uuid_,
            "width":     width,
        },
    }


def row_node(row_id, chart_ids, tab_id):
    return {
        "type": "ROW",
        "id":   row_id,
        "children": chart_ids,
        "parents": ["ROOT_ID", "GRID_ID", "TABS-main", tab_id],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }


def tab_node(tab_id, label, row_ids):
    return {
        "type": "TAB",
        "id":   tab_id,
        "children": row_ids,
        "parents": ["ROOT_ID", "GRID_ID", "TABS-main"],
        "meta": {
            "defaultText": "Tab title",
            "placeholder": "Tab title",
            "text":        label,
        },
    }


# 
# MAIN
# 

def main():
    base_dir  = os.path.dirname(os.path.abspath(__file__))
    build_dir = os.path.join(base_dir, "dashboard_build")

    # 1. Clean build dir
    if os.path.exists(build_dir):
        shutil.rmtree(build_dir)
    for sub in ["databases", "datasets/Doris", "charts", "dashboards"]:
        os.makedirs(os.path.join(build_dir, sub), exist_ok=True)

    print("Step 1: metadata.yaml")
    save_yaml(
        {"version": "1.0.0", "type": "assets", "timestamp": "2026-06-22T14:00:00.000000+00:00"},
        os.path.join(build_dir, "metadata.yaml"),
    )

    # 2. Database 
    print("Step 2: databases/Doris.yaml")
    save_yaml({
        "database_name": "Doris",
        "sqlalchemy_uri": "mysql+pymysql://root@doris-fe:9030/thelook_dw",
        "cache_timeout":  None,
        "expose_in_sqllab": True,
        "allow_run_async":  False,
        "allow_ctas": False,
        "allow_cvas": True,
        "allow_dml":  True,
        "allow_file_upload": False,
        "extra": {"allows_virtual_table_explore": True},
        "impersonate_user": False,
        "configuration_method": "sqlalchemy_form",
        "uuid":    DB_UUID,
        "version": "1.0.0",
    }, os.path.join(build_dir, "databases", "Doris.yaml"))

    # 3. Datasets 
    print("Step 3: datasets")
    for key in DS:
        save_yaml(
            gen_dataset(key),
            os.path.join(build_dir, "datasets", "Doris", f"{TABLE_NAMES[key]}.yaml"),
        )

    # 4. Charts 
    print("Step 4: charts")

    # TAB 1 · Sales Overview     # KPI row
    kpi_revenue = big_number(
        "Gross Revenue", "aa000001-0001-0001-0001-000000000001",
        "sales_flat", metric_sql("SUM(sale_price)", "Gross Revenue"),
        "order_item_created_at", SALES_RANGE, ",.0f",
    )
    kpi_orders = big_number(
        "Total Orders", "aa000001-0001-0001-0001-000000000002",
        "sales_flat", metric_sql("COUNT(DISTINCT order_id)", "Orders"),
        "order_item_created_at", SALES_RANGE,
    )
    kpi_profit = big_number(
        "Net Profit", "aa000001-0001-0001-0001-000000000003",
        "sales_flat", metric_sql("SUM(product_profit)", "Net Profit"),
        "order_item_created_at", SALES_RANGE, ",.0f",
    )
    kpi_customers = big_number(
        "Unique Customers", "aa000001-0001-0001-0001-000000000004",
        "sales_flat", metric_sql("COUNT(DISTINCT user_id)", "Customers"),
        "order_item_created_at", SALES_RANGE,
    )

    # Revenue trend (monthly)
    revenue_trend = line_chart(
        "Monthly Revenue & Profit", "aa000001-0001-0001-0001-000000000005",
        "sales_hourly", "order_date",
        [
            metric_simple("total_revenue", "SUM", "Revenue"),
            metric_simple("total_profit",  "SUM", "Profit"),
        ],
        "order_hour", SALES_RANGE,
    )
    revenue_trend["params"]["time_grain_sqla"] = "P1M"

    # Order status pie
    order_status_pie = pie_chart(
        "Order Status Distribution", "aa000001-0001-0001-0001-000000000006",
        "sales_flat", "order_item_status",
        metric_sql("COUNT(*)", "Orders"),
        "order_item_created_at", SALES_RANGE,
    )

    # Revenue by category bar
    cat_revenue_bar = bar_chart(
        "Revenue by Product Category", "aa000001-0001-0001-0001-000000000007",
        "sales_hourly", "product_category",
        [metric_simple("total_revenue", "SUM", "Revenue")],
        "order_hour", SALES_RANGE,
        is_horizontal=True,
    )

    # Revenue by country bar
    country_bar = bar_chart(
        "Top Countries by Revenue", "aa000001-0001-0001-0001-000000000008",
        "sales_hourly", "user_country",
        [metric_simple("total_revenue", "SUM", "Revenue")],
        "order_hour", SALES_RANGE,
        is_horizontal=True,
    )

    # Gender pie
    gender_pie = pie_chart(
        "Sales by Gender", "aa000001-0001-0001-0001-000000000009",
        "sales_flat", "user_gender",
        metric_sql("SUM(sale_price)", "Revenue"),
        "order_item_created_at", SALES_RANGE,
    )

    # Top products table
    top_products = table_chart(
        "Top Products by Revenue", "aa000001-0001-0001-0001-000000000010",
        "product_perf", ["product_name", "product_category", "product_brand"],
        [
            metric_simple("total_revenue", "SUM", "Revenue"),
            metric_simple("net_items_sold", "SUM", "Items Sold", "BIGINT"),
            metric_simple("return_rate",   "AVG", "Return Rate %"),
        ],
        "product_id", "No filter",  # product_perf has no time col
    )
    # product_perf has no time filter - override adhoc_filters to empty
    top_products["params"]["adhoc_filters"] = []

    # TAB 2 · Clickstream 
    kpi_sessions = big_number(
        "Total Sessions", "bb000002-0002-0002-0002-000000000001",
        "clk_sessions", metric_sql("COUNT(DISTINCT session_id)", "Sessions"),
        "session_start", CLICK_RANGE,
    )
    kpi_events = big_number(
        "Total Events", "bb000002-0002-0002-0002-000000000002",
        "clk_window", metric_sql("SUM(total_events)", "Events"),
        "event_date", CLICK_RANGE,
    )
    kpi_purchases = big_number(
        "Sessions with Purchase", "bb000002-0002-0002-0002-000000000003",
        "clk_sessions", metric_sql("SUM(purchase_count)", "Purchases"),
        "session_start", CLICK_RANGE,
    )

    # Events by type bar
    event_type_bar = bar_chart(
        "Events by Type", "bb000002-0002-0002-0002-000000000004",
        "clk_window", "event_type",
        [metric_simple("total_events", "SUM", "Events", "BIGINT")],
        "event_date", CLICK_RANGE,
        is_horizontal=True,
    )

    # Traffic source pie
    traffic_pie = pie_chart(
        "Sessions by Traffic Source", "bb000002-0002-0002-0002-000000000005",
        "clk_sessions", "traffic_source",
        metric_sql("COUNT(DISTINCT session_id)", "Sessions"),
        "session_start", CLICK_RANGE,
    )

    # Browser popularity
    browser_bar = bar_chart(
        "Browser Popularity", "bb000002-0002-0002-0002-000000000006",
        "clk_sessions", "browser",
        [metric_sql("COUNT(DISTINCT session_id)", "Sessions")],
        "session_start", CLICK_RANGE,
        is_horizontal=True,
    )

    # Daily events trend
    events_trend = line_chart(
        "Daily Events Trend", "bb000002-0002-0002-0002-000000000007",
        "clk_window", "event_date",
        [metric_simple("total_events", "SUM", "Events", "BIGINT")],
        "event_date", CLICK_RANGE,
    )
    events_trend["params"]["time_grain_sqla"] = "P1D"
    events_trend["params"]["xAxisForceCategorical"] = False

    # Conversion funnel table
    funnel_table = table_chart(
        "Conversion Funnel (Sessions)", "bb000002-0002-0002-0002-000000000008",
        "clk_sessions", ["top_category"],
        [
            metric_sql("COUNT(DISTINCT session_id)",                  "Sessions"),
            metric_sql("SUM(CASE WHEN saw_product    THEN 1 ELSE 0 END)", "Viewed Product"),
            metric_sql("SUM(CASE WHEN added_to_cart  THEN 1 ELSE 0 END)", "Added to Cart"),
            metric_sql("SUM(CASE WHEN purchased      THEN 1 ELSE 0 END)", "Purchased"),
        ],
        "session_start", CLICK_RANGE,
    )

    # Save charts 
    all_charts = [
        # tab1
        kpi_revenue, kpi_orders, kpi_profit, kpi_customers,
        revenue_trend, order_status_pie,
        cat_revenue_bar, country_bar,
        gender_pie, top_products,
        # tab2
        kpi_sessions, kpi_events, kpi_purchases,
        event_type_bar, traffic_pie,
        browser_bar, events_trend, funnel_table,
    ]
    for c in all_charts:
        save_yaml(gen_chart_yaml(c), os.path.join(build_dir, "charts", f"{c['uuid']}.yaml"))

    # 5. Dashboard layout 
    print("Step 5: dashboard layout")

    def cid(spec):
        return f"CHART-{spec['uuid']}"

    T1, T2 = "TAB-sales", "TAB-click"

    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "type": "ROOT", "id": "ROOT_ID",
            "children": ["GRID_ID"],
        },
        "GRID_ID": {
            "type": "GRID", "id": "GRID_ID",
            "children": ["TABS-main"],
            "parents": ["ROOT_ID"],
        },
        "TABS-main": {
            "type": "TABS", "id": "TABS-main",
            "children": [T1, T2],
            "parents": ["ROOT_ID", "GRID_ID"],
        },
        # Tab 1 
        T1: tab_node(T1, "Sales Overview", ["ROW-t1-kpi", "ROW-t1-trend", "ROW-t1-cat", "ROW-t1-prod"]),
        "ROW-t1-kpi": row_node("ROW-t1-kpi",
            [cid(kpi_revenue), cid(kpi_orders), cid(kpi_profit), cid(kpi_customers)], T1),
        cid(kpi_revenue):   chart_node(cid(kpi_revenue),   kpi_revenue["uuid"],   kpi_revenue["slice_name"],   "ROW-t1-kpi", T1, 3, 25),
        cid(kpi_orders):    chart_node(cid(kpi_orders),    kpi_orders["uuid"],    kpi_orders["slice_name"],    "ROW-t1-kpi", T1, 3, 25),
        cid(kpi_profit):    chart_node(cid(kpi_profit),    kpi_profit["uuid"],    kpi_profit["slice_name"],    "ROW-t1-kpi", T1, 3, 25),
        cid(kpi_customers): chart_node(cid(kpi_customers), kpi_customers["uuid"], kpi_customers["slice_name"], "ROW-t1-kpi", T1, 3, 25),

        "ROW-t1-trend": row_node("ROW-t1-trend",
            [cid(revenue_trend), cid(order_status_pie)], T1),
        cid(revenue_trend):     chart_node(cid(revenue_trend),     revenue_trend["uuid"],     revenue_trend["slice_name"],     "ROW-t1-trend", T1, 8, 55),
        cid(order_status_pie):  chart_node(cid(order_status_pie),  order_status_pie["uuid"],  order_status_pie["slice_name"],  "ROW-t1-trend", T1, 4, 55),

        "ROW-t1-cat": row_node("ROW-t1-cat",
            [cid(cat_revenue_bar), cid(country_bar), cid(gender_pie)], T1),
        cid(cat_revenue_bar): chart_node(cid(cat_revenue_bar), cat_revenue_bar["uuid"], cat_revenue_bar["slice_name"], "ROW-t1-cat", T1, 5, 60),
        cid(country_bar):     chart_node(cid(country_bar),     country_bar["uuid"],     country_bar["slice_name"],     "ROW-t1-cat", T1, 4, 60),
        cid(gender_pie):      chart_node(cid(gender_pie),      gender_pie["uuid"],      gender_pie["slice_name"],      "ROW-t1-cat", T1, 3, 60),

        "ROW-t1-prod": row_node("ROW-t1-prod",
            [cid(top_products)], T1),
        cid(top_products): chart_node(cid(top_products), top_products["uuid"], top_products["slice_name"], "ROW-t1-prod", T1, 12, 80),

        # Tab 2 
        T2: tab_node(T2, "Clickstream", ["ROW-t2-kpi", "ROW-t2-mid", "ROW-t2-bot"]),
        "ROW-t2-kpi": row_node("ROW-t2-kpi",
            [cid(kpi_sessions), cid(kpi_events), cid(kpi_purchases)], T2),
        cid(kpi_sessions):  chart_node(cid(kpi_sessions),  kpi_sessions["uuid"],  kpi_sessions["slice_name"],  "ROW-t2-kpi", T2, 4, 25),
        cid(kpi_events):    chart_node(cid(kpi_events),    kpi_events["uuid"],    kpi_events["slice_name"],    "ROW-t2-kpi", T2, 4, 25),
        cid(kpi_purchases): chart_node(cid(kpi_purchases), kpi_purchases["uuid"], kpi_purchases["slice_name"], "ROW-t2-kpi", T2, 4, 25),

        "ROW-t2-mid": row_node("ROW-t2-mid",
            [cid(events_trend), cid(traffic_pie)], T2),
        cid(events_trend): chart_node(cid(events_trend), events_trend["uuid"], events_trend["slice_name"], "ROW-t2-mid", T2, 8, 55),
        cid(traffic_pie):  chart_node(cid(traffic_pie),  traffic_pie["uuid"],  traffic_pie["slice_name"],  "ROW-t2-mid", T2, 4, 55),

        "ROW-t2-bot": row_node("ROW-t2-bot",
            [cid(event_type_bar), cid(browser_bar), cid(funnel_table)], T2),
        cid(event_type_bar): chart_node(cid(event_type_bar), event_type_bar["uuid"], event_type_bar["slice_name"], "ROW-t2-bot", T2, 4, 65),
        cid(browser_bar):    chart_node(cid(browser_bar),    browser_bar["uuid"],    browser_bar["slice_name"],    "ROW-t2-bot", T2, 3, 65),
        cid(funnel_table):   chart_node(cid(funnel_table),   funnel_table["uuid"],   funnel_table["slice_name"],   "ROW-t2-bot", T2, 5, 65),
    }

    # Native time-range filters 
    # The filter_time type exposes Superset's built-in date-range picker in the
    # filter sidebar with quick-select presets:
    #   Last 5 minutes | Last hour | Last 6 hours | Last day |
    #   Last week | Last month | custom range (calendar picker)
    def time_filter(fid, name, targets, default_value, tabs_in_scope):
        return {
            "id":         fid,
            "name":       name,
            "filterType": "filter_time",
            "type":       "NATIVE_FILTER",
            "targets":    targets,
            "controlValues": {"enableEmptyFilter": False},
            "defaultDataMask": {
                "extraFormData": {},
                "filterState":   {"value": default_value},
                "ownState":      {},
            },
            "cascadeParentIds": [],
            "requiredFirst":    False,
            "tabsInScope":      tabs_in_scope,
            "chartsInScope":    [],
            "scope":            {"rootPath": ["ROOT_ID"], "excluded": []},
            "description":      "",
        }

    native_filters = [
        time_filter(
            "NATIVE_FILTER-sales-time",
            "Sales Period",
            [
                {"datasetUuid": DS["sales_flat"],   "column": {"name": "order_item_created_at"}},
                {"datasetUuid": DS["sales_hourly"], "column": {"name": "order_hour"}},
            ],
            SALES_RANGE,
            ["TAB-sales"],
        ),
        time_filter(
            "NATIVE_FILTER-click-time",
            "Clickstream Period",
            [
                {"datasetUuid": DS["clk_window"],   "column": {"name": "event_date"}},
                {"datasetUuid": DS["clk_sessions"], "column": {"name": "session_start"}},
            ],
            CLICK_RANGE,
            ["TAB-click"],
        ),
    ]

    dashboard = {
        "dashboard_title": "TheLook E-Commerce Dashboard",
        "description":     "Sales (2022-2025) and Clickstream (June 2026) on Apache Doris.",
        "css":             "",
        "slug":            None,
        "certified_by":    "",
        "certification_details": "",
        "published":       True,
        "uuid":            "a6f1a068-fa2d-4cc2-beab-dfbc812cc140",
        "position":        position,
        "metadata": {
            "color_scheme_domain":  [],
            "shared_label_colors":  [],
            "map_label_colors":     {},
            "label_colors":         {},
            "chart_configuration":  {},
            "global_chart_configuration": {
                "scope":         {"rootPath": ["ROOT_ID"], "excluded": []},
                "chartsInScope": [],
            },
            "color_scheme":         "bnbColors",
            "refresh_frequency":    0,
            "expanded_slices":      {},
            "timed_refresh_immune_slices": [],
            "cross_filters_enabled": True,
            "default_filters":      "{}",
            "native_filter_configuration": native_filters,
        },
        "theme_uuid": None,
        "version":    "1.0.0",
    }
    save_yaml(dashboard, os.path.join(build_dir, "dashboards", "TheLook_ECommerce.yaml"))

    # 6. ZIP 
    print("Step 6: zipping")
    zip_file = os.path.join(base_dir, "dashboard_assets.zip")
    if os.path.exists(zip_file):
        os.remove(zip_file)
    with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(build_dir):
            for fname in files:
                fp  = os.path.join(root, fname)
                arc = "dashboard_assets/" + os.path.relpath(fp, build_dir).replace("\\", "/")
                zf.write(fp, arc)
    print(f"  -> {zip_file}")

    # 7. Upload     
    print("Step 7: uploading to Superset")
    session = requests.Session()
    try:
        r = session.post(
            f"{SUPERSET_URL}/api/v1/security/login",
            json={"username": SUPERSET_USER, "password": SUPERSET_PASSWORD,
                  "provider": "db", "refresh": True},
            timeout=10,
        )
        r.raise_for_status()
        token = r.json()["access_token"]
        session.headers.update({"Authorization": f"Bearer {token}",
                                 "Referer": f"{SUPERSET_URL}/"})
        print("  Authenticated.")

        csrf = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token", timeout=10)
        csrf.raise_for_status()
        csrf_token = csrf.json()["result"]

        import_res = session.post(
            f"{SUPERSET_URL}/api/v1/assets/import",
            headers={"Authorization": f"Bearer {token}", "X-CSRFToken": csrf_token},
            files={"bundle": ("dashboard_assets.zip", open(zip_file, "rb"), "application/zip")},
            data={"passwords": json.dumps({"databases/Doris.yaml": ""}), "overwrite": "true"},
            timeout=60,
        )

        if import_res.status_code == 200:
            print("\n[OK] DASHBOARD IMPORTED SUCCESSFULLY!")
            print("   -> http://localhost:8088/dashboard/list")
        else:
            print(f"\n[FAIL] Import failed [{import_res.status_code}]")
            try:
                print(import_res.json())
            except Exception:
                print(import_res.text[:500])
            sys.exit(1)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if os.path.exists(build_dir):
            shutil.rmtree(build_dir)
        print("  Build dir cleaned up.")


if __name__ == "__main__":
    main()
