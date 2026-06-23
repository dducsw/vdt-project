# Apache Doris Database Scripts (`src/doris`)

This directory contains the database schema definitions and change data capture (CDC) ingestion configuration scripts for Apache Doris.

## File Registry

1. **[ddl.sql](file:///d:/Projects/vdt-project/src/doris/ddl.sql)**
   * **Purpose**: Defines the table structures and schema definitions (DDL) for the `thelook_dw` database.
   * **Details**: Sets up the dead-letter queue table (`ods_clickstream_deadletter`), the partitioned clickstream events table (`dwd_clickstream_events` using dynamic range partitions), and the fact/dimension tables for CDC ingestion.

2. **[routine_load.sql](file:///d:/Projects/vdt-project/src/doris/routine_load.sql)**
   * **Purpose**: Creates and configures native **Doris Routine Load** jobs to consume CDC database updates directly from Kafka.
   * **Details**: Parses Debezium JSON payloads, converts raw epoch microseconds into Doris `DATETIME` formats, and maps transaction delete operations using the case expression to `__DORIS_DELETE_SIGN__`.

3. **[views.sql](file:///d:/Projects/vdt-project/src/doris/views.sql)**
   * **Purpose**: Defines analytical summary views in the DWS layer.
   * **Details**: Includes window-based clickstream aggregations (`dws_clickstream_window_agg`) and session-level analytics (`dws_clickstream_sessions`).

## How to Use

Connect to the Doris FE node via any MySQL-compatible client (port `9030`) and execute the scripts:

```bash
# Log in to Doris FE MySQL interface
mysql -h localhost -P 9030 -u root -p

# Create the schema and table structures (Run this first)
SOURCE src/doris/ddl.sql;

# Deploy the Routine Load ingestion pipelines
SOURCE src/doris/routine_load.sql;

# Define the analytical database views
SOURCE src/doris/views.sql;
```
