# Debezium Connector Configuration (`src/debezium`)

This directory contains configuration parameters and deployment scripts to establish real-time Change Data Capture (CDC) from the source PostgreSQL database.

## File Registry

1. **[debezium_connector.json](file:///d:/Projects/vdt-project/src/debezium/debezium_connector.json)**
   * **Purpose**: Configuration payload for the Debezium PostgreSQL connector.
   * **Details**: Directs the connector to connect to `thelook_db`, monitor specified tables for changes (inserts, updates, deletes), and publish events to Kafka as JSON payloads.

2. **[register_debezium.py](file:///d:/Projects/vdt-project/src/debezium/register_debezium.py)**
   * **Purpose**: Utility script that sends a HTTP POST request registering the connector configuration with Debezium Connect's REST endpoint (port `8083`).

## How to Use

Once the Docker Compose network is fully initialized and operational, deploy the Debezium connector:

```bash
# Register the connector
python src/debezium/register_debezium.py
```

Inspect the status of the deployed connector:
```bash
curl -s http://localhost:8083/connectors/postgres-connector/status
```
