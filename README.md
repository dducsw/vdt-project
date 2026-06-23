# Real-Time ODS & Analytics System (Apache Doris + Spark + Kafka + Debezium)

This repository contains the architecture, configuration, pipelines, and benchmarks for a real-time Operational Data Store (ODS) and analytics platform. The project is based on the data architecture from **TheLook eCommerce**, featuring a transactional PostgreSQL database and simulated real-time user clickstream events.

---

## 🏗️ Architecture Overview

The platform implements two ingestion pathways optimized for latency, consistency, and resource efficiency:

```
                                  +-------------------+
                                  | PostgreSQL Source |
                                  +---------+---------+
                                            | (Debezium CDC)
                                            v
+------------------+              +---------+---------+
| Datagen Producer |              |   Kafka Broker    |
+--------+---------+              +----+--------+-----+
         | (Clickstream Events)        |        |
         v                             |        | (dim/fact topics)
+--------+---------+                   |        v
|   Kafka Broker   |                   |  +-----+-------------------------+
+--------+---------+                   |  | Doris Native Routine Load    |
         |                             |  | (Direct SQL ingestion)        |
         v                             |  +-----+-------------------------+
+--------+---------+                   |        |
| PySpark Stream   |<------------------+        | (Change Propagation)
| (JDBC lookup)    |                            v
+--------+---------+                   +--------+-------------------------+
         | (Enriched CSV Load)         |       Apache Doris OLAP         |
         v                             |   (Unique Key / Duplicate Key)   |
+--------+---------+                   +----------------------------------+
| Doris dwd_events |
+------------------+
```

1. **Change Data Capture (CDC) Pipeline**:
   * Captures database changes (Insert, Update, Delete) from PostgreSQL using **Debezium**.
   * Publishes updates to Kafka topics.
   * Ingests changes directly into **Apache Doris Unique Key tables** using native **Doris Routine Load** (SQL-based), bypassing Spark processing to save JVM memory and CPU overhead.
   * Handles delete propagation via `__DORIS_DELETE_SIGN__` mapping.

2. **Clickstream Processing Pipeline**:
   * Simulates high-velocity event streams via a Python data generator publishing to Kafka.
   * Processes streams using **PySpark Structured Streaming (Spark 3.5.6)**.
   * Applies watermarking (10 mins) and deduplication.
   * Performs dynamic dimension enrichment by joining event records with `dim_products` via MySQL JDBC from Doris FE.
   * Streams enriched records into **Apache Doris Duplicate Key tables** (`dwd_clickstream_events`) using the Spark Doris Connector (CSV Stream Load format).

---

## 📁 Repository Structure

```
vdt-project/
├── config/                  # Monitoring and configuration rules
├── data/                    # Shared data volumes (Spark checkpoints)
├── datagen/                 # Clickstream event generator (Confluent-Kafka, Faker)
├── demo/
│   └── benchmark.py         # Query latency & ingestion throughput benchmark suite
├── docker/
│   ├── doris/               # Config files for Doris FE and BE
│   ├── grafana/             # Monitoring dashboard provisions
│   └── spark/               # Spark 3.5.6 Docker image specifications
├── docker-compose.yaml      # Cluster assembly (16 containers)
├── docs/                    # Architectural requirements & logs
└── src/
    ├── debezium/            # Debezium capture registration configs
    ├── doris/               # Doris table structures (DDL), Routine Loads, & Views
    └── spark/               # PySpark streaming logic and serialization utilities
```

---

## 🚀 Deployment Guide

### Prerequisites
* Docker & Docker Compose
* Python 3.10+ (for running the benchmark locally)

### Step 1: Spin up the Infrastructure
Start all containers in background mode:
```bash
docker compose up -d
```

### Step 2: Register Debezium CDC Connector
Deploy the Change Data Capture connector to begin streaming PostgreSQL updates:
```bash
python src/debezium/register_debezium.py
```

### Step 3: Initialize Doris Tables, Routine Loads, and Views
Connect to Doris FE MySQL interface (port `9030`) and execute the SQL definitions:
```bash
# Log in to Doris FE MySQL terminal
mysql -h localhost -P 9030 -u root

# Within MySQL prompt, source the files sequentially:
mysql> SOURCE src/doris/ddl.sql;
mysql> SOURCE src/doris/routine_load.sql;
mysql> SOURCE src/doris/views.sql;
```

### Step 4: Submit Spark Clickstream Ingestion Pipeline
Submit the PySpark structured streaming job to the Spark Master node:
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/scripts/clickstream_pipeline.py
```

---

## 📊 System Benchmarking

The platform includes a built-in benchmark runner (`demo/benchmark.py`) that evaluates ingestion throughput, E2E processing lag, query latencies, and container resource footprints.

To execute the benchmark:
```bash
python demo/benchmark.py
```

### Key Performance Metrics (Sample Run)

#### Ingestion Throughput
* **Clickstream Ingestion (Spark to Doris)**: **~6,129 events/sec** peak throughput.
* **CDC Ingestion (Kafka Direct to Doris)**: Up to **~16,380 rows/sec** (fact tables) with **0 errors**.

#### Analytical Query Latency
Analytical queries ran 10 times in a warm-up sequence to record exact Avg/P95/P99 latency values:

| Query ID | Description | Avg Latency | P95 Latency | P99 Latency |
| :--- | :--- | :--- | :--- | :--- |
| **CS-1** | Total clickstream events & unique sessions | 69.5 ms | 89.4 ms | 92.8 ms |
| **CS-2** | E-commerce conversion funnel analysis | 55.8 ms | 70.3 ms | 75.9 ms |
| **CS-3** | Top 10 traffic sources by purchases | 59.6 ms | 70.9 ms | 73.3 ms |
| **CS-4** | DWS Aggregation sliding windows | 157.5 ms | 220.6 ms | 223.6 ms |
| **CS-5** | DWS Session analysis metrics | 184.6 ms | 211.1 ms | 213.4 ms |
| **CDC-1** | Orders count grouped by status | 4.9 ms | 7.1 ms | 8.0 ms |
| **CDC-2** | Category revenue (fact + dim JOIN) | 4.5 ms | 5.1 ms | 5.1 ms |
| **CDC-4** | Geo-demographics multi-table JOIN | 3.9 ms | 5.0 ms | 5.1 ms |
| **CDC-5** | Stock levels per Distribution Center | 3.1 ms | 4.1 ms | 4.1 ms |

#### Architectural Resource Optimization
By migrating CDC ingestion from Spark Streaming to native Doris Routine Loads, Spark JVM allocations were eliminated for database transactions:

* **Spark Cluster CPU Saving**: **~77% - 110% CPU load saved**.
* **Memory footprint**: Reduced JVM heap allocations by ~3GB (no longer running Spark executors for fact/dimension tables).
* **Latency**: Directly reading from Kafka to Doris FE/BE cuts out the intermediate Spark driver serialization step.
