# PySpark Ingestion & Processing Pipelines (`src/spark`)

This directory contains the source code for the real-time clickstream processing and enrichment jobs using **Spark Structured Streaming**.

## File Registry

1. **[clickstream_pipeline.py](file:///d:/Projects/vdt-project/src/spark/clickstream_pipeline.py)**
   * **Purpose**: Main streaming execution script designed to run on Spark 3.5.6.
   * **Details**:
     * Ingests clickstream event streams from the Kafka topic `clickstream-events`.
     * Applies schemas and filters corrupted records into a dead-letter sink.
     * Implements deduplication and 10-minute watermarking policies.
     * Enriches the clickstream events with product metadata from Doris via MySQL JDBC.
     * Saves processed records into Doris using distributed writing.

2. **[doris_utils.py](file:///d:/Projects/vdt-project/src/spark/doris_utils.py)**
   * **Purpose**: Utility module supplying helper functions for writing Spark DataFrames to Doris.
   * **Details**: Configured to write using the default **CSV format** of the Spark Doris Connector for optimal throughput and to avoid JSON parsing/wrapping conflicts in Doris BE.

3. **[schemas.py](file:///d:/Projects/vdt-project/src/spark/schemas.py)**
   * **Purpose**: Declares strict PySpark `StructType` structures representing database CDC records and raw clickstream JSON events.

4. **[cdc_pipeline.py](file:///d:/Projects/vdt-project/src/spark/cdc_pipeline.py)** (Archived / Deprecated)
   * **Purpose**: Legacy Spark-based database CDC pipeline.
   * **Details**: Retired in favor of native SQL-based **Doris Routine Loads** to optimize CPU/JVM memory resource consumption across the Spark cluster.

## How to Run

Submit the Clickstream job to the Spark Master container:

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/scripts/clickstream_pipeline.py
```
