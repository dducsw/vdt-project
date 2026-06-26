# theLook eCommerce Real-Time Data Ingestion & Generation Suite (`datagen`)

This directory contains the real-time clickstream simulator and transactional database management scripts for theLook eCommerce platform. It is designed to model a production eCommerce environment where client applications (browsers, mobile apps) ingest events via an HTTP API Gateway, and database updates are captured via Change Data Capture (CDC).

---

## 🏗️ Architecture Overview

The suite consists of two distinct components:

```
1. Clickstream Pipeline:
   Simulator (http_client.py) ──[HTTP POST]──► BFF Gateway (stream_app.py) ──[Kafka TCP]──► Kafka (clickstream-events)

2. CDC Database Pipeline:
   Smart Launcher (run_generator.py) ──[Auto Seed if empty]──► PostgreSQL ──[CDC / WAL]──► Debezium ──► Kafka (cdc.demo.*)
```

### 1. Clickstream Event Ingestion (HTTP Gateway + Simulator)
* **BFF (Backend-For-Frontend) Ingestion Gateway (`stream_app.py`)**: A FastAPI web server that acts as a secure buffer. It exposes a POST endpoint (`/events/ingest`) to receive clickstream events from client simulators and publish them to the Kafka `clickstream-events` topic.
* **Clickstream Simulator (`http_client.py`)**: An HTTP client simulator that mimics actual user browsing behaviors (pages viewed, search, cart operations, checkout) and sends event batches to the BFF. It has been stripped of direct Kafka connections and user-generation logic (since users are managed by the transactional database).

### 2. Transactional Database & CDC Generator
* **Smart Launcher Wrapper (`run_generator.py`)**: Connects to PostgreSQL and inspects the `users` table:
  * **If empty/uninitialized**: Automatically runs `src/seed_from_csv.py` to seed baseline catalog data (products, distribution centers, and baseline users/orders) and runs `generator.py` with `--init-num-users 1000` to start active simulations.
  * **If data exists**: Skips seeding and runs `generator.py` with `--init-num-users 0` in resume mode to simulate incoming orders for existing users.
* **Baseline Seeder (`src/seed_from_csv.py`)**: Cleans and loads baseline CSV catalog data into PostgreSQL, shifting dates forward (e.g. by 4 years) to keep transaction timestamps current.

---

## ⚙️ Configuration Variables

Configuration is handled through Environment Variables (configured in your local terminal or `docker-compose.yaml`):

| Environment Variable | Component | Description | Default |
| :--- | :--- | :--- | :--- |
| `BFF_URL` | Clickstream | Target URL of the BFF Ingestion Gateway | `http://localhost:8000` |
| `PUBLISH_RATE_HZ` | Clickstream | Clickstream generation rate (events per second) | `10` |
| `BATCH_SIZE` | Clickstream | Number of clickstream events buffered before flushing via HTTP | `10` |
| `PG_HOST` | Database / CDC | PostgreSQL Hostname | `localhost` |
| `PG_PORT` | Database / CDC | PostgreSQL Port | `5434` |
| `PG_DB` | Database / CDC | Database Name | `thelook_db` |
| `PG_USER` | Database / CDC | Database Username | `db_user` |
| `PG_PASSWORD` | Database / CDC | Database Password | `db_password` |
| `PG_SCHEMA` | Database / CDC | Database Schema | `demo` |
| `YEAR_SHIFT` | Database / CDC | Years to shift dates for seeded CSV data | `4` |
| `AVG_QPS` | Database / CDC | Target transactional events generated per second | `5` |

---

## 🚀 Usage Guide

Ensure you have installed the required dependencies from `requirements.txt` and activated your Python virtual environment:
```bash
pip install -r requirements.txt
```

### 1. Running Ingest & Clickstream (HTTP Gateway)
Normally, these run inside Docker containers as configured in `docker-compose.yaml`. To run them locally:

* **Start BFF Ingestion Gateway**:
  ```bash
  uvicorn stream_app:app --host 0.0.0.0 --port 8000
  ```
* **Start Clickstream Simulator**:
  ```bash
  python http_client.py
  ```

### 2. Seeding and Simulating Database Changes (CDC Source)
You can run database tasks using the `Makefile` (Linux/macOS/Git Bash) or PowerShell script (Windows):

* **Smart Start (Recommended)**:
  Checks the database, seeds it if empty, and starts the generator in the correct mode automatically.
  ```bash
  # Using Make
  make gendata
  
  # Using PowerShell
  .\manage_data.ps1 -Action gendata
  ```

* **Force Reset & Start Generator**:
  Force truncates PostgreSQL tables, seeds catalogs, and runs generator starting with 1000 initial users.
  ```bash
  # Using Make
  make reset-seed-gendata
  
  # Using PowerShell
  .\manage_data.ps1 -Action reset-seed-gendata
  ```

* **One-Time Manual Database Seeding**:
  Performs seeding of PostgreSQL from CSV without launching the continuous generator.
  ```bash
  # Using Make
  make seed-snapshot
  
  # Using PowerShell
  .\manage_data.ps1 -Action seed-snapshot
  ```