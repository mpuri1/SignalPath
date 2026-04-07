# AuraRail: Real-Time Train Telemetry

A high-fidelity, event-driven data platform simulating BNSF-style train telemetry (GPS, Speed, Bearing Temp) using **Kafka**, **Spark Streaming**, and **Airflow**.

## 🏗 Architecture
1. **Producer (`src/telemetry_producer.py`)**: Simulates synthetic train events and publishes to a Kafka topic. Supports a "Mock Mode" for local testing if credentials are missing.
2. **Processor (`src/stream_processor.py`)**: A PySpark Streaming job that consumes from Kafka, calculates 10-minute windowed averages for bearing temperatures, and writes to **Delta Lake** (Silver/Gold layers).
3. **Orchestration (`dags/rail_maintenance_dag.py`)**: An Airflow DAG that schedules daily maintenance audits to scan for sensor anomalies in the Gold table.

## 🚀 Getting Started

### 1. Prerequisites
- Python 3.12+
- Apache Spark 3.5.0+ (Installed via `pyspark` dependency)
- A Confluent Cloud account (Free Tier)

### 2. Configuration
Copy `.env.template` to `.env` and fill in your Confluent Cloud credentials:
```bash
cp .env.template .env
```

### 3. Running Simulation (Mock Mode)
To verify the telemetry generator without a Kafka cluster:
```bash
python src/telemetry_producer.py
```

### 4. Production Run
Once Kafka credentials are set:
1. **Start the Producer**: `python src/telemetry_producer.py`
2. **Start the Processor**: `python src/stream_processor.py`
3. **Verify Data**: Check `./data/silver/telemetry_raw` for raw events and `./data/gold/telemetry_alerts` for maintenance flags.

## 🛠 Tech Stack
- **Streaming**: Confluent Cloud (Kafka)
- **Processing**: PySpark + Delta Lake
- **Orchestration**: Apache Airflow
- **Environment**: UV / Python 3.12
