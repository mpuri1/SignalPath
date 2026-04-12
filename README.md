# SignalPath: Real-Time Fleet Telemetry & Semantic Mesh

A high-fidelity, event-driven data platform for industrial fleet telemetry (GPS, Speed, Bearing Temp), architected as a **Data Mesh** node using **Kafka**, **Spark Streaming**, and a **Semantic Layer**.

## 🏗 Architecture
1. **Producer (`src/telemetry_producer.py`)**: Simulates high-throughput synthetic fleet events.
2. **Processor (`src/stream_processor.py`)**: A PySpark Structured Streaming job that performs windowed aggregations and writes to **Delta Lake** (Medallion Architecture).
3. **Semantic Layer (`src/semantic_layer.py`)**: Transforms Silver-layer telemetry into **Trusted Data Products** (Gold semantic models) optimized for AI/ML consumption.
4. **Orchestration (`dags/rail_maintenance_dag.py`)**: An Airflow DAG orchestrating daily maintenance audits across the mesh.

## 🧪 Experimentation Infrastructure (A/B Testing)
SignalPath features a built-in **Experimentation Backbone** allowing engineers to test new predictive maintenance algorithms side-by-side:
- **Deterministic Assignment**: Assets are consistently assigned to `CONTROL` or `TREATMENT` groups via hashing.
- **Side-by-Side Execution**: The Spark processor evaluates multiple models in parallel for the same telemetry stream.
- **Metric Attribution**: The Semantic Layer generates a **Variant Performance Report**, comparing alert accuracy and lead times between models.

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

