# RailStream-5000: Architecture & Interview Strategy

This project is a high-fidelity demonstration of **Real-Time Data Engineering** designed to satisfy the "Basic Qualifications" of the BNSF Senior/Staff Data Engineer role.

## 1. Core Component Stack

| Component | Technology | Role | BNSF Alignment |
| :--- | :--- | :--- | :--- |
| **Ingestion** | **Apache Kafka** | Real-time event broker (Confluent Cloud). | **High** (Basic Qualification) |
| **Processing** | **Spark Structured Streaming** | Micro-batch processing & windowing. | **High** (Basic Qualification) |
| **Storage** | **Delta Lake** | ACID-compliant storage on S3/Local. | **High** (Lakehouse Pattern) |
| **Governance** | **Schema Registry (Avro)** | Enforcing contract between producer/consumer. | **Staff Level** (Data Quality) |
| **Orchestration** | **Airflow** | Managing batch-backfill or cleanup DAGs. | **Medium** (Optional) |

---

## 2. Project Organization

```text
railstream_5000/
├── config/
│   ├── settings.py          # Confluent Cloud credentials & Kafka topics
│   └── schema.avsc          # Avro schema definition (Governance)
├── src/
│   ├── producer.py          # Telemetry simulator (GPS, Speed, Sensors)
│   ├── processor.py         # Spark Structured Streaming logic
│   └── common/
│       └── utils.py         # Shared logger, error handler
├── tests/                   # Unit tests for transformation logic
├── data/                    # Local storage for Delta Lake tables
├── README.md                # Architectural diagrams & setup
└── RailStream_Architecture.md # This strategy document
```

---

## 3. High-Impact "Staff" Patterns

To excel in the BNSF interview, this project should demonstrate more than just "moving data." It should show:

1.  **Schema Enforcement**: Use Avro in your Producer to prove you understand **Contract-Driven Development**. If the producer sends bad data, the project should fail gracefully, not corrupt the lake.
2.  **Stateful Processing**: Implement a "Geofence Violation" logic where Spark remembers the last 5 minutes of a train's location to detect if it entered a restricted zone.
3.  **Idempotence & Checkpointing**: Use Spark's `.checkpointLocation()` to show how you handle **Fault Tolerance**. If the job crashes, it should pick up exactly where it left off.
4.  **The "Trino" Question**: 
    *   **Is it required?** No, BNSF JD focuses on **Snowflake** and **Spark**. 
    *   **Is it helpful?** Trino is excellent for "Federated Queries" (reading from Delta Lake + a relational DB). If you want to show off, you could use Trino to query your Delta tables, but for BNSF, **Spark SQL** is their primary query engine.

---

## 4. Interview Defense Strategy

**Recruiter**: "Tell me about your experience with Kafka."
**Answer**: "For the RailStream project, I architected a real-time telemetry pipeline. I used **Kafka** with an **Avro Schema Registry** to ensure data governance. I then utilized **Spark Structured Streaming** to perform windowed aggregations and detect speed violations across 11M+ (simulated) location updates, ensuring **Exactly-Once** semantics via checkpointing."

**Why this works**: It tells a story of **Architecture, Governance, and Scale**—the three markers of a Staff Engineer.
