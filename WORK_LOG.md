# SignalPath Development Log

---

## [2026-04-07 17:45] Project Initialization & Strategic Strategy
**Change Description**: Initialized SignalPath (formerly RailStream-5000) for real-time telemetry processing.
**Rationale**: Need to demonstrate Kafka/Spark streaming capabilities for high-throughput industrial IoT.
**Challenge/Learning**: High-velocity data needs strict schema enforcement to prevent downstream Lakehouse corruption.

### Strategic Notes (Internal Only):
To excel in a distributed systems interview, this project demonstrates:
1. **Schema Enforcement**: Uses Avro to prove Contract-Driven Development.
2. **Stateful Processing**: Windowed averages for "Bearing Temperatures" show deep Spark internals knowledge.
3. **Fault Tolerance**: Spark's .checkpointLocation() used for resilience.
4. **Interview Script**: "I architected a real-time telemetry pipeline for high-throughput sensor data. I utilized Kafka with a Schema Registry to ensure data governance, then used Spark Structured Streaming to process windowed aggregates with Exactly-Once semantics."

---

## [2026-04-08 08:30] Apple DSI Strategic Alignment (Platform Pivot)
**Change Description**: Architected a Semantic Layer (`semantic_layer.py`) and shifted project narrative toward Data Mesh.
**Rationale**: Apple's Data Solutions & Initiatives team values platform engineers who build "Trusted Data Products" rather than just pipelines.
**Challenge/Learning**: Defining "Semantic Richness"—learned that surfacing metadata (e.g., `maintenance_status` flags) is key for ML consumers.

### Strategic Notes (Internal Only):
To excel in the Apple DSI interview:
1. **Data Mesh**: Position SignalPath as a "Mesh Node." It doesn't just store data; it provides an interface (the Semantic Layer) that other teams (AI/ML) can consume without knowing Spark internals.
2. **Iceberg Narrative**: If asked about Iceberg vs. Delta: *"I used Delta Lake locally for ACID compliance, but the architecture is Lakehouse-agnostic. We could swap the storage layer for Apache Iceberg (Apple's standard) without changing the Kafka/Spark core."*
3. **Owner/Impact**: Emphasize "Ownership" of the whole stack—from raw telemetry to the semantic data product.

---

## [2026-04-10 14:50] Foundation for Real-Time Experimentation & PR Excellence
**Change Description**: Implemented a production-grade A/B Testing Framework and integrated Staff-level optimizations from Copilot Review.
**Rationale**: Demonstrates the ability to not just build a pipeline, but a *platform* for hypothesis testing—a critical skill for Senior/Staff roles at Apple and EA.
**Challenge/Learning**: Realized that `append` mode in windowed streaming adds significant latency; moved to `update` mode to ensure "Safe-to-Fail" experimentation.

### Strategic Notes (Internal Only):
1. **Experimentation Backbone**: We implemented deterministic hashing (`md5`) for variant allocation. In interviews, explain how this prevents "cross-contamination" of experiment data.
2. **Safety-First Alerting**: Based on the Copilot Review, we prioritized `max_temp` over `avg_temp`. This is the perfect talking point for "Operational Excellence"—knowing when a simple average masks a catastrophic failure.
3. **PR Standard**: We codified the PR Summary to highlight "Analytical & Business Impact." This shows you communicate with downstream stakeholders (DS/Product), not just writing code in a vacuum.
4. **Interview Script**: "I architected a live experimentation framework within our Spark streaming pipeline. I ensured deterministic variant allocation via hashing and optimized for low-latency alerting using Spark's 'update' mode. This allowed us to A/B test our predictive maintenance models in real-time without impacting system stability."
