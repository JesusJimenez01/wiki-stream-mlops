Note: This repository is a consolidated, refactored, and translated version of a project originally developed during my AI & Big Data specialization. The codebase has been cleaned for portfolio demonstration purposes.

# WIKI STREAM MLOPS

A real-time data platform that transforms Wikipedia edits into automatically generated news stories in English.

One-line summary:

**Wikipedia SSE -> Redpanda -> Spark (Bronze/Silver/Gold) -> MongoDB -> FastAPI/Web -> Prometheus/Grafana**

## Portfolio Snapshot

* End-to-end real-time pipeline with clear layer separation.
* Spark + Delta Lake + MinIO for durable processing.
* AI newsroom generated locally with Ollama.
* FastAPI web app and Grafana observability ready for demos.
* Data quality checks for Silver and Gold.

---

## 1. Problem Statement

This project implements an automated newsroom using a medallion lakehouse architecture:

* **Ingestion** of real-time events from Wikipedia.
* **Editorial curation** to remove technical noise (bots, minor changes, reverts, namespace noise).
* **AI News Generation** (headline, summary, and tags).
* **Web/API Serving** for human consumption and business observability.
* **Full observability** across the pipeline, infrastructure, and inference quality.

Academic goal: to demonstrate an end-to-end Big Data flow focusing on data quality, operations, and business value.

---

## 2. Layered Architecture

### 2.1 Layer View

1. **Ingestion**
   * `producer/producer.py` listens to Wikipedia SSE and publishes to Redpanda.

2. **Bronze (raw)**
   * `spark-jobs/bronze_ingestion.py` persists raw events to Delta Lake (MinIO).

3. **Silver (curation + topic discovery)**
   * `spark-jobs/silver_processing.py` cleans, normalizes, and implements an analytics layer to select publishable topics.
   * Generates `silver/wiki_clean` and `silver/wiki_topics`.

4. **Gold (AI newsroom)**
   * `spark-jobs/gold_enrichment.py` consumes Silver topics and calls Ollama.
   * Applies deduplication, update control, and story lifecycle management.
   * Generates `gold/wiki_news` and inference metrics.

5. **Serving**
   * `spark-jobs/mongo_serving.py` publishes to MongoDB with idempotent upserts.
   * Main collection: `wikipedia.news`.

6. **Consumption and Observability**
   * `newsroom-api/app.py` exposes web, API, and metrics.
   * Prometheus + Grafana + exporters (host/containers/GPU).

### 2.2 E2E Data Flow

1. Wikipedia event enters via SSE.
2. Published to Redpanda.
3. Bronze stores it retaining full historical detail.
4. Silver filters noise and builds candidate topics.
5. Gold drafts the news and manages live story updates.
6. Serving publishes to MongoDB for the web/API.
7. Grafana displays technical health and product KPIs.

---

## 3. Components and Technologies

* **Local Orchestration:** Docker Compose
* **Streaming Bus:** Redpanda
* **Processing:** Apache Spark Structured Streaming
* **Lakehouse:** MinIO + Delta Lake
* **Local AI:** Ollama (`qwen3:8b`)
* **Serving:** MongoDB + FastAPI
* **Observability:** Prometheus + Grafana + Node Exporter + cAdvisor + DCGM Exporter

---

## 4. Quick Start

### 4.1 Startup

From the `wiki-stream-mlops` folder:

1. Create your environment file from the template:
```bash
cp .env.example .env
```

2. Start the infrastructure (it will build images if necessary):
```bash
docker compose up -d
```

3. Download the AI model into the Ollama container (only required the first time):
```bash
docker exec -it wiki-ollama ollama pull qwen3:8b
```
The model stays in the Ollama data volume, so it is preserved if you only remove the data volumes below.

### 4.2 Main Endpoints

* Web: `http://localhost:8085`
* News API: `http://localhost:8085/api/news?limit=25`
* Stats API: `http://localhost:8085/api/stats`
* Metrics: `http://localhost:8085/metrics`
* Grafana: `http://localhost:3000`
* Prometheus: `http://localhost:9090`
* Redpanda Console: `http://localhost:8888`
* Spark UI (master): `http://localhost:8080`
* MinIO Console: `http://localhost:9001`

### 4.3 Clean Data Reset (Keeping AI model)

```bash
docker compose down --remove-orphans
docker volume rm $(docker volume ls -q | grep -E '_?(redpanda_data|minio_data|mongo_data|prometheus_data|grafana_data)$')
docker compose up -d --build
```

This reset **does not** delete the `*_ollama_models` volume.

---

## 5. Recommended Configuration

1. Copy `.env.example` to `.env`.
2. Keep `SILVER_EVENT_HOLD_MINUTES=5` as a stable baseline.
3. Adjust only if necessary:
   * volume (`SILVER_TOPIC_TOP_N`, `GOLD_MAX_EVENTS_PER_BATCH`)
   * editorial rigor (`SILVER_TOPIC_MIN_DOC_FREQ`, Gold deduplication thresholds)
   * model (`OLLAMA_MODEL`)

Note: in `newsroom-api`, the feed prioritizes pieces with valid inference and keeps obvious noise off the front page.

---

## 6. Quality and Validations

### 6.1 Silver Quality Check (Data)

```bash
docker exec wiki-spark-silver /opt/spark/bin/spark-submit --master local[1] /opt/tests/quality_check.py
```

Expected:

* `SILVER_BAD_BOT_OR_MINOR=0`
* `SILVER_BAD_NORMALIZATION=0`
* `SILVER_MISSING_EDITORIAL_SIGNALS=0`
* `SILVER_BAD_EDITORIAL_TOPIC=0`

### 6.2 Gold Quality Check (AI)

```bash
docker exec wiki-spark-gold /opt/spark/bin/spark-submit --master local[1] /opt/tests/gold_quality_report.py
```

Strict version:

```bash
docker exec -e GOLD_REQUIRE_DATA=true -e GOLD_MIN_SUCCESS_RATE=0.80 wiki-spark-gold /opt/spark/bin/spark-submit --master local[1] /opt/tests/gold_quality_report.py
```

### 6.3 MongoDB Check

```bash
docker exec wiki-mongodb mongosh -u wikipedia -p wikipedia123 --authenticationDatabase admin --quiet --eval "db.getSiblingDB('wikipedia').news.find({}, { _id: 1, timestamp: 1, topic_term: 1, headline: 1, update_seq: 1 }).sort({ timestamp: -1 }).limit(10).toArray()"
```

---

## 7. Dashboards and Observability

The main Grafana dashboard is automatically provisioned and combines technical and product KPIs.

### 7.1 What to Monitor

1. **Pipeline Health**
   * Difference between `inputRate` and `processingRate` in Spark.
   * Throughput and Redpanda state (`/public_metrics`).

2. **Inference Quality**
   * `inference_ok` ratio.
   * Age of the latest published news.

3. **Resources**
   * Host: CPU/RAM/disk (Node Exporter).
   * Containers: CPU/RAM/network per service (cAdvisor).
   * GPU: usage and VRAM (DCGM Exporter).

4. **Serving Layer**
   * Latencies and request rates in `newsroom-api`.

### 7.2 Recommended Tracking KPIs

* `inference_ok >= 0.98`
* API p95 latency within SLO
* Continuous publishing without sustained backlog
* Low ratio of discarded pieces in manual QA

---

## 8. Functional Outcome Analysis

### 8.1 Strengths

* Complete real-time and decoupled pipeline by layers.
* Good technical quality control in Silver and stability in serving.
* Sufficient observability to operate and troubleshoot.
* Idempotent and traceable publishing (`story_id:update_seq`).

### 8.2 Known Risks

* In low-signal events (categories/metadata), some news might sound generic.
* Occasional multilingual noise in extreme contexts.
* Real production costs sensitive to egress, retention, and telemetry.

### 8.3 Recommended Mitigations

* Minimum editorial threshold before publishing.
* Unicode normalization + language detection + exclusion rules.
* Prompt/model A/B testing and continuous evaluation.
* End-to-end linguistic quality test suite.

---

## 9. Path to Production

### 9.1 Target Architecture (Enterprise)

* Managed Kubernetes: Amazon EKS
* Managed Kafka: Amazon MSK or Confluent Cloud
* Lakehouse: S3 + Delta Lake
* Serving: MongoDB Atlas + EKS API
* Observability: Grafana Cloud / Managed Prometheus
* Inference: GPU workers with autoscaling

### 9.2 Reference Costs

Assumptions:

* Operations in EU region (e.g. `eu-south-2` or `eu-west-1`).

Guiding Scenarios:

1. **Managed Baseline (MSK + EKS + Atlas + Grafana)**
   * Without VAT: `~$750/month`
   * With 21% VAT: `~$900/month`

2. **Medium (baseline + dedicated processing compute)**
   * Without VAT: `~$1,800/month`
   * With 21% VAT: `~$2,175/month`

3. **Kafka Serverless Alternative (Confluent Standard)**
   * Without VAT: `~$530/month`
   * With 21% VAT: `~$640/month`

### 9.3 Production Limitations and Closure Plan

1. **Variable editorial quality in low signal**
   * Action: minimum score + withhold publishing if threshold is unmet.

2. **Multilingual/transliteration noise**
   * Action: normalization pipeline and blacklist of non-publishable patterns.

3. **Single model dependency**
   * Action: multi-model strategy with fallback.

4. **Improvable text tests**
   * Action: automated validations for headline/summary/readability.

5. **Cost overrun risk**
   * Action: early FinOps (budgets, alerts, and weekly reviews).

### 9.4 Suggested Go-Live Criteria

* Sustained `inference_ok >= 0.98`
* Less than 5% editorial rejection on a daily sample
* API p95 latency within SLO
* Budget deviation <= 15% across two billing cycles

---

## 10. Pricing Sources

* AWS EKS Pricing: https://aws.amazon.com/eks/pricing/
* AWS MSK Pricing: https://aws.amazon.com/msk/pricing/
* AWS EC2 On-Demand Pricing: https://aws.amazon.com/ec2/pricing/on-demand/
* AWS S3 Pricing: https://aws.amazon.com/s3/pricing/
* MongoDB Atlas Pricing: https://www.mongodb.com/pricing
* Grafana Cloud Pricing: https://grafana.com/pricing/
* Confluent Cloud Pricing: https://www.confluent.io/confluent-cloud/pricing/

---

## 11. Project Structure (Overview)

* `producer/` SSE ingestion -> Redpanda
* `spark-jobs/` bronze, silver, gold, and serving
* `newsroom-api/` web, API, and metrics
* `observability/` Prometheus + Grafana provisioning
* `tests/` data quality and gold validations

---

## 12. Screenshots

### Web UI

![Wikipedia Web UI](screenshots/web.png)

### Business dashboard

![Wikipedia Business dashboard](screenshots/dashboard_business.png)

### Infrastructure dashboard

![Wikipedia Infrastructure dashboard 1](screenshots/dashboard_infraestructure1.png)

![Wikipedia Infrastructure dashboard 2](screenshots/dashboard_infraestructure2.png)

### Pipeline dashboard

![Wikipedia Pipeline dashboard](screenshots/dashboard_pipeline.png)
