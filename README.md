<div align="center">

# Real-Time Schema Drift Detection Pipeline

### Kafka-Native · Stateful · Multi-Dataset · Production-Grade

**5 drift types · F1 = 0.955 average · Validated on 4 public datasets**

[![Pipeline](https://github.com/houcem58/realtime-streaming-pipeline/actions/workflows/pipeline.yml/badge.svg)](https://github.com/houcem58/realtime-streaming-pipeline/actions/workflows/pipeline.yml)
[![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-blue)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.x-black)](https://kafka.apache.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen)](docs/Architecture.md)

</div>

---

> A production-grade streaming pipeline that detects schema and statistical drift in real-time
> event streams using Apache Kafka. Validated against four public datasets using a two-level
> evaluation methodology with F1 ≥ 0.92 across all datasets (average 0.955).
>
> Designed for data engineering teams operating at scale where silent schema changes, upstream
> format mutations, and distribution shifts silently corrupt downstream analytics.

---

## The Problem

Modern data platforms ingest events from dozens of upstream producers — IoT sensors, application
logs, payment systems, e-commerce APIs. Any of these can silently change their schema: a column
gets renamed, a numeric type becomes a string, NULL rates spike, or a new category value appears.

Without real-time detection, these changes propagate undetected through the pipeline, corrupting
analytics, breaking ML models, and generating compliance incidents — often discovered days later.

**Traditional approaches fail** because they are either batch-only (too slow), rule-based
(require manual schema maintenance), or produce too many false positives on benign fluctuations.

---

## The Solution

A stateful, micro-batch drift detector that:

- **Fits a statistical baseline** on the first batch of events per stream
- **Detects 5 drift types** per micro-batch with configurable thresholds
- **Routes drift events** to a dedicated Kafka topic with severity, column, and recommended action
- **Calibrates per dataset** using domain-specific parameters — never one-size-fits-all
- **Achieves F1 ≥ 0.92** validated on independent public datasets the detector never trained on

---

## Architecture

```
Real Dataset (NASA / Retail / Olist / HDFS CSV)
    │  Adapter → normalized 20-column event schema
    ▼
DriftInjector (deterministic, seed=42)
    │  Domain-specific perturbations at known windows
    │  Detector is completely blind to injection schedule
    ▼
Kafka Producer ──► streaming.raw_events
                        │
          ┌─────────────┼──────────────────────────┐
          ▼             ▼             ▼             ▼
  drift_events   processed_events  contract_events  metrics
  (detector)     (transformed)     (schema pass/   (throughput
                                    fail)           latency)
          │
          ▼
  DriftDetector (micro-batch, stateful)
    │  fit_baseline() on first batch
    │  detect() on every subsequent batch
    ▼
  DriftEvaluator
    │  detected_windows vs injection_schedule
    ▼
  F1 / Precision / Recall / FPR per dataset
```

---

## Benchmark Results

All results from a single deterministic run (`seed=42`). Thresholds calibrated once per
dataset to account for domain-specific column semantics — see [docs/Calibration.md](docs/Calibration.md).

### Level 1 — Independent Validation (Real Labels)

| Dataset | Source | Windows | F1 | Precision | Recall | FPR |
|---|---|---|---|---|---|---|
| LogHub HDFS | Production Hadoop cluster | 60 | **0.9455** | **1.0000** | 0.8966 | 0.00 |

HDFS uses pre-existing block-level failure labels from [LogHub](https://github.com/logpai/loghub) —
ground truth the detector never sees. Precision = 1.0 means zero false alarms.

### Level 2 — Controlled Injection on Real Data

| Dataset | Domain | Events | Windows | F1 | Precision | Recall | FPR |
|---|---|---|---|---|---|---|---|
| NASA HTTP | Web server logs | 20K | 37 | **1.0000** | 1.0000 | 1.0000 | 0.00 |
| Online Retail II | E-commerce | 20K | 37 | **0.9231** | 0.8571 | 1.0000 | 0.08 |
| Olist E-Commerce | Marketplace | 15K | 27 | **0.9524** | 0.9091 | 1.0000 | 0.06 |

**Methodology:** `DriftInjector` applies perturbations at runtime at known windows. Detector
is fully blind to injection schedule. Ground truth = injection schedule. F1 computed at
window level (binary: any drift event detected = positive).

> Follows established stream drift detection practices:
> [Gama et al., 2014](https://dl.acm.org/doi/10.1145/2523813) ·
> [Evidently AI evaluation guide](https://www.evidentlyai.com/) ·
> [WhyLabs drift methodology](https://whylabs.ai/)

---

## Drift Types Detected

| Type | Detection Method | Example |
|---|---|---|
| `schema_rename` | Missing column + new column appeared | `event_type` → `event_template` |
| `type_drift` | dtype changed incompatibly | numeric column becomes object |
| `missingness_drift` | NULL rate jumped ≥ 5% | `category` NaN rate: 2% → 35% |
| `value_drift` | Unseen category exceeded frequency threshold | `region` = `"SUSPICIOUS_REGION"` |
| `distribution_drift` | Mean shifted > 4σ (continuous) or rate ≥ 2.5× (binary) | `bytes` collapsed to 0 |

**Key design decision:** Binary columns use a rate-based alert — fires only when rate ≥ 2.5×
baseline AND delta ≥ 5%. This prevents a normal 5%→8% fluctuation from being flagged as drift.

---

## Business Value

| Scenario | Without Drift Detection | With This Pipeline |
|---|---|---|
| Schema rename upstream | Analytics break silently | Alert within 1 batch (< 30s) |
| NULL spike in key column | ML model degrades undetected | Missingness alert + severity score |
| New category value | Downstream JOIN fails | Value drift event routed to dead-letter |
| Distribution shift | Reports show incorrect aggregates | Distribution alert + recommended action |

Designed for teams operating data pipelines where silent upstream changes cost hours of
debugging and create compliance risk.

---

## Quick Start

### Standalone (no Kafka required)

```bash
git clone https://github.com/houcem58/realtime-streaming-pipeline.git
cd realtime-streaming-pipeline
pip install -r requirements.txt

# End-to-end demo on HDFS sample data
python scripts/run_demo.py

# HDFS F1 evaluation (real independent labels)
python scripts/eval_drift_detection.py

# Multi-dataset evaluation (NASA / Retail / Olist)
python scripts/eval_kafka_stream.py
```

### With Makefile

```bash
make install     # install dependencies
make test        # run unit tests
make demo        # end-to-end demo
make eval        # full benchmark evaluation
make docker-up   # start Kafka + Zookeeper
```

### Kafka E2E Mode

```bash
make docker-up

# Wait for Kafka to be ready, then:
python kafka/ensure_topics.py
python scripts/eval_kafka_stream.py --kafka

# Optional: Kafka UI at http://localhost:8080
docker compose --profile ui up -d
```

### Custom Dataset Paths

```bash
python scripts/eval_kafka_stream.py \
    --nasa /path/to/nasa_http.csv \
    --retail /path/to/online_retail_ii.csv \
    --olist-orders /path/to/olist_orders_dataset.csv \
    --olist-payments /path/to/olist_order_payments_dataset.csv
```

Or via environment variables:

```bash
export DRIFT_NASA=/data/nasa_http.csv
export DRIFT_RETAIL=/data/online_retail_ii.csv
python scripts/eval_kafka_stream.py
```

---

## Project Structure

```
realtime-streaming-pipeline/
├── streaming/
│   ├── drift_detector.py      # Core detector — 5 drift types, stateful micro-batch
│   ├── executor.py            # StreamingExecutor + KafkaStreamingExecutor
│   ├── sources.py             # CSV, Simulated, and Kafka stream sources
│   └── live_state.py          # In-memory batch and event store
├── adapters/
│   ├── loghub_hdfs_adapter.py # LogHub HDFS log parser + window builder
│   ├── nasa_adapter.py        # NASA HTTP log normalizer
│   ├── retail_adapter.py      # Online Retail II normalizer
│   └── olist_adapter.py       # Olist e-commerce normalizer
├── evaluation/
│   ├── drift_injector.py      # Runtime drift injection (domain-specific, seed=42)
│   ├── metrics.py             # F1, Precision, Recall, FPR — window-level binary
│   ├── kafka_eval.py          # Standalone + Kafka E2E evaluator
│   └── test_metrics.py        # Unit tests for DriftMetrics
├── kafka/
│   ├── ensure_topics.py       # Topic creation utility
│   ├── produce_events.py      # CSV → Kafka with time-replay speedup
│   └── consume_events.py      # Diagnostic consumer
├── scripts/
│   ├── run_demo.py            # End-to-end demo (no Kafka)
│   ├── eval_drift_detection.py # HDFS F1 evaluation (Level 1)
│   └── eval_kafka_stream.py   # 3-dataset evaluation runner (Level 2)
├── config/
│   └── kafka_config.json      # Topic and broker configuration
├── data/
│   ├── events_sample.csv      # LogHub HDFS sample (200 events)
│   ├── window_labels.csv      # 60-window ground truth labels
│   └── schema.json            # Event schema definition
├── docs/
│   ├── Architecture.md        # System design and component diagram
│   ├── Calibration.md         # Per-dataset threshold decisions
│   └── BusinessCase.md        # Use cases and business value
├── Makefile                   # Developer experience shortcuts
├── Dockerfile                 # Single-container image
├── docker-compose.yml         # Kafka + Zookeeper stack
├── requirements.txt
├── CHANGELOG.md
├── CONTRIBUTING.md
├── ROADMAP.md
├── SECURITY.md
└── LICENSE                    # Apache 2.0
```

---

## Technology Stack

| Layer | Technology |
|---|---|
| Stream broker | Apache Kafka 3.x + Zookeeper |
| Drift detection | Custom stateful micro-batch engine |
| Data processing | pandas 3.x, NumPy |
| Evaluation | scikit-learn (F1, confusion matrix) |
| Containerisation | Docker + Docker Compose |
| CI/CD | GitHub Actions (unified pipeline) |
| Language | Python 3.11+ |

---

## Documentation

| Document | Description |
|---|---|
| [Architecture](docs/Architecture.md) | System design, component interactions, Kafka topology |
| [Calibration](docs/Calibration.md) | Per-dataset threshold decisions and methodology |
| [Business Case](docs/BusinessCase.md) | Use cases, ROI context, target environments |
| [Roadmap](ROADMAP.md) | Planned enhancements and future directions |
| [Contributing](CONTRIBUTING.md) | How to contribute, code standards, PR process |
| [Security](SECURITY.md) | Reporting vulnerabilities |
| [Changelog](CHANGELOG.md) | Version history |

---

## Datasets

| Dataset | Source | Size | License |
|---|---|---|---|
| [LogHub HDFS](https://github.com/logpai/loghub) | Production Hadoop cluster | 11M lines | Research use |
| [NASA HTTP](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html) | Kennedy Space Center, 1995 | 1.9M requests | Public domain |
| [Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii) | UCI ML Repository | 1M transactions | CC BY 4.0 |
| [Olist E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) | Brazilian marketplace | 100K orders | CC BY-NC-SA 4.0 |

---

## Kafka Topics

| Topic | Purpose |
|---|---|
| `streaming.raw_events` | Inbound events from producers |
| `streaming.processed_events` | Transformed batch output |
| `streaming.drift_events` | Detected drift events with severity and recommended action |
| `streaming.contract_events` | Schema contract pass/fail per batch |
| `streaming.metrics` | Throughput, latency, drift count per batch |
| `streaming.errors` | Dead-letter queue for failed batches |

---

## Author

**Houcem Hammami** — Technical Manager, AI & Data Engineering

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](https://linkedin.com/in/houcem-hammami)
[![Email](https://img.shields.io/badge/Email-houcem0508%40gmail.com-red)](mailto:houcem0508@gmail.com)

---

## License

Copyright 2025–2026 Houcem Hammami. Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).
