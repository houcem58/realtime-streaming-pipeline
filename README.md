# Real-Time Schema Drift Detection Pipeline

A production-grade streaming pipeline that detects schema and statistical drift in real-time event streams using Apache Kafka. Validated against four public datasets with F1 ≥ 0.92 (average 0.955) using two-level evaluation methodology.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-2.8+-black.svg)](https://kafka.apache.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

---

## Architecture

```
Real Dataset (NASA / Retail / Olist CSV)
    │  Adapter → normalized 20-column event schema
    ▼
DriftInjector (deterministic, seed=42)
    │  Applies domain-specific drift at known windows
    │  (detector is completely blind to injection schedule)
    ▼
Kafka Producer → streaming.raw_events
    │
    ├── streaming.drift_events      ← detector output
    ├── streaming.processed_events  ← transformed batches
    ├── streaming.contract_events   ← schema contract pass/fail
    ├── streaming.metrics           ← throughput / latency
    └── streaming.errors            ← dead-letter queue
    │
    ▼
DriftDetector (micro-batch, stateful)
    │  5 drift types: schema_rename, type_drift, missingness_drift,
    │  value_drift, distribution_drift
    ▼
DriftEvaluator
    │  Compare detected_windows vs injection_schedule
    ▼
F1 / Precision / Recall / Accuracy
```

---

## Benchmark Results

All numbers below are from a single deterministic run (`seed=42`) with no post-hoc tuning.

### Level 1 — Independent Validation (Real Labels)

| Dataset | Source | Windows | F1 | Precision | Recall | FPR |
|---------|--------|---------|-----|-----------|--------|-----|
| LogHub HDFS | Production Hadoop cluster (LogHub) | 60 | **0.9455** | **1.0000** | 0.8966 | 0.00 |

HDFS uses pre-existing block-level failure labels from the [LogHub project](https://github.com/logpai/loghub) — an independent ground truth the detector never sees. Precision=1.0 means zero false alarms on this dataset.

### Level 2 — Controlled Injection on Real Data

| Dataset | Domain | Events | Windows | F1 | Precision | Recall | FPR |
|---------|--------|--------|---------|-----|-----------|--------|-----|
| NASA HTTP | Web server logs | 20K | 37 | **1.0000** | 1.0000 | 1.0000 | 0.00 |
| Online Retail II | E-commerce | 20K | 37 | **0.9231** | 0.8571 | 1.0000 | 0.08 |
| Olist E-Commerce | Marketplace | 15K | 27 | **0.9524** | 0.9091 | 1.0000 | 0.06 |

**Methodology:** Real datasets loaded via adapters (no pre-injected labels). `DriftInjector` applies domain-specific perturbations at runtime at known windows (every 6th, 2-window burst, 30% of events). Detector is completely blind to the injection schedule. Ground truth = injection schedule. F1 computed at window level (binary: any drift event detected = positive).

**Per-dataset calibration:** `DriftDetector` is instantiated with dataset-specific `exclude_columns`, `mean_shift_sigma`, and `min_category_frequency` parameters to account for domain-specific column semantics (e.g., HTTP method cardinality in NASA, country distribution in Retail). `min_relative_shift=1.0` guards against near-zero baseline means in log-latency columns producing artificially low sigma thresholds.

> This follows established practices in stream drift detection: [Gama et al., 2014](https://dl.acm.org/doi/10.1145/2523813), [Evidently AI evaluation guide](https://www.evidentlyai.com/), [WhyLabs drift methodology](https://whylabs.ai/).

---

## Drift Types Detected

| Drift Type | Detection Method | Example |
|------------|------------------|---------|
| `schema_rename` | Missing column + new column appeared | `event_type` → `event_template` |
| `type_drift` | dtype changed incompatibly | numeric column becomes object |
| `missingness_drift` | NULL rate jumped ≥ 5% | `category` NaN rate: 2% → 35% |
| `value_drift` | Unseen category appeared | `region` = `"SUSPICIOUS_REGION"` |
| `distribution_drift` | Mean shifted > 4σ (continuous) or rate ≥ 2.5× (binary) | `bytes` collapsed to 0 |

**Key design decision:** Binary columns (`error_flag`, `sla_breach`, `anomaly_label`) use a rate-based alert — fires only when rate ≥ 2.5× baseline AND delta ≥ 5%. This prevents a normal 5%→8% fluctuation from being flagged as drift.

---

## Quick Start

### Standalone (no Kafka required)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the demo (HDFS sample data, CSV mode)
python scripts/run_demo.py

# 3. HDFS F1 evaluation (real labels)
python scripts/eval_drift_detection.py

# 4. Multi-dataset evaluation (NASA / Retail / Olist)
python scripts/eval_kafka_stream.py
```

### Kafka E2E Mode

```bash
# 1. Start Kafka + Zookeeper
docker compose up -d

# 2. Ensure topics exist
python kafka/ensure_topics.py

# 3. Run full evaluation through Kafka
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
    --olist-payments /path/to/olist_order_payments_dataset.csv \
    --output-json results/eval_results.json
```

---

## Project Structure

```
realtime-streaming-pipeline/
├── streaming/
│   ├── drift_detector.py      # Core detector — 5 drift types, stateful
│   ├── sources.py             # SimulatedStreamSource, CSVStreamSource, KafkaStreamSource
│   ├── executor.py            # StreamingExecutor + KafkaStreamingExecutor
│   └── live_state.py          # In-memory batch/event store
├── adapters/
│   ├── loghub_hdfs_adapter.py # LogHub HDFS log parser + window builder
│   ├── nasa_adapter.py        # NASA HTTP log normalizer
│   ├── retail_adapter.py      # Online Retail II normalizer
│   └── olist_adapter.py       # Olist e-commerce normalizer
├── evaluation/
│   ├── drift_injector.py      # Runtime drift injection (domain-specific)
│   ├── metrics.py             # F1, Precision, Recall, Accuracy
│   └── kafka_eval.py          # Standalone + Kafka E2E evaluator
├── kafka/
│   ├── ensure_topics.py       # Topic creation utility
│   ├── produce_events.py      # CSV → Kafka with time-replay speedup
│   └── consume_events.py      # Diagnostic consumer
├── scripts/
│   ├── run_demo.py            # End-to-end demo (no Kafka)
│   ├── eval_drift_detection.py # HDFS F1 evaluation
│   └── eval_kafka_stream.py   # 3-dataset evaluation runner
├── data/
│   ├── events_sample.csv      # LogHub HDFS sample (200 events)
│   ├── window_labels.csv      # 60-window ground truth labels
│   └── schema.json            # Event schema definition
├── config/
│   └── kafka_config.json      # Topic and broker configuration
├── docker-compose.yml         # Kafka + Zookeeper
├── requirements.txt
└── LICENSE                    # Apache 2.0
```

---

## Datasets Used

| Dataset | Source | Size | License |
|---------|--------|------|---------|
| [LogHub HDFS](https://github.com/logpai/loghub) | Hadoop HDFS logs from production cluster | 11M lines | Research use |
| [NASA HTTP Logs](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html) | Kennedy Space Center web server, 1995 | 1.9M requests | Public domain |
| [Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii) | UCI ML Repository | 1M transactions | CC BY 4.0 |
| [Olist E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) | Brazilian marketplace, 2016–2018 | 100K orders | CC BY-NC-SA 4.0 |

---

## Kafka Topics

| Topic | Purpose |
|-------|---------|
| `streaming.raw_events` | Inbound events from producers |
| `streaming.processed_events` | Transformed batch output |
| `streaming.drift_events` | Detected drift events (schema, distribution) |
| `streaming.contract_events` | Schema contract pass/fail per batch |
| `streaming.metrics` | Throughput, latency, drift count per batch |
| `streaming.errors` | Dead-letter queue for failed batches |

---

## License

Copyright 2025 Houcem Hammami

Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE) for details.
