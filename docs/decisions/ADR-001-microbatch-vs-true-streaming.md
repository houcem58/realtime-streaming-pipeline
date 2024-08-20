# ADR-001 — Micro-batch Processing vs. True Streaming

**Status:** Accepted  
**Date:** 2025-08-01  
**Author:** Houcem Hammami  
**Reviewers:** —  

---

## Context

The drift detection system must process event streams from four heterogeneous datasets
(HDFS server logs, NASA HTTP access logs, Online Retail II transactions, Olist e-commerce orders)
and emit structured drift events with sub-second latency targets under normal load.

Two primary processing models were evaluated:

**Option A — True record-by-record streaming**  
Each event is processed independently as it arrives. Drift is detected per-event by comparing
the incoming record against a running statistical baseline.

**Option B — Micro-batch windowed processing**  
Events are accumulated into fixed-size windows (configurable, default: 500 events per batch).
Drift is detected by comparing the batch's statistical profile against a fitted baseline.

---

## Decision

**Adopted: Option B — Micro-batch processing.**

---

## Rationale

### Statistical validity requirements

Drift detection algorithms (KS test, chi-squared test, sigma-based threshold comparison)
require a minimum sample size to produce statistically valid results. Running detection on
individual records produces high false positive rates because the sample variance of a
single observation is undefined or infinite.

A batch of ≥ 50 events provides enough power to distinguish genuine distributional shifts
from natural sampling noise. The chosen default window of 500 events yields good statistical
power across all four dataset domains while keeping latency below 2 seconds at the expected
ingestion throughput of 1,000 events/second.

### Alignment with operational use case

The primary failure mode this system guards against is **data pipeline drift** — the gradual
or sudden change in schema, value distribution, or missingness patterns in operational data
feeds. These phenomena manifest over minutes to hours, not milliseconds. A 500-event window
(~0.5 seconds at 1k events/s) is more than sufficient to detect the relevant failure modes
before downstream systems ingest corrupted data.

### Evaluation reproducibility

Micro-batch processing allows deterministic replay against CSV fixtures, enabling the
`DriftMetrics` evaluation framework to produce reproducible benchmark results (F1, Precision,
Recall, FPR per dataset). True streaming requires real-time Kafka infrastructure for every
evaluation run, which conflicts with the goal of lightweight, reproducible benchmarks.

### Implementation complexity

Per-event streaming requires stateful computation with complex watermarking, out-of-order
event handling, and exactly-once semantics — appropriate for Flink or Spark Streaming
deployments. The current scope (single-node drift detection with optional Kafka integration)
does not justify this operational overhead.

---

## Consequences

**Positive:**
- Statistically valid drift detection with configurable confidence
- Reproducible evaluation framework (CSV-based, no Kafka dependency)
- Simple executor model: `StreamingExecutor` and `KafkaStreamingExecutor` share the same detector interface
- Clear separation between evaluation mode and Kafka E2E mode

**Negative:**
- Latency is bounded below by window accumulation time (~0.5 s at 1k events/s)
- Not suitable for per-event SLA requirements < 100 ms
- Window boundary effects: a drift event that straddles two batches may be attributed to either

**Mitigation:**
- Window size is a configuration parameter, not a constant — operators can reduce it for latency-sensitive deployments at the cost of higher false positive rates
- The Calibration.md document records the per-dataset threshold tuning that compensates for window boundary effects

---

## Alternatives Rejected

| Alternative | Rejection reason |
|---|---|
| Apache Flink micro-batch | Operational overhead unjustified at current scale; adds JVM dependency |
| Spark Structured Streaming | Same; also requires cluster infrastructure not available locally |
| Per-event detection with EWMA | No minimum-sample guarantee; EWMA hyperparameter tuning required per dataset |
| Tumbling windows (time-based) | Sparse datasets produce variable-size windows that break statistical tests |

---

## Review Trigger

This decision should be revisited if:
- Throughput exceeds 100k events/second (window accumulation latency becomes problematic)
- A per-event SLA < 100 ms is introduced
- The system is migrated to a managed streaming platform (Flink, Spark, Beam)
