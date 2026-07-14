# Business Case

## Problem Statement

Data engineering teams operating event-driven platforms face a persistent challenge: upstream
schema changes propagate silently through the pipeline before they are detected.

Common failure modes:

- A microservice renames a field. Analytics break. The issue is discovered in next morning's report.
- A payment provider changes a numeric column to string. An ML model starts returning null predictions.
- NULL rates spike in a key dimension. Aggregations undercount. Finance reports the wrong revenue.
- A new category value appears. A downstream `CASE` statement hits an unmatched branch.

In each case, the cost is not just the bug — it is the time-to-detect, the audit trail,
the manual investigation, and the downstream trust erosion.

## Target Environments

This pipeline addresses three primary deployment scenarios:

**1. Real-Time Data Platform Monitoring**
Data engineering teams running Kafka-based platforms who need automated schema governance
without manual contract maintenance per-topic.

**2. ML Feature Pipeline Quality**
ML engineering teams where feature drift silently degrades model performance. The pipeline
can be placed upstream of feature stores to catch distribution shifts before they reach models.

**3. Regulatory Compliance Pipelines**
Financial services, healthcare, and telecoms teams where data format changes must be
detected, logged, and routed for compliance review before downstream processing.

## Quantified Value

| Metric | Benchmark |
|---|---|
| Average F1 across 4 datasets | 0.955 |
| False alarm rate (FPR) | 0.00–0.08 (dataset-dependent) |
| Detection latency | Within 1 micro-batch (< 30s typical) |
| Datasets validated | 4 (log, web, retail, marketplace) |
| Drift types detected | 5 (schema, type, missingness, value, distribution) |

## Comparison to Alternatives

| Approach | Limitation |
|---|---|
| Manual schema contracts | Requires manual maintenance; breaks on every upstream change |
| Batch data quality checks | 24h+ detection latency |
| Generic ML drift tools | Require labelled data; not schema-aware |
| This pipeline | Real-time, schema-aware, configurable, no labelled data required |
