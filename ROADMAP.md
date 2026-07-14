# Roadmap

## Current Status — v1.2.0

The pipeline detects 5 drift types across 4 validated public datasets with F1 = 0.955 average.
Architecture, documentation, and CI/CD are production-grade.

---

## Near-Term (v1.3.x)

### Drift Severity Scoring
Currently drift events carry a boolean flag. The next iteration will add a continuous severity
score (0.0–1.0) based on effect size (Cohen's d for continuous columns, relative rate change
for binary). This enables downstream systems to prioritise alerts.

### Schema Registry Integration
Add a `schema_contract.json` per dataset and validate every batch against it.
Violations route to `streaming.contract_events` with the specific constraint that failed.

### Window-Level Drift Report
Add a summary report at end of evaluation run: per-column drift frequency, most-drifted
columns, time-distribution of detections.

---

## Medium-Term (v2.x)

### Adaptive Thresholds
Replace fixed σ thresholds with rolling-window baselines that adapt to gradual drift
without losing sensitivity to sudden changes. Inspired by ADWIN ([Bifet & Gavaldà, 2007](https://link.springer.com/chapter/10.1007/978-3-540-75488-6_22)).

### REST API for Drift Events
Add a FastAPI service that exposes drift event history, per-column statistics, and a
`/health` endpoint for integration with monitoring stacks.

### Additional Dataset Adapters
- Taxi trip records (NYC TLC)
- IoT sensor data (UCI Appliances Energy)
- Financial time-series (Yahoo Finance)

### Grafana + Prometheus Dashboard
Emit metrics from `streaming.metrics` to a Prometheus-compatible endpoint and include
a Grafana dashboard template for real-time drift monitoring.

---

## Long-Term (v3.x)

### Multivariate Drift Detection
Current detection operates per-column. A multivariate mode would detect correlated
shifts that are invisible column-by-column (e.g., a covariance structure change).

### Plug-in Architecture
Refactor `DriftDetector` to accept registered drift-type plug-ins, enabling external
contributors to add detection methods without modifying core code.

### Cloud-Native Deployment Template
Terraform modules for deploying on AWS MSK + Lambda or GCP Dataflow.

---

## Won't Do

- Real-time model retraining (out of scope — this is a detection layer, not MLOps)
- Proprietary dataset adapters (all datasets must be publicly available for reproducibility)
