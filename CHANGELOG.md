# Changelog

All notable changes to this project are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versions follow [Semantic Versioning](https://semver.org/).

---

## [1.2.0] — 2026-01-20

### Added
- `docs/Architecture.md` — full system design, component diagram, Kafka topology
- `docs/Calibration.md` — per-dataset threshold decisions with methodology rationale
- `docs/BusinessCase.md` — use cases, target environments, comparison to alternatives
- `CONTRIBUTING.md` — contributor guide, code standards, adapter extension guide
- `SECURITY.md` — vulnerability reporting process
- `ROADMAP.md` — future directions and planned enhancements
- `Makefile` — developer experience shortcuts (`make test`, `make eval`, `make docker-up`)
- `.pre-commit-config.yaml` — ruff + trailing whitespace + YAML validation hooks
- `.dockerignore` — exclude dev artifacts from Docker build context

### Changed
- README restructured with manager-level narrative (The Problem, The Solution, Business Value)
- README now includes Author section and Documentation index
- Benchmark tables clarified: Level 1 (real labels) vs Level 2 (controlled injection)
- Pipeline badge added at top of README

---

## [1.1.0] — 2025-12-15

### Added
- Unified `pipeline.yml` — replaces separate ci.yml + cd.yml with sequential job pipeline
- `concurrency` group to cancel stale runs on push
- Docker image publish to GHCR on main push

### Changed
- Moved from `workflow_run` trigger to direct `push: branches: [main]`

### Fixed
- CD was always "skipped" due to `workflow_run` completing even on CI failure
- `import os` missing in `scripts/eval_kafka_stream.py` (caused F821 lint failure)

---

## [1.0.0] — 2025-11-10

### Added
- `DriftDetector` — 5 drift types: schema_rename, type_drift, missingness_drift, value_drift, distribution_drift
- `DriftInjector` — deterministic runtime drift injection for controlled evaluation (seed=42)
- `DriftMetrics` — window-level binary classification: F1, Precision, Recall, FPR, FNR
- Dataset adapters: LogHub HDFS, NASA HTTP, Online Retail II, Olist E-Commerce
- `StreamingExecutor` and `KafkaStreamingExecutor` — two-mode execution layer
- Kafka topic schema: raw_events, processed_events, drift_events, contract_events, metrics, errors
- Evaluation methodology: Level 1 (real labels) on HDFS, Level 2 (controlled injection) on 3 datasets
- Benchmark results: F1 = 0.955 average across 4 datasets
- Sample data: 200-event HDFS sample + 60-window ground truth labels
- Dockerfile and docker-compose.yml for Kafka + Zookeeper stack
- Apache 2.0 license
