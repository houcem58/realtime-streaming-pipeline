# Runbook — Realtime Streaming Pipeline

This document covers normal operation, common failure modes, and recovery procedures.

---

## Quick Reference

| Command | Purpose |
|---|---|
| `make docker-up` | Start Kafka + Zookeeper |
| `make eval` | Run full evaluation suite (CSV mode, no Kafka) |
| `python scripts/eval_kafka_stream.py --kafka` | Run Kafka E2E evaluation |
| `python kafka/ensure_topics.py` | Create/verify all Kafka topics |
| `make test` | Run unit tests |
| `make lint` | Run ruff linter |
| `docker compose logs kafka` | Stream Kafka logs |

---

## Normal Operations

### Daily evaluation run

```bash
make docker-up          # ensure Kafka is running
make eval               # run CSV-mode drift evaluation (no Kafka required)
```

Expected output:
```
[HDFS]    F1=0.82  Precision=0.91  Recall=0.75  FPR=0.04
[NASA]    F1=0.89  Precision=0.93  Recall=0.85  FPR=0.03
[RETAIL]  F1=0.78  Precision=0.88  Recall=0.70  FPR=0.06
[OLIST]   F1=0.74  Precision=0.85  Recall=0.65  FPR=0.07
```

Variance of ±0.03 in F1 is normal across runs. Values below 0.65 indicate a regression.

### Adding a new dataset

1. Create `adapters/your_dataset_adapter.py` implementing `BaseAdapter.normalize(row) → dict`
2. Add a `DriftConfig` entry in `streaming/config.py`
3. Calibrate thresholds (see [Calibration.md](Calibration.md))
4. Add a Level 2 benchmark run with injected drift to validate
5. Update the benchmark table in [README.md](../README.md)

---

## Failure Modes

### Kafka fails to start

**Symptom:** `make docker-up` exits with `Container exiting`, `Connection refused` on port 9092.

**Diagnosis:**
```bash
docker compose ps
docker compose logs zookeeper
docker compose logs kafka
```

**Common causes:**
- Port 9092 already in use: `netstat -ano | findstr 9092` (Windows) / `lsof -i :9092` (Linux)
- Previous Kafka left dirty state: stop all containers, then `docker compose down -v && docker compose up -d`
- Insufficient memory: Kafka requires ~1.5 GB heap. Close other containers first.

**Recovery:**
```bash
docker compose down -v
docker compose up -d
python kafka/ensure_topics.py   # recreate topics after volume wipe
```

---

### Drift detector reporting F1 < 0.60 on a dataset

**Symptom:** Benchmark output shows F1 below expected baseline.

**Diagnosis steps:**
1. Verify the dataset CSV is unmodified: check file hash against `data/checksums.md` (if present)
2. Check that `DriftConfig` for the dataset has not been accidentally modified
3. Run with `--debug` flag to inspect per-window detection events:
   ```bash
   python scripts/eval_drift_detection.py --dataset hdfs --debug
   ```
4. Compare against the baseline ADR: [ADR-002](decisions/ADR-002-threshold-calibration-strategy.md)

**Common causes:**
- Threshold regression after a code change
- Dataset file corruption or re-download with different encoding
- Window size changed without recalibration

---

### `ensure_topics.py` fails — topic already exists

**Symptom:** `TopicAlreadyExistsException` or `TOPIC_ALREADY_EXISTS` error.

**Resolution:** This is safe to ignore. Topics are created with `if_not_exists=True` by default.
If you need to reset topics (e.g., after changing partition count):
```bash
python kafka/ensure_topics.py --delete-existing   # if flag is implemented
# or manually:
docker exec -it <kafka-container> kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic streaming.raw_events
```

---

### CI pipeline fails on `validate-notebook` step

**Symptom:** GitHub Actions shows `NotebookValidationError` or `nbformat` exception.

**Diagnosis:** Run locally:
```bash
python -c "import nbformat; nbformat.validate(nbformat.read(open('notebooks/evaluation_report.ipynb'), as_version=4))"
```

**Common cause:** Notebook cell missing `id` field (nbformat 4.5 requirement).

**Fix:**
```python
import uuid, nbformat
nb = nbformat.read(open("notebooks/evaluation_report.ipynb"), as_version=4)
for cell in nb.cells:
    if "id" not in cell:
        cell["id"] = uuid.uuid4().hex[:8]
nbformat.write(nb, open("notebooks/evaluation_report.ipynb", "w"))
```

---

## Kafka Topic Reference

| Topic | Retention | Partitions | Description |
|---|---|---|---|
| `streaming.raw_events` | 24h | 1 | Normalized events from adapters |
| `streaming.drift_events` | 7d | 1 | Structured drift alerts |
| `streaming.processed_events` | 24h | 1 | Events post-detection |
| `streaming.contract_events` | 7d | 1 | Schema contract pass/fail |
| `streaming.metrics` | 24h | 1 | Throughput, latency, drift count |
| `streaming.errors` | 7d | 1 | Dead-letter handler |

---

## Health Checks

```bash
# Kafka connectivity
docker exec -it <kafka-container> kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Topic list
docker exec -it <kafka-container> kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consumer lag (example)
docker exec -it <kafka-container> kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group drift-detector-group
```

---

## Escalation

If a benchmark regression cannot be diagnosed within 30 minutes:
1. Open a GitHub issue with the label `regression` and attach the full `make eval` output
2. Tag the failing dataset name in the issue title
3. If the issue is in Kafka E2E mode only (not CSV mode), label additionally as `kafka`
