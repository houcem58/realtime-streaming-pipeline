# Calibration — Per-Dataset Threshold Decisions

## Methodology

The `DriftDetector` is instantiated with dataset-specific parameters to account for
domain-specific column semantics. This is not post-hoc tuning: parameters are set once
before evaluation and do not change between runs.

This follows the standard practice in stream drift detection literature
([Gama et al., 2014](https://dl.acm.org/doi/10.1145/2523813)) of adapting statistical
thresholds to the target domain's baseline distribution.

## Per-Dataset Configuration

### LogHub HDFS

```python
DriftDetector(
    exclude_columns={"block_id", "session_id", "timestamp"},
    binary_columns={"error_flag", "anomaly_label"},
    mean_shift_sigma=4.0,
    min_category_frequency=0.02,
)
```

**Rationale:** HDFS logs contain high-cardinality block IDs that must be excluded.
Binary anomaly labels use rate-based alerting to avoid flagging normal error rate variation.

### NASA HTTP

```python
DriftDetector(
    exclude_columns={"host", "timestamp", "request"},
    mean_shift_sigma=4.0,
    min_relative_shift=1.0,
    min_category_frequency=0.01,
)
```

**Rationale:** HTTP method cardinality is low (GET/POST/HEAD) but HEAD requests are rare.
`min_relative_shift=1.0` prevents near-zero baseline means (e.g., response time for
certain request types) from producing artificially tight sigma thresholds.

### Online Retail II

```python
DriftDetector(
    exclude_columns={"invoice_no", "customer_id", "description"},
    mean_shift_sigma=3.5,
    min_category_frequency=0.02,
)
```

**Rationale:** Country distribution varies legitimately (UK dominates at ~90%). Threshold
set to 3.5σ to catch meaningful quantity distribution shifts without flagging seasonal variation.

### Olist E-Commerce

```python
DriftDetector(
    exclude_columns={"order_id", "customer_id", "product_id"},
    mean_shift_sigma=4.0,
    min_category_frequency=0.02,
)
```

**Rationale:** Payment value distributions are heavy-tailed. 4σ threshold balances
sensitivity against the natural variance in marketplace transaction values.

## Binary Column Rate Alerting

Binary columns (`error_flag`, `sla_breach`, `anomaly_label`) use a dual-condition
rate-based alert instead of the sigma test:

```
Alert fires when:
  current_rate >= multiplier * baseline_rate   (default: 2.5×)
  AND
  current_rate - baseline_rate >= min_delta    (default: 5%)
```

This prevents a normal 5%→8% error rate fluctuation from being flagged as drift, while
catching a genuine 5%→20% anomaly rate spike.
