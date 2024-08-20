# ADR-002 — Drift Detection Threshold Calibration Strategy

**Status:** Accepted  
**Date:** 2025-08-15  
**Author:** Houcem Hammami  
**Reviewers:** —  

---

## Context

The `DriftDetector` uses five independent detection checks. Each check requires a numerical
threshold that determines when a statistical signal becomes a drift event. Setting a single
global threshold across all datasets produces either excessive false positives (too tight) or
missed detections (too loose).

Four datasets are supported: LogHub HDFS, NASA HTTP, Online Retail II, Olist e-commerce.
Each has different schema, cardinality, and distributional characteristics.

---

## Decision

**Per-dataset configuration with documented derivation, not global tuning.**

Each dataset has its own `DriftConfig` object specifying:
- `missingness_threshold` — minimum NULL rate jump to flag as drift
- `sigma_threshold` — number of standard deviations from baseline mean
- `value_drift_floor` — minimum frequency for unseen category to trigger alert
- `binary_rate_multiplier` — for binary/boolean columns: `current_rate >= N × baseline_rate`
- `binary_rate_delta` — minimum absolute rate change to accompany the multiplier

---

## Rationale

### Binary column rate alerting (2.5× multiplier + 5% delta)

Binary columns (e.g., HTTP status indicators, fraud flags) have baseline rates that vary
widely across datasets. A log server error rate might be 0.2%; a fraud flag rate might be 8%.

A pure multiplier (`>= 2.5×`) alerts correctly on the 0.2% baseline (fires at 0.5%) but
is insensitive on the 8% baseline (fires only at 20%, by which point the pipeline is already
severely degraded). Adding a minimum absolute delta (`>= 5%`) prevents both scenarios:

- Low-baseline columns: 0.2% baseline, `0.2% × 2.5 = 0.5%` — multiplier is the binding constraint
- High-baseline columns: 8% baseline, `8% × 2.5 = 20%` — delta is the binding constraint

This combination was validated against all four Level 2 benchmark runs (injected drift) and
produced zero false positives on the Level 1 (real label) runs.

### Sigma threshold (dataset-specific, range 2.0–4.0)

Numerical columns are compared using: `|μ_current − μ_baseline| > k × σ_baseline`

The multiplier `k` was chosen per dataset by:
1. Running baseline window fitting on the first 20% of each dataset's events
2. Computing the empirical distribution of `|μ_batch − μ_baseline| / σ_baseline` for clean windows
3. Setting `k` at the 99th percentile of the clean-window distribution
4. Verifying on the injected-drift Level 2 runs that the threshold fires within 1 window
   of the injection point

HDFS and NASA HTTP required higher k (3.5–4.0) due to higher natural variability in
server log metrics. Retail and Olist required lower k (2.0–2.5) due to more stable
commercial transaction distributions.

### Why not adaptive / learned thresholds?

Adaptive thresholds (e.g., ADWIN, Page-Hinkley) were evaluated. They were rejected because:
- They require unbounded memory to track full distribution history
- They are sensitive to the order of events in the baseline window
- They produce non-reproducible results across different batch sizes
- The evaluation benchmark requires frozen reproducibility artifacts (deterministic seed=42)

---

## Consequences

**Positive:**
- All thresholds are documented with derivation, not "magic numbers"
- Reproducible: same configuration produces identical results given identical input
- Per-dataset tuning produces better precision/recall trade-off than any global threshold
- Calibration decisions are testable: the `DriftMetrics` framework can validate each threshold

**Negative:**
- Adding a new dataset requires calibration work before reliable detection
- The calibration process is manual — no automated threshold search

**Mitigation (future work, see ROADMAP.md):**
- Automated threshold search using the Level 2 benchmark as a hyperparameter optimization target
- Adaptive threshold mode as an opt-in configuration for online production deployments

---

## Review Trigger

This decision should be revisited if:
- A new dataset is added that exhibits distribution characteristics outside the current range
- The Level 1 benchmark F1 score drops below 0.85 after a code change
- A production deployment reports > 5% false positive rate in a 30-day window
