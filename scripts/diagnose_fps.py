"""Diagnostic: show which column+drift_type causes each FP and FN per dataset."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd
from adapters.nasa_adapter import load_nasa
from adapters.retail_adapter import load_retail
from adapters.olist_adapter import load_olist
from evaluation.drift_injector import DriftInjector
from streaming.drift_detector import DriftDetector

ETL = Path(r"C:\Users\houce\Desktop\ETL")

DATASETS = {
    "nasa_http": {
        "loader": lambda: load_nasa(ETL / "data/real/nasa/nasa_http.csv",
                                     target_events=20000, duration_minutes=2880),
        "domain": "nasa_http",
        "exclude_extra": {"status"},
        "sigma": 5.0,
        "hc_skip": 10,
    },
    "hdfs": {
        "loader": lambda: None,  # uses label-based eval, skip here
        "domain": "generic",
        "exclude_extra": set(),
        "sigma": 4.0,
        "hc_skip": 50,
    },
}

BASE_EXCLUDE = {
    "event_id", "session_id", "entity_id", "message",
    "timestamp", "template_id", "source_dataset", "domain",
}
BASE_BINARY = {"error_flag", "anomaly_label", "sla_breach"}

WINDOW_SIZE = 500
BASELINE_WINDOWS = 3


def diagnose(name, df, domain, exclude_extra, sigma, hc_skip):
    windows = [df.iloc[i:i+WINDOW_SIZE].reset_index(drop=True)
               for i in range(0, len(df), WINDOW_SIZE) if len(df.iloc[i:i+WINDOW_SIZE]) > 0]

    injector = DriftInjector(domain=domain, inject_every=6, burst_length=2, seed=42)
    detector = DriftDetector(
        missing_threshold=0.05,
        mean_shift_sigma=sigma,
        min_std=1.0,
        high_cardinality_skip=hc_skip,
        rate_alert_multiplier=2.5,
        rate_alert_min_delta=0.05,
        exclude_columns=BASE_EXCLUDE | exclude_extra,
        binary_columns=BASE_BINARY,
    )

    print(f"\n{'='*70}")
    print(f"DIAGNOSTIC: {name}  |  {len(windows)} windows  |  sigma={sigma}  |  hc_skip={hc_skip}")
    print(f"Excluded extra columns: {exclude_extra or 'none'}")
    print(f"{'='*70}")

    # Print baseline column stats
    baseline_df = pd.concat([windows[i] for i in range(min(BASELINE_WINDOWS, len(windows)))], ignore_index=True)
    detector.fit_baseline(baseline_df)
    active = [c for c in baseline_df.columns if c not in detector.exclude_columns]
    numeric = baseline_df[active].select_dtypes(include="number").columns.tolist()
    categorical = [c for c in active if c not in numeric and c not in BASE_BINARY]
    print(f"\nBaseline numeric columns tracked: {[c for c in numeric if c not in BASE_BINARY]}")
    print(f"Baseline categorical columns tracked (n_unique): "
          f"{ {c: baseline_df[c].nunique() for c in categorical} }")
    print(f"Baseline binary columns tracked: {[c for c in numeric if c in BASE_BINARY]}")

    print(f"\n{'Window':>8} {'Type':>6} {'Detected':>9}  Drift events (column | type | base->cur)")
    print("-" * 70)

    for idx, window in enumerate(windows):
        if idx < BASELINE_WINDOWS:
            continue
        is_anomaly = injector.is_anomaly_window(idx)
        perturbed = injector.inject(window, idx)
        events = detector.detect(perturbed)
        detected = len(events) > 0

        if is_anomaly and detected:
            label = "TP"
        elif not is_anomaly and detected:
            label = "FP"
        elif is_anomaly and not detected:
            label = "FN"
        else:
            continue  # TN — don't print

        drift_type = injector.get_drift_type(idx) or "-"
        print(f"\n  win={idx:03d} [{label}]  injected={drift_type}")
        for e in events:
            base_v = e.get("baseline_value", "")[:40]
            cur_v = e.get("current_value", "")[:40]
            print(f"           col={e['column']:25s}  type={e['drift_type']:22s}  {base_v} -> {cur_v}")


def diagnose_hdfs(events_path, labels_path):
    """HDFS uses real labels, not injector. Show what fires on clean windows."""
    events_df = pd.read_csv(events_path)
    events_df["timestamp"] = pd.to_datetime(events_df["timestamp"], utc=True, format="mixed")
    events_df = events_df.sort_values("timestamp").reset_index(drop=True)

    labels_df = pd.read_csv(labels_path)
    labels_df["start_time"] = pd.to_datetime(labels_df["start_time"], utc=True)
    labels_df["end_time"] = pd.to_datetime(labels_df["end_time"], utc=True)

    detector = DriftDetector(
        missing_threshold=0.05, mean_shift_sigma=4.0, min_std=1.0,
        high_cardinality_skip=50, rate_alert_multiplier=2.5, rate_alert_min_delta=0.05,
        exclude_columns=BASE_EXCLUDE, binary_columns=BASE_BINARY,
    )

    print(f"\n{'='*70}")
    print("DIAGNOSTIC: HDFS LogHub (real labels)")
    print(f"{'='*70}")

    baseline_fitted = False
    for idx, (_, label_row) in enumerate(labels_df.iterrows()):
        window_df = events_df[
            (events_df["timestamp"] >= label_row["start_time"]) &
            (events_df["timestamp"] < label_row["end_time"])
        ]
        if window_df.empty:
            continue
        if not baseline_fitted:
            detector.fit_baseline(window_df)
            baseline_fitted = True
            active = [c for c in window_df.columns if c not in detector.exclude_columns]
            numeric = window_df[active].select_dtypes(include="number").columns.tolist()
            categorical = [c for c in active if c not in numeric and c not in BASE_BINARY]
            print(f"Baseline categorical cols (n_unique): { {c: window_df[c].nunique() for c in categorical} }")
            continue

        events = detector.detect(window_df)
        detected = len(events) > 0
        is_anomaly = bool(label_row["has_anomaly"])

        if not is_anomaly and detected:
            label = "FP"
        elif is_anomaly and not detected:
            label = "FN"
        else:
            continue

        print(f"\n  win={idx:03d} [{label}]  ground_truth={is_anomaly}")
        for e in events:
            bv = str(e.get("baseline_value", ""))[:40]
            cv = str(e.get("current_value", ""))[:40]
            print(f"           col={e['column']:25s}  type={e['drift_type']:22s}  {bv} -> {cv}")


if __name__ == "__main__":
    print("Loading NASA HTTP ...")
    df_nasa = load_nasa(ETL / "data/real/nasa/nasa_http.csv",
                        target_events=20000, duration_minutes=2880)
    diagnose("NASA HTTP", df_nasa, "nasa_http",
             exclude_extra={"status"}, sigma=5.0, hc_skip=10)

    print("\n\nRunning HDFS diagnostic ...")
    diagnose_hdfs(
        ETL / "bench_v6/streaming_drift/public_logs/data/public_log_events.csv",
        ETL / "bench_v6/streaming_drift/public_logs/data/public_log_window_labels.csv",
    )
