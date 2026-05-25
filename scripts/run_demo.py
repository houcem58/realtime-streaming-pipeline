# Copyright 2025 Houcem Hammami
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
End-to-end demo — runs the streaming drift detector without Kafka.

Replays the public LogHub HDFS dataset (data/events_sample.csv) in micro-batches
using CSVStreamSource, then runs DriftDetector on each batch and prints a summary.

Usage:
    python scripts/run_demo.py
    python scripts/run_demo.py --batches 20 --batch_size 100
    python scripts/run_demo.py --inject error_rate_spike   # inject synthetic drift
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from streaming.drift_detector import DriftDetector
from streaming.executor import StreamingExecutor
from streaming.sources import CSVStreamSource, SimulatedStreamSource

SAMPLE_DATA = ROOT / "data" / "events_sample.csv"
LABELS_DATA = ROOT / "data" / "window_labels.csv"


def run_csv_demo(batches: int, batch_size: int) -> None:
    print(f"\n[DEMO] Replaying LogHub HDFS public log — {batches} batches × {batch_size} events")
    print(f"[DEMO] Source: {SAMPLE_DATA}\n")

    source = CSVStreamSource(path=SAMPLE_DATA, batch_size=batch_size)
    detector = DriftDetector(
        missing_threshold=0.05,
        mean_shift_sigma=4.0,
        min_std=1.0,
        high_cardinality_skip=50,
        rate_alert_multiplier=2.5,
        rate_alert_min_delta=0.05,
    )
    executor = StreamingExecutor(source=source, drift_detector=detector)
    results = executor.run(max_batches=batches)

    print(f"{'Batch':>5}  {'Rows':>6}  {'Drift Events':>12}  {'Latency (ms)':>12}  {'Action'}")
    print("-" * 70)
    total_drift = 0
    for r in results:
        print(
            f"{r.batch_index:>5}  {r.rows:>6}  {len(r.drift_events):>12}  "
            f"{r.latency_ms:>12.2f}  {r.action_taken}"
        )
        total_drift += len(r.drift_events)
        if r.drift_events:
            for e in r.drift_events:
                print(f"         ↳ [{e['severity'].upper():6}] {e['drift_type']} on '{e['column']}' "
                      f"(baseline={e['baseline_value'][:30]}, current={e['current_value'][:30]})")

    print("-" * 70)
    print(f"\nSummary: {len(results)} batches processed | {total_drift} total drift events detected")
    print(json.dumps(executor.metrics(), indent=2))

    summary_df = detector.summarize_events()
    if not summary_df.empty:
        print(f"\nDrift event breakdown:")
        print(summary_df.groupby(["drift_type", "severity"])["column"].count().to_string())


def run_simulated_demo(inject: str | None, batches: int, batch_size: int) -> None:
    drifts = []
    if inject == "error_rate_spike":
        drifts = [{"type": "error_rate_spike", "at_batch": 3}]
    elif inject == "schema_rename":
        drifts = [{"type": "schema_rename", "at_batch": 3}]
    elif inject == "latency_shift":
        drifts = [{"type": "latency_shift", "at_batch": 3}]
    elif inject == "new_severity":
        drifts = [{"type": "new_severity", "at_batch": 3}]

    print(f"\n[DEMO] Simulated stream — injected drift: {inject or 'none'}")
    source = SimulatedStreamSource(batch_size=batch_size, max_batches=batches, drifts=drifts)
    detector = DriftDetector()
    executor = StreamingExecutor(source=source, drift_detector=detector)
    results = executor.run(max_batches=batches)

    print(f"{'Batch':>5}  {'Rows':>6}  {'Drift Events':>12}  {'Latency (ms)':>12}")
    print("-" * 50)
    for r in results:
        print(f"{r.batch_index:>5}  {r.rows:>6}  {len(r.drift_events):>12}  {r.latency_ms:>12.2f}")
        for e in r.drift_events:
            print(f"         ↳ [{e['severity'].upper():6}] {e['drift_type']} on '{e['column']}'")


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming drift detection demo")
    parser.add_argument("--batches", type=int, default=10)
    parser.add_argument("--batch_size", type=int, default=50)
    parser.add_argument("--mode", choices=["csv", "simulated"], default="csv")
    parser.add_argument(
        "--inject",
        choices=["error_rate_spike", "schema_rename", "latency_shift", "new_severity"],
        default=None,
        help="Inject a drift type into the simulated stream",
    )
    args = parser.parse_args()

    if args.mode == "csv":
        run_csv_demo(args.batches, args.batch_size)
    else:
        run_simulated_demo(args.inject, args.batches, args.batch_size)


if __name__ == "__main__":
    main()
