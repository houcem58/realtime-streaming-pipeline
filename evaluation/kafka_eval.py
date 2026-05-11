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
Drift Detection Evaluator
--------------------------
Supports two evaluation modes:

  Standalone (CSV, no Kafka)
  ──────────────────────────
  Load a real dataset → split into windows → apply DriftInjector at known
  windows → run DriftDetector on each window → compare against injection
  schedule → compute F1/Precision/Recall.

  Kafka E2E
  ─────────
  Same pipeline but events flow through Kafka. The producer injects drift
  at known windows; the consumer runs the detector. After the stream
  finishes, metrics are computed from the recorded schedule vs. detections.

Evaluation methodology:
  - Windows with drift injected  = anomaly (y_true=1)
  - Windows where detector fired = detected (y_pred=1)
  - Detector is blind to injection schedule (scientifically correct)

Usage
-----
    from evaluation.kafka_eval import DriftEvaluator

    ev = DriftEvaluator(
        dataset_name="nasa_http",
        df=load_nasa("data/nasa_http.csv"),
        window_size=500,
        injector=DriftInjector(domain="nasa_http"),
    )
    result = ev.run_standalone()
    print(result.report(dataset="NASA HTTP"))
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from evaluation.drift_injector import DriftInjector
from evaluation.metrics import DriftMetrics

try:
    from confluent_kafka import Consumer, Producer, KafkaException
    _KAFKA_AVAILABLE = True
except ImportError:
    _KAFKA_AVAILABLE = False

from streaming.drift_detector import DriftDetector


@dataclass
class EvalConfig:
    window_size: int = 500
    baseline_windows: int = 3
    bootstrap_servers: str = "localhost:9092"
    topic_raw: str = "streaming.raw_events"
    topic_drift: str = "streaming.drift_events"
    group_id: str = "eval-consumer"
    produce_delay_ms: float = 0.0


def _split_windows(df: pd.DataFrame, window_size: int) -> list[pd.DataFrame]:
    windows = []
    for start in range(0, len(df), window_size):
        chunk = df.iloc[start: start + window_size]
        if not chunk.empty:
            windows.append(chunk.reset_index(drop=True))
    return windows


class DriftEvaluator:
    """
    Evaluates drift detection accuracy against a controlled injection schedule.

    Parameters
    ----------
    dataset_name : str
        Human-readable label for reports ("nasa_http", "retail", "olist").
    df : pd.DataFrame
        Pre-normalized dataset (output of load_nasa / load_retail / load_olist).
    window_size : int
        Number of events per evaluation window.
    injector : DriftInjector
        Injector configured for the right domain.
    detector : DriftDetector, optional
        Override with a custom-tuned detector. If None, uses default params.
    config : EvalConfig, optional
        Kafka connection and evaluation parameters.
    """

    def __init__(
        self,
        dataset_name: str,
        df: pd.DataFrame,
        window_size: int = 500,
        injector: Optional[DriftInjector] = None,
        detector: Optional[DriftDetector] = None,
        config: Optional[EvalConfig] = None,
    ):
        self.dataset_name = dataset_name
        self.df = df.reset_index(drop=True)
        self.window_size = window_size
        self.injector = injector or DriftInjector(domain="generic")
        self.detector = detector or DriftDetector(
            missing_threshold=0.05,
            mean_shift_sigma=4.0,
            min_std=1.0,
            high_cardinality_skip=50,
            rate_alert_multiplier=2.5,
            rate_alert_min_delta=0.05,
            exclude_columns={
                "event_id", "session_id", "entity_id", "message",
                "timestamp", "template_id", "source_dataset", "domain",
            },
            binary_columns={"error_flag", "anomaly_label", "sla_breach"},
        )
        self.config = config or EvalConfig(window_size=window_size)

    # ── Standalone (CSV) evaluation ──────────────────────────────────────────

    def run_standalone(self, verbose: bool = True) -> DriftMetrics:
        """
        Run evaluation entirely in memory — no Kafka required.
        Returns DriftMetrics for this dataset.
        """
        windows = _split_windows(self.df, self.window_size)
        n_windows = len(windows)
        schedule = self.injector.get_schedule(n_windows)

        self.detector.reset()
        y_true: list[int] = []
        y_pred: list[int] = []

        if verbose:
            print(f"\n[{self.dataset_name}] {n_windows} windows, {len(self.df):,} events")
            print(f"  Anomaly windows: {sum(schedule)} / {n_windows}")
            print(f"  Injector: {self.injector.describe()}")
            print()

        for idx, (window, is_anomaly) in enumerate(zip(windows, schedule)):
            # Fit baseline on first N clean windows
            if idx < self.config.baseline_windows:
                self.detector.fit_baseline(window)
                continue

            perturbed = self.injector.inject(window, idx)
            events = self.detector.detect(perturbed)
            detected = len(events) > 0

            y_true.append(int(is_anomaly))
            y_pred.append(int(detected))

            if verbose and (is_anomaly or detected):
                drift_type = self.injector.get_drift_type(idx) or "-"
                marker = "TP" if is_anomaly and detected else (
                    "FP" if detected else "FN"
                )
                print(f"  Window {idx:03d} [{marker}] injected={drift_type}, "
                      f"detected={len(events)} event(s)")

        metrics = DriftMetrics(y_true, y_pred)
        if verbose:
            print()
            print(metrics.report(dataset=self.dataset_name))
        return metrics

    # ── Kafka E2E evaluation ─────────────────────────────────────────────────

    def run_kafka_e2e(
        self,
        verbose: bool = True,
        timeout_seconds: int = 300,
    ) -> DriftMetrics:
        """
        Run evaluation end-to-end through Kafka:
          1. Produce events to streaming.raw_events (with drift injection)
          2. Consume from streaming.drift_events (detector output)
          3. Compare vs. injection schedule → metrics

        Requires confluent-kafka and a running Kafka broker.
        """
        if not _KAFKA_AVAILABLE:
            raise RuntimeError(
                "confluent-kafka is not installed. "
                "Run: pip install confluent-kafka"
            )

        windows = _split_windows(self.df, self.window_size)
        n_windows = len(windows)
        schedule = self.injector.get_schedule(n_windows)
        baseline_n = self.config.baseline_windows

        producer = Producer({"bootstrap.servers": self.config.bootstrap_servers})
        consumer = Consumer({
            "bootstrap.servers": self.config.bootstrap_servers,
            "group.id": self.config.group_id,
            "auto.offset.reset": "latest",
        })
        consumer.subscribe([self.config.topic_drift])

        detected_windows: set[int] = set()
        produced_count = 0

        if verbose:
            print(f"\n[Kafka E2E] {self.dataset_name}: producing {n_windows} windows...")

        for idx, (window, is_anomaly) in enumerate(zip(windows, schedule)):
            perturbed = self.injector.inject(window, idx)
            for _, row in perturbed.iterrows():
                msg = json.dumps({
                    **row.to_dict(),
                    "_window_idx": idx,
                    "_is_baseline": idx < baseline_n,
                })
                producer.produce(
                    self.config.topic_raw,
                    value=msg.encode("utf-8"),
                )
                produced_count += 1

            producer.flush()
            if self.config.produce_delay_ms > 0:
                time.sleep(self.config.produce_delay_ms / 1000.0)

        if verbose:
            print(f"  Produced {produced_count:,} events. Consuming drift events...")

        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.error():
                if verbose:
                    print(f"  Consumer error: {msg.error()}")
                continue
            try:
                drift_event = json.loads(msg.value().decode("utf-8"))
                w_idx = drift_event.get("window_idx", -1)
                if w_idx >= 0:
                    detected_windows.add(w_idx)
            except Exception:
                pass

        consumer.close()

        y_true = [int(schedule[i]) for i in range(baseline_n, n_windows)]
        y_pred = [int(i in detected_windows) for i in range(baseline_n, n_windows)]

        metrics = DriftMetrics(y_true, y_pred)
        if verbose:
            print()
            print(metrics.report(dataset=f"{self.dataset_name} (Kafka E2E)"))
        return metrics
