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

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd


class StreamSource:
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def next_batch(self) -> pd.DataFrame: ...
    def is_available(self) -> bool: ...
    def describe(self) -> Dict[str, Any]: ...


@dataclass
class SimulatedStreamSource(StreamSource):
    """Generates synthetic log events with optional injected drift scenarios."""

    batch_size: int = 50
    max_batches: int = 10
    seed: int = 42
    drifts: List[Dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._rng = np.random.default_rng(self.seed)
        self._batch_idx = 0
        self._started = False

    def start(self) -> None:
        self._started = True
        self._batch_idx = 0

    def stop(self) -> None:
        self._started = False

    def is_available(self) -> bool:
        return self._started and self._batch_idx < self.max_batches

    def _base_batch(self) -> pd.DataFrame:
        n = self.batch_size
        idx = np.arange(self._batch_idx * n, (self._batch_idx + 1) * n)
        return pd.DataFrame({
            "event_id": [f"evt_{i:06d}" for i in idx],
            "timestamp": pd.Timestamp("2008-11-09T00:00:00Z") + pd.to_timedelta(idx * 30, unit="s"),
            "domain": self._rng.choice(["hdfs", "ecommerce", "support"], size=n, p=[0.5, 0.3, 0.2]),
            "event_type": self._rng.choice(["E5", "E9", "E11", "E22", "E26"], size=n),
            "severity": self._rng.choice(["INFO", "WARN", "ERROR"], size=n, p=[0.80, 0.15, 0.05]),
            "component": "dfs.DataNode",
            "latency_ms": self._rng.exponential(scale=50, size=n).round(3),
            "error_flag": self._rng.choice([False, True], size=n, p=[0.95, 0.05]),
            "anomaly_label": 0,
        })

    def _apply_drift(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for drift in self.drifts:
            at = int(drift.get("at_batch", 3))
            if self._batch_idx < at:
                continue
            kind = drift.get("type")
            if kind == "schema_rename" and "event_type" in out.columns:
                out = out.rename(columns={"event_type": "event_template"})
            elif kind == "error_rate_spike" and "error_flag" in out.columns:
                # Spike error rate from 5% to ~40%
                n_errors = int(len(out) * 0.40)
                out.loc[out.index[:n_errors], "error_flag"] = True
                out.loc[out.index[:n_errors], "anomaly_label"] = 1
            elif kind == "latency_shift" and "latency_ms" in out.columns:
                out["latency_ms"] = out["latency_ms"] + 500
            elif kind == "missingness_spike" and "component" in out.columns:
                out.loc[out.index[: max(1, len(out) // 3)], "component"] = np.nan
            elif kind == "new_severity" and "severity" in out.columns:
                out.loc[out.index[:3], "severity"] = "FATAL"
        return out

    def next_batch(self) -> pd.DataFrame:
        if not self._started:
            self.start()
        if self._batch_idx >= self.max_batches:
            return pd.DataFrame()
        df = self._apply_drift(self._base_batch())
        self._batch_idx += 1
        return df

    def describe(self) -> Dict[str, Any]:
        return {"type": "simulated", "batch_size": self.batch_size, "max_batches": self.max_batches, "drifts": self.drifts}


@dataclass
class CSVStreamSource(StreamSource):
    """Replays events from a CSV file in micro-batches."""

    path: str | Path
    batch_size: int = 100
    loop: bool = False

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        self._df = pd.DataFrame()
        self._pos = 0
        self._started = False

    def start(self) -> None:
        self._df = pd.read_csv(self.path) if self.path.exists() else pd.DataFrame()
        self._pos = 0
        self._started = True

    def stop(self) -> None:
        self._started = False

    def is_available(self) -> bool:
        return self._started and (self.loop or self._pos < len(self._df))

    def next_batch(self) -> pd.DataFrame:
        if not self._started:
            self.start()
        if self._df.empty:
            return pd.DataFrame()
        if self._pos >= len(self._df):
            if not self.loop:
                return pd.DataFrame()
            self._pos = 0
        batch = self._df.iloc[self._pos: self._pos + self.batch_size].copy()
        self._pos += self.batch_size
        return batch

    def describe(self) -> Dict[str, Any]:
        return {"type": "csv", "path": str(self.path), "batch_size": self.batch_size, "loop": self.loop}


class KafkaStreamSource(StreamSource):
    """Consumes events from a Kafka topic in micro-batches."""

    def __init__(
        self,
        brokers: str = "localhost:9092",
        topic: str = "streaming.raw_events",
        group_id: str = "streaming-drift-consumer",
        batch_size: int = 100,
    ):
        self.brokers = brokers
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.warning = ""
        self.consumer = None

    def start(self) -> None:
        try:
            from confluent_kafka import Consumer
            self.consumer = Consumer({
                "bootstrap.servers": self.brokers,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
            })
            self.consumer.subscribe([self.topic])
        except Exception as exc:
            self.warning = f"Kafka unavailable: {type(exc).__name__}: {exc}"
            self.consumer = None

    def stop(self) -> None:
        if self.consumer is not None:
            self.consumer.close()
        self.consumer = None

    def is_available(self) -> bool:
        return self.consumer is not None

    def next_batch(self) -> pd.DataFrame:
        if self.consumer is None:
            return pd.DataFrame()
        rows = []
        for _ in range(self.batch_size):
            msg = self.consumer.poll(0.05)
            if msg is None or msg.error():
                continue
            try:
                rows.append(pd.read_json(msg.value(), typ="series").to_dict())
            except Exception:
                rows.append({"raw": msg.value().decode("utf-8", errors="ignore")})
        return pd.DataFrame(rows)

    def describe(self) -> Dict[str, Any]:
        return {"type": "kafka", "brokers": self.brokers, "topic": self.topic, "available": self.is_available(), "warning": self.warning}
