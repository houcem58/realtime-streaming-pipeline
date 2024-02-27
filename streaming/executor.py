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

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from streaming.drift_detector import DriftDetector
from streaming.live_state import LiveStateStore


@dataclass
class BatchResult:
    batch_index: int
    rows: int
    result: pd.DataFrame
    contract_status: Dict[str, Any]
    latency_ms: float
    drift_events: List[Dict[str, Any]]
    action_taken: str


class StreamingExecutor:
    """
    Micro-batch streaming executor with built-in drift detection.

    Works with any StreamSource (SimulatedStreamSource, CSVStreamSource, KafkaStreamSource).
    On each batch:
      1. Reads next batch from source
      2. Runs DriftDetector (fits baseline on first batch, detects on subsequent)
      3. Applies processor function (or default aggregation)
      4. Emits BatchResult with drift events, latency, and contract status

    Modes:
      - compile_once_execute_many : execute the same compiled plan regardless of drift
      - replan_on_drift           : flag batches where drift was detected for reprocessing
    """

    def __init__(
        self,
        source: Any,
        *,
        mode: str = "compile_once_execute_many",
        processor: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        live_state: Optional[LiveStateStore] = None,
        drift_detector: Optional[DriftDetector] = None,
    ):
        self.source = source
        self.mode = mode
        self.processor = processor
        self.live_state = live_state or LiveStateStore()
        self.drift_detector = drift_detector or DriftDetector()
        self.batch_index = 0
        self._baseline_fit = False

    def _default_process(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        numeric = df.select_dtypes(include="number").columns.tolist()
        categorical = [c for c in df.columns if c not in numeric]
        if categorical and numeric:
            group = categorical[0]
            metric = numeric[0]
            return df.groupby(group)[metric].agg(["sum", "mean", "count"]).reset_index().head(20)
        if numeric:
            return pd.DataFrame({c: [df[c].sum()] for c in numeric[:5]})
        return pd.DataFrame({"rows": [len(df)]})

    @staticmethod
    def _contract_status(result: pd.DataFrame) -> Dict[str, Any]:
        return {
            "passed": isinstance(result, pd.DataFrame) and not result.empty,
            "shape": list(result.shape) if isinstance(result, pd.DataFrame) else None,
        }

    def process_next(self) -> BatchResult:
        start = perf_counter()
        batch = self.source.next_batch()

        if not self._baseline_fit and not batch.empty:
            self.drift_detector.fit_baseline(batch)
            self._baseline_fit = True
            drift_events: List[Dict[str, Any]] = []
        else:
            drift_events = self.drift_detector.detect(batch) if not batch.empty else []

        action = (
            "reprofile_replan_recommended"
            if drift_events and self.mode == "replan_on_drift"
            else "execute_compiled_plan"
        )

        result = self.processor(batch) if self.processor is not None else self._default_process(batch)
        latency_ms = (perf_counter() - start) * 1000
        contract = self._contract_status(result)

        self.live_state.append_batch(batch, {
            "latency_ms": latency_ms,
            "contract_status": contract,
            "drift_events": drift_events,
            "action_taken": action,
        })

        out = BatchResult(
            batch_index=self.batch_index,
            rows=len(batch),
            result=result,
            contract_status=contract,
            latency_ms=latency_ms,
            drift_events=drift_events,
            action_taken=action,
        )
        self.batch_index += 1
        return out

    def run(self, max_batches: int = 10) -> List[BatchResult]:
        self.source.start()
        results: List[BatchResult] = []
        try:
            while len(results) < max_batches and self.source.is_available():
                results.append(self.process_next())
        finally:
            self.source.stop()
        return results

    def metrics(self) -> Dict[str, Any]:
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **self.live_state.get_metrics(),
        }


class KafkaStreamingExecutor:
    """
    Kafka-first micro-batch executor with multi-topic output architecture.

    Topics (all configurable):
      - raw_events        : input
      - processed_events  : transformed output per batch
      - drift_events      : one message per drift event detected
      - contract_events   : contract pass/fail per batch
      - metrics           : throughput and latency per batch
      - errors            : dead-letter queue for failed batches
    """

    def __init__(
        self,
        *,
        brokers: str = "localhost:9092",
        raw_topic: str = "streaming.raw_events",
        processed_topic: str = "streaming.processed_events",
        drift_topic: str = "streaming.drift_events",
        contract_topic: str = "streaming.contract_events",
        metrics_topic: str = "streaming.metrics",
        errors_topic: str = "streaming.errors",
        group_id: str = "streaming-executor",
        max_batch_size: int = 100,
        max_wait_seconds: float = 2.0,
        output_dir: str | Path = "results/streaming",
        live_state: Optional[LiveStateStore] = None,
        processor: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        drift_detector: Optional[DriftDetector] = None,
    ):
        try:
            from confluent_kafka import Consumer, Producer  # noqa: F401
        except Exception as exc:
            raise RuntimeError(
                "Kafka is required. Start with: docker compose up -d kafka"
            ) from exc

        self.brokers = brokers
        self.raw_topic = raw_topic
        self.processed_topic = processed_topic
        self.drift_topic = drift_topic
        self.contract_topic = contract_topic
        self.metrics_topic = metrics_topic
        self.errors_topic = errors_topic
        self.group_id = group_id
        self.max_batch_size = max_batch_size
        self.max_wait_seconds = max_wait_seconds
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.live_state = live_state or LiveStateStore()
        self.processor = processor
        self.drift_detector = drift_detector or DriftDetector()
        self._baseline_fit = False
        self._batch_index = 0
        self._consumed = 0
        self._errors = 0
        self._latencies: List[float] = []

    @staticmethod
    def _json_default(value: Any) -> str:
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        return str(value)

    def _consumer(self):
        from confluent_kafka import Consumer
        c = Consumer({
            "bootstrap.servers": self.brokers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c.subscribe([self.raw_topic])
        return c

    def _producer(self):
        from confluent_kafka import Producer
        return Producer({"bootstrap.servers": self.brokers})

    def _emit(self, producer: Any, topic: str, event: Dict[str, Any]) -> None:
        producer.produce(topic, json.dumps(event, default=self._json_default).encode("utf-8"))
        producer.poll(0)

    def _read_batch(self, consumer: Any) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        deadline = perf_counter() + self.max_wait_seconds
        while len(rows) < self.max_batch_size and perf_counter() < deadline:
            msg = consumer.poll(0.2)
            if msg is None:
                continue
            if msg.error():
                self._errors += 1
                continue
            try:
                rows.append(json.loads(msg.value().decode("utf-8")))
            except Exception as exc:
                self._errors += 1
                rows.append({"raw": msg.value().decode("utf-8", errors="ignore"), "parse_error": type(exc).__name__})
        self._consumed += len(rows)
        return pd.DataFrame(rows)

    def _default_process(self, df: pd.DataFrame) -> pd.DataFrame:
        return StreamingExecutor(source=None, drift_detector=self.drift_detector)._default_process(df)

    def process_batches(self, max_batches: int = 10) -> pd.DataFrame:
        consumer = self._consumer()
        producer = self._producer()
        summaries: List[Dict[str, Any]] = []
        events_path = self.output_dir / "kafka_stream_events.jsonl"
        try:
            for _ in range(max_batches):
                start = perf_counter()
                batch = self._read_batch(consumer)
                if batch.empty:
                    continue

                if not self._baseline_fit:
                    self.drift_detector.fit_baseline(batch)
                    self._baseline_fit = True
                    drift_events: List[Dict[str, Any]] = []
                else:
                    drift_events = self.drift_detector.detect(batch)

                result = self.processor(batch) if self.processor else self._default_process(batch)
                contract = {"passed": isinstance(result, pd.DataFrame), "shape": list(result.shape)}
                latency_ms = (perf_counter() - start) * 1000
                self._latencies.append(latency_ms)

                self.live_state.append_batch(batch, {
                    "latency_ms": latency_ms,
                    "contract_status": contract,
                    "drift_events": drift_events,
                })

                for row in result.head(100).to_dict(orient="records"):
                    self._emit(producer, self.processed_topic, {"batch": self._batch_index, **row})
                for event in drift_events:
                    self._emit(producer, self.drift_topic, {"batch": self._batch_index, **event})
                self._emit(producer, self.contract_topic, {"batch": self._batch_index, **contract})

                metric = {
                    "batch_index": self._batch_index,
                    "consumed_events": self._consumed,
                    "rows_processed": len(batch),
                    "latency_ms": round(latency_ms, 2),
                    "drift_events_count": len(drift_events),
                    "error_count": self._errors,
                    "throughput_eps": round(len(batch) / max(latency_ms / 1000, 1e-9), 1),
                }
                self._emit(producer, self.metrics_topic, metric)
                producer.flush(5)

                with events_path.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps({"batch": self._batch_index, "metrics": metric, "drift_events": drift_events}, default=self._json_default) + "\n")

                summaries.append(metric)
                self._batch_index += 1

        except Exception as exc:
            self._errors += 1
            self._emit(producer, self.errors_topic, {"error_type": type(exc).__name__, "message": str(exc)})
            producer.flush(5)
            raise
        finally:
            consumer.close()

        summary_df = pd.DataFrame(summaries)
        summary_df.to_csv(self.output_dir / "kafka_stream_summary.csv", index=False)
        return summary_df
