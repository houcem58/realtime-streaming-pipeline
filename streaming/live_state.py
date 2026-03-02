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
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class StateSnapshot:
    latest_df: pd.DataFrame
    drift_events: List[Dict[str, Any]]
    contract_events: List[Dict[str, Any]]
    last_contract_status: Optional[Dict[str, Any]]


class LiveStateStore:
    """In-memory store of recent batch results, drift events, and metrics."""

    def __init__(self, max_batches: int = 50):
        self.max_batches = max_batches
        self._batches: List[pd.DataFrame] = []
        self._meta: List[Dict[str, Any]] = []
        self._drift_events: List[Dict[str, Any]] = []
        self._contract_events: List[Dict[str, Any]] = []
        self._last_contract_status: Optional[Dict[str, Any]] = None

    def append_batch(self, df: pd.DataFrame, meta: Dict[str, Any]) -> None:
        self._batches.append(df)
        self._meta.append(meta)
        if len(self._batches) > self.max_batches:
            self._batches.pop(0)
            self._meta.pop(0)
        self._drift_events.extend(meta.get("drift_events") or [])
        if "contract_status" in meta:
            self._last_contract_status = meta["contract_status"]
            self._contract_events.append(meta["contract_status"])

    def snapshot(self) -> StateSnapshot:
        latest = self._batches[-1] if self._batches else pd.DataFrame()
        return StateSnapshot(
            latest_df=latest,
            drift_events=list(self._drift_events),
            contract_events=list(self._contract_events),
            last_contract_status=self._last_contract_status,
        )

    def get_metrics(self) -> Dict[str, Any]:
        latencies = [m.get("latency_ms", 0) for m in self._meta if "latency_ms" in m]
        drift_counts = [len(m.get("drift_events") or []) for m in self._meta]
        return {
            "batches_processed": len(self._batches),
            "total_drift_events": len(self._drift_events),
            "avg_latency_ms": round(sum(latencies) / max(len(latencies), 1), 2),
            "avg_drift_events_per_batch": round(sum(drift_counts) / max(len(drift_counts), 1), 2),
        }
