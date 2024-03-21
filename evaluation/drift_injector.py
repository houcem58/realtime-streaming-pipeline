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
Runtime Drift Injector
----------------------
Applies domain-specific drift perturbations to event batches at runtime
in the Kafka producer stream. The detector on the consumer side is
completely blind to the injection schedule — it only sees the stream.

The injection schedule is deterministic (fixed seed) and saved separately
so that evaluation metrics (F1, precision, recall) can be computed against
the known ground truth.

Supported drift types per domain:

  nasa_http:
    - error_rate_spike   : 30% of events get status → "500", error_flag=True
    - bytes_collapse     : bytes → 0 (server returning empty responses)

  retail:
    - price_spike        : unit_price × 5 (pricing anomaly / supplier change)
    - return_rate_spike  : 30% of events become RETURN (negative quantity spike)
    - new_country        : inject new country value "SUSPICIOUS_REGION"

  olist:
    - cancellation_spike : 35% of orders forced to status "canceled"
    - delivery_delay     : delivery_delay_minutes × 10, sla_breach=True
    - payment_shift      : payment_value × 0.1 (payment processing anomaly)

  generic (any domain):
    - schema_rename      : renames "event_type" → "event_template"
    - missingness_spike  : 30% of "category" values set to NaN
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

# Injection every N windows, for a burst of M windows
_DEFAULT_EVERY = 6
_DEFAULT_BURST = 2

DOMAIN_DRIFT_TYPES: dict[str, list[str]] = {
    "nasa_http":  ["error_rate_spike", "bytes_collapse"],
    "retail":     ["price_spike", "return_rate_spike", "new_country"],
    "olist":      ["cancellation_spike", "delivery_delay", "payment_shift"],
    "generic":    ["schema_rename", "missingness_spike"],
}


class DriftInjector:
    """
    Applies domain-specific drift perturbations at runtime in the producer.

    Parameters
    ----------
    domain : str
        One of: "nasa_http", "retail", "olist", "generic".
    inject_every : int
        Inject drift every N windows (periodic schedule). Default 6.
    burst_length : int
        Number of consecutive anomaly windows per burst. Default 2.
    inject_ratio : float
        Fraction of events in an anomaly window to perturb. Default 0.30.
    seed : int
        Random seed for reproducibility. Default 42.
    """

    def __init__(
        self,
        domain: str = "generic",
        inject_every: int = _DEFAULT_EVERY,
        burst_length: int = _DEFAULT_BURST,
        inject_ratio: float = 0.30,
        seed: int = 42,
    ):
        self.domain = domain
        self.inject_every = inject_every
        self.burst_length = burst_length
        self.inject_ratio = inject_ratio
        self._rng = np.random.default_rng(seed)
        self._drift_types = DOMAIN_DRIFT_TYPES.get(domain, DOMAIN_DRIFT_TYPES["generic"])

    # ── Schedule ─────────────────────────────────────────────────────────────

    def is_anomaly_window(self, window_idx: int) -> bool:
        """Returns True if this window should have drift injected."""
        pos = window_idx % self.inject_every
        return pos >= (self.inject_every - self.burst_length)

    def get_schedule(self, n_windows: int) -> list[bool]:
        """Returns the full injection schedule as a list of booleans."""
        return [self.is_anomaly_window(i) for i in range(n_windows)]

    def get_drift_type(self, window_idx: int) -> str | None:
        """Returns which drift type to apply at this window (cycles through list)."""
        if not self.is_anomaly_window(window_idx):
            return None
        burst_pos = window_idx % self.inject_every - (self.inject_every - self.burst_length)
        return self._drift_types[burst_pos % len(self._drift_types)]

    # ── Injection ─────────────────────────────────────────────────────────────

    def inject(self, df: pd.DataFrame, window_idx: int) -> pd.DataFrame:
        """
        Apply drift to a batch of events if window_idx is an anomaly window.
        Returns the (possibly modified) DataFrame. Original is not mutated.
        """
        if df.empty or not self.is_anomaly_window(window_idx):
            return df

        out = df.copy()
        drift_type = self.get_drift_type(window_idx)
        n_inject = max(1, int(len(out) * self.inject_ratio))
        target_idx = out.index[:n_inject]

        if drift_type == "error_rate_spike":
            if "status" in out.columns:
                out.loc[target_idx, "status"] = "500"
            out.loc[target_idx, "error_flag"] = True
            out.loc[target_idx, "severity"] = "ERROR"
            out.loc[target_idx, "anomaly_label"] = 1

        elif drift_type == "bytes_collapse":
            if "bytes" in out.columns:
                out.loc[target_idx, "bytes"] = 0.0
            if "latency_ms" in out.columns:
                out.loc[target_idx, "latency_ms"] = 0.0
            out.loc[target_idx, "anomaly_label"] = 1

        elif drift_type == "price_spike":
            if "unit_price" in out.columns:
                out.loc[target_idx, "unit_price"] *= 5.0
            if "amount" in out.columns:
                out.loc[target_idx, "amount"] *= 5.0
            out.loc[target_idx, "anomaly_label"] = 1

        elif drift_type == "return_rate_spike":
            out.loc[target_idx, "event_type"] = "RETURN"
            out.loc[target_idx, "error_flag"] = True
            out.loc[target_idx, "severity"] = "WARN"
            if "quantity" in out.columns:
                out.loc[target_idx, "quantity"] = out.loc[target_idx, "quantity"].abs() * -1
            out.loc[target_idx, "anomaly_label"] = 1

        elif drift_type == "new_country":
            if "region" in out.columns:
                out.loc[target_idx, "region"] = "SUSPICIOUS_REGION"
            out.loc[target_idx, "anomaly_label"] = 1

        elif drift_type == "cancellation_spike":
            out.loc[target_idx, "event_type"] = "canceled"
            out.loc[target_idx, "status"] = "canceled"
            out.loc[target_idx, "error_flag"] = True
            out.loc[target_idx, "severity"] = "WARN"
            if "category" in out.columns:
                out.loc[target_idx, "category"] = "canceled"
            out.loc[target_idx, "anomaly_label"] = 1

        elif drift_type == "delivery_delay":
            if "delivery_delay_minutes" in out.columns:
                out.loc[target_idx, "delivery_delay_minutes"] *= 10.0
            if "latency_ms" in out.columns:
                out.loc[target_idx, "latency_ms"] *= 10.0
            out.loc[target_idx, "sla_breach"] = True
            out.loc[target_idx, "anomaly_label"] = 1

        elif drift_type == "payment_shift":
            if "amount" in out.columns:
                out.loc[target_idx, "amount"] *= 0.1
            out.loc[target_idx, "anomaly_label"] = 1

        elif drift_type == "schema_rename":
            if "event_type" in out.columns:
                out = out.rename(columns={"event_type": "event_template"})
            out.loc[target_idx, "anomaly_label"] = 1

        elif drift_type == "missingness_spike":
            if "category" in out.columns:
                out.loc[target_idx, "category"] = np.nan
            out.loc[target_idx, "anomaly_label"] = 1

        return out

    def describe(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "inject_every": self.inject_every,
            "burst_length": self.burst_length,
            "inject_ratio": self.inject_ratio,
            "drift_types": self._drift_types,
        }
