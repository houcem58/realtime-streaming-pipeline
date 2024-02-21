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

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import pandas as pd


class DriftDetector:
    """
    Deterministic micro-batch schema and distribution drift detector
    adapted for structured log data (HDFS LogHub format).

    Detects 5 drift types per batch:
      - schema_rename      : expected column missing, new column appeared
      - type_drift         : column dtype changed incompatibly
      - missingness_drift  : NULL rate jumped by >= missing_threshold
      - value_drift        : genuinely unseen category appeared (low-cardinality cols only)
      - distribution_drift : numeric mean shifted (continuous) or rate doubled (binary)

    Key design decisions vs. naive detectors:
      - Binary/boolean columns (error_flag, anomaly_label) use a RATE-based test:
        flag only when rate increases by >= rate_alert_min_delta AND >= rate_alert_multiplier * baseline.
        This prevents a normal 5%->8% error rate from firing as drift.
      - Columns in exclude_columns (IDs, free text, timestamps) are never checked.
      - High-cardinality columns (> high_cardinality_skip unique values) skip
        value_drift and cardinality_drift -- avoids false positives on session_id / block IDs.
      - min_std guards sigma math against near-zero std on columns like latency_ms
        where many values are 0.
      - min_category_frequency: new category value must appear in >= this fraction of
        the current batch to fire value_drift. Prevents single rare occurrences (one
        HEAD request, one new log template) from triggering false alarms.
      - min_relative_shift: distribution_drift additionally requires the absolute mean
        shift to be >= this fraction of the baseline mean. Prevents firing when the
        baseline mean is near zero and the min_std floor causes artificially low sigma.
    """

    def __init__(
        self,
        missing_threshold: float = 0.05,
        mean_shift_sigma: float = 4.0,
        min_std: float = 1.0,
        high_cardinality_skip: int = 50,
        rate_alert_multiplier: float = 2.5,
        rate_alert_min_delta: float = 0.05,
        min_category_frequency: float = 0.02,
        min_relative_shift: float = 0.0,
        exclude_columns: Optional[Set[str]] = None,
        binary_columns: Optional[Set[str]] = None,
    ):
        self.missing_threshold = missing_threshold
        self.mean_shift_sigma = mean_shift_sigma
        self.min_std = min_std
        self.high_cardinality_skip = high_cardinality_skip
        self.rate_alert_multiplier = rate_alert_multiplier
        self.rate_alert_min_delta = rate_alert_min_delta
        self.min_category_frequency = min_category_frequency
        self.min_relative_shift = min_relative_shift

        # Columns to completely skip (IDs, free text, raw timestamps)
        self.exclude_columns: Set[str] = exclude_columns or {
            "event_id", "session_id", "entity_id", "message",
            "timestamp", "template_id",
        }

        # Boolean/rate columns — use rate-based alerting instead of sigma
        self.binary_columns: Set[str] = binary_columns or {
            "error_flag", "anomaly_label", "sla_breach",
        }

        self.baseline: Dict[str, Any] = {}
        self.events: List[Dict[str, Any]] = []

    # ── baseline ─────────────────────────────────────────────────────────────

    def fit_baseline(self, df: pd.DataFrame) -> None:
        active = [c for c in df.columns.astype(str) if c not in self.exclude_columns]
        numeric = df[active].select_dtypes(include="number").columns.astype(str).tolist()
        categorical = [c for c in active if c not in numeric]

        # Separate binary columns from continuous numeric
        continuous = [c for c in numeric if c not in self.binary_columns]
        binary = [c for c in numeric if c in self.binary_columns]

        self.baseline = {
            "columns": set(active),
            "dtypes": {str(c): str(t) for c, t in df[active].dtypes.items()},
            "missing": df[active].isna().mean().to_dict(),
            "continuous": {
                c: {
                    "mean": float(pd.to_numeric(df[c], errors="coerce").mean()),
                    "std": float(pd.to_numeric(df[c], errors="coerce").std() or 0.0),
                }
                for c in continuous
            },
            # Binary columns: store rate (mean of 0/1)
            "binary_rates": {
                c: float(pd.to_numeric(df[c], errors="coerce").fillna(0).mean())
                for c in binary
            },
            # Low-cardinality categoricals only
            "categories": {
                c: set(df[c].dropna().astype(str).unique().tolist())
                for c in categorical
                if df[c].nunique(dropna=True) <= self.high_cardinality_skip
            },
            "cardinality": {
                c: int(df[c].nunique(dropna=True))
                for c in categorical
                if df[c].nunique(dropna=True) <= self.high_cardinality_skip
            },
        }

    # ── internal helpers ──────────────────────────────────────────────────────

    def _event(
        self,
        drift_type: str,
        column: str,
        severity: str,
        baseline_value: Any,
        current_value: Any,
        recommended_action: str,
    ) -> Dict[str, Any]:
        return {
            "drift_type": drift_type,
            "column": column,
            "severity": severity,
            "detected": True,
            "baseline_value": str(baseline_value),
            "current_value": str(current_value),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "recommended_action": recommended_action,
        }

    def _check_binary_rate(self, col: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """Rate-based alert for binary columns (error_flag, sla_breach, anomaly_label)."""
        baseline_rate = self.baseline["binary_rates"].get(col)
        if baseline_rate is None:
            return None
        cur_rate = float(pd.to_numeric(df[col], errors="coerce").fillna(0).mean())
        delta = cur_rate - baseline_rate
        # Alert only when rate both doubled (multiplier) AND increased by min_delta points
        if (
            delta >= self.rate_alert_min_delta
            and cur_rate >= self.rate_alert_multiplier * max(baseline_rate, 1e-6)
        ):
            severity = "high" if delta > 0.15 else "medium"
            return self._event(
                "distribution_drift", col, severity,
                f"rate={baseline_rate:.4f}",
                f"rate={cur_rate:.4f} (+{delta:.4f})",
                "investigate_error_rate_spike",
            )
        return None

    # ── main detection ────────────────────────────────────────────────────────

    def detect(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if not self.baseline:
            self.fit_baseline(df)
            return []

        events: List[Dict[str, Any]] = []
        active_cols = [c for c in df.columns.astype(str) if c not in self.exclude_columns]
        current_cols = set(active_cols)
        missing_cols = self.baseline["columns"] - current_cols
        new_cols = current_cols - self.baseline["columns"]

        # Schema rename detection
        for col in sorted(missing_cols):
            similar = [c for c in new_cols if col.lower().split("_")[0] in c.lower()]
            events.append(self._event(
                "schema_rename", col, "high", col,
                similar or "missing", "reprofile_and_remap_column",
            ))

        for col in sorted(current_cols & self.baseline["columns"]):
            # ── dtype drift ──────────────────────────────────────────────────
            base_dtype = self.baseline["dtypes"].get(col, "")
            cur_dtype = str(df[col].dtype)
            if base_dtype != cur_dtype and (
                "float" in base_dtype or "int" in base_dtype or cur_dtype == "object"
            ):
                parse_ok = (
                    pd.to_numeric(df[col], errors="coerce").notna().mean()
                    if cur_dtype == "object" else 1.0
                )
                severity = "medium" if parse_ok < 0.95 else "low"
                events.append(self._event(
                    "type_drift", col, severity,
                    base_dtype, f"{cur_dtype}; parse_ok={parse_ok:.3f}",
                    "coerce_or_reprofile",
                ))

            # ── missingness drift ────────────────────────────────────────────
            base_missing = float(self.baseline["missing"].get(col, 0.0))
            cur_missing = float(df[col].isna().mean())
            if cur_missing - base_missing >= self.missing_threshold:
                events.append(self._event(
                    "missingness_drift", col, "medium",
                    f"{base_missing:.4f}", f"{cur_missing:.4f}",
                    "flag_missingness_and_validate_contract",
                ))

            # ── categorical: value drift (skip high-cardinality) ─────────────
            if col in self.baseline["categories"]:
                base_values = self.baseline["categories"][col]
                cur_series = df[col].dropna().astype(str)
                cur_values = set(cur_series.unique().tolist())
                unseen = cur_values - base_values
                if unseen:
                    # Only fire if at least one unseen value exceeds the frequency threshold.
                    # A single HEAD request or a rare log template appearing once in 500
                    # events (0.2%) is noise, not drift.
                    n = max(len(cur_series), 1)
                    freq = {v: (cur_series == v).sum() / n for v in unseen}
                    significant = {v for v, f in freq.items() if f >= self.min_category_frequency}
                    if significant:
                        events.append(self._event(
                            "value_drift", col, "low",
                            sorted(base_values)[:5], sorted(significant)[:5],
                            "map_or_mark_unknown_category",
                        ))
                base_card = int(self.baseline["cardinality"].get(col, 0))
                cur_card = int(df[col].nunique(dropna=True))
                if base_card and abs(cur_card - base_card) / max(base_card, 1) > 0.5:
                    events.append(self._event(
                        "cardinality_drift", col, "low",
                        base_card, cur_card,
                        "inspect_category_cardinality",
                    ))

            # ── binary/rate columns (error_flag, sla_breach, anomaly_label) ──
            if col in self.binary_columns and col in self.baseline["binary_rates"]:
                alert = self._check_binary_rate(col, df)
                if alert:
                    events.append(alert)
                continue  # don't also run sigma test on binary cols

            # ── continuous numeric: sigma-based distribution drift ────────────
            if col in self.baseline["continuous"]:
                cur = pd.to_numeric(df[col], errors="coerce")
                cur_mean = float(cur.mean()) if cur.notna().any() else 0.0
                base = self.baseline["continuous"][col]
                base_mean = float(base.get("mean", 0.0))
                std = max(float(base.get("std") or 0.0), self.min_std)
                abs_shift = abs(cur_mean - base_mean)
                sigma_threshold = self.mean_shift_sigma * std
                # Optional relative guard: when baseline mean is near zero, the min_std
                # floor makes sigma very tight. Require shift >= min_relative_shift * |base_mean|
                # to avoid firing on noise amplified by a near-zero baseline mean.
                relative_ok = (
                    self.min_relative_shift == 0.0
                    or abs_shift >= self.min_relative_shift * max(abs(base_mean), 1e-6)
                )
                if abs_shift > sigma_threshold and relative_ok:
                    events.append(self._event(
                        "distribution_drift", col, "medium",
                        round(base_mean, 4), round(cur_mean, 4),
                        "validate_distribution_shift",
                    ))

        self.events.extend(events)
        return events

    def reset(self) -> None:
        self.baseline = {}
        self.events = []

    def summarize_events(self) -> pd.DataFrame:
        return pd.DataFrame(self.events)
