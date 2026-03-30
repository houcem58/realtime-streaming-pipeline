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
NASA HTTP Log Adapter
---------------------
Normalizes NASA Kennedy Space Center HTTP server logs into the
standard streaming event schema. No drift injection — drift is
applied at runtime by DriftInjector in the producer.

Source: nasa_http.csv  (host, timestamp, method, path, status, bytes)
Reference: NASA Kennedy Space Center HTTP server logs, Aug-Sep 1995
           https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
"""

from __future__ import annotations

import math
import re
from pathlib import Path

import pandas as pd

MONTH_MAP = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}
TS_RE = re.compile(r"(\d{2})/(\w{3})/(\d{4}):(\d{2}:\d{2}:\d{2})\s([+-]\d{4})")


def _parse_ts(ts_str: str) -> pd.Timestamp | None:
    m = TS_RE.match(str(ts_str).strip())
    if not m:
        return None
    day, mon, year, time_part, tz = m.groups()
    mon_num = MONTH_MAP.get(mon, "01")
    sign = 1 if tz[0] == "+" else -1
    offset = pd.Timedelta(hours=sign * int(tz[1:3]), minutes=sign * int(tz[3:5]))
    try:
        return (pd.Timestamp(f"{year}-{mon_num}-{day}T{time_part}") - offset).tz_localize("UTC")
    except Exception:
        return None


def _evenly_sample(df: pd.DataFrame, target: int) -> pd.DataFrame:
    if target <= 0 or len(df) <= target:
        return df.copy()
    step = len(df) / float(target)
    idx = sorted({min(int(math.floor(i * step)), len(df) - 1) for i in range(target)})
    return df.iloc[idx].copy().reset_index(drop=True)


def load_nasa(
    raw_path: str | Path,
    target_events: int = 20000,
    duration_minutes: int = 60,
) -> pd.DataFrame:
    """
    Load and normalize NASA HTTP logs to the standard streaming schema.

    Returns a DataFrame sorted by timestamp, ready for streaming.
    All events have anomaly_label=0 — drift is injected at runtime.
    """
    raw_path = Path(raw_path)
    df = pd.read_csv(raw_path)
    df.columns = [c.strip() for c in df.columns]
    df = df.dropna(subset=["timestamp"]).reset_index(drop=True)
    df["_ts"] = df["timestamp"].apply(_parse_ts)
    df = df.dropna(subset=["_ts"]).sort_values("_ts").reset_index(drop=True)

    start = df["_ts"].iloc[0].floor("1min")
    df = df[df["_ts"] <= start + pd.Timedelta(minutes=duration_minutes)]
    df = _evenly_sample(df, target_events)

    df["status"] = pd.to_numeric(df["status"], errors="coerce").fillna(200).astype(int)
    df["bytes"] = pd.to_numeric(df["bytes"], errors="coerce").fillna(0.0)

    return pd.DataFrame({
        "timestamp":              df["_ts"].dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "event_id":               [f"nasa_{i:09d}" for i in range(len(df))],
        "source_dataset":         "nasa_http",
        "domain":                 "http",
        "entity_id":              df["host"].astype(str).str[:50],
        "event_type":             df["method"].astype(str),
        "severity":               df["status"].apply(
                                      lambda s: "ERROR" if s >= 500 else ("WARN" if s >= 400 else "INFO")
                                  ),
        "status":                 df["status"].astype(str),
        "error_flag":             (df["status"] >= 400),
        "anomaly_label":          0,
        "latency_ms":             (df["bytes"] / 1024.0).round(3),
        "bytes":                  df["bytes"],
        "region":                 "nasa_http",
        "channel":                "http",
        "category":               (df["status"] // 100).astype(str) + "xx",
        "delivery_delay_minutes": 0.0,
        "sla_breach":             (df["status"] >= 500),
    }).sort_values("timestamp").reset_index(drop=True)
