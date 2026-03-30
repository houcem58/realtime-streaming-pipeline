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
Online Retail II Adapter
------------------------
Normalizes the UCI Online Retail II dataset into the standard streaming
event schema. No drift injection — drift is applied at runtime by
DriftInjector in the producer.

Source: online_retail_ii.csv
Columns: Invoice, StockCode, Description, Quantity, InvoiceDate, Price,
         Customer ID, Country

Realistic drift types for this domain:
  - price_spike        : unit price distribution shifts (supplier change, pricing error)
  - return_rate_spike  : cancellation/return rate jumps (negative Quantity)
  - new_country        : new country appears in transactions (market expansion or fraud)
  - quantity_collapse  : bulk orders disappear (supply chain disruption)
"""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd


def _evenly_sample(df: pd.DataFrame, target: int) -> pd.DataFrame:
    if target <= 0 or len(df) <= target:
        return df.copy()
    step = len(df) / float(target)
    idx = sorted({min(int(math.floor(i * step)), len(df) - 1) for i in range(target)})
    return df.iloc[idx].copy().reset_index(drop=True)


def load_retail(
    raw_path: str | Path,
    target_events: int = 20000,
    duration_days: int = 30,
) -> pd.DataFrame:
    """
    Load and normalize Online Retail II transactions to the standard streaming schema.

    Returns a DataFrame sorted by timestamp, ready for streaming.
    All events have anomaly_label=0 — drift is injected at runtime.

    Normalization rules:
      - Negative Quantity = return/cancellation → error_flag=True, severity=WARN
      - Price = unit price (amount = Price * abs(Quantity))
      - latency_ms = 0 (no latency concept in retail transactions)
    """
    raw_path = Path(raw_path)

    # Online Retail II can be large; read with dtype hints for speed
    df = pd.read_csv(
        raw_path,
        dtype={"Invoice": str, "StockCode": str, "Description": str,
               "Country": str, "Customer ID": str},
        on_bad_lines="skip",
        encoding="latin-1",
    )
    df.columns = [c.strip() for c in df.columns]

    # Parse InvoiceDate — format varies: "12/1/10 8:26" or "2010-12-01 08:26:00"
    df["_ts"] = pd.to_datetime(df["InvoiceDate"], infer_datetime_format=True, errors="coerce", utc=True)
    df = df.dropna(subset=["_ts", "Price", "Quantity"]).sort_values("_ts").reset_index(drop=True)

    start = df["_ts"].iloc[0].floor("1D")
    df = df[df["_ts"] <= start + pd.Timedelta(days=duration_days)]
    df = _evenly_sample(df, target_events)

    df["Price"] = pd.to_numeric(df["Price"], errors="coerce").fillna(0.0)
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    is_return = df["Quantity"] < 0

    return pd.DataFrame({
        "timestamp":              df["_ts"].dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "event_id":               df["Invoice"].astype(str) + "_" + df["StockCode"].astype(str),
        "source_dataset":         "online_retail_ii",
        "domain":                 "retail",
        "entity_id":              df["Customer ID"].fillna("unknown").astype(str).str[:30],
        "event_type":             is_return.map({True: "RETURN", False: "PURCHASE"}),
        "severity":               is_return.map({True: "WARN", False: "INFO"}),
        "status":                 is_return.map({True: "return", False: "ok"}),
        "error_flag":             is_return,
        "anomaly_label":          0,
        "latency_ms":             0.0,
        "amount":                 (df["Price"] * df["Quantity"].abs()).round(2),
        "unit_price":             df["Price"],
        "quantity":               df["Quantity"],
        "region":                 df["Country"].fillna("Unknown").astype(str).str[:50],
        "channel":                "retail",
        "category":               df["StockCode"].astype(str).str[:10],
        "delivery_delay_minutes": 0.0,
        "sla_breach":             False,
    }).sort_values("timestamp").reset_index(drop=True)
