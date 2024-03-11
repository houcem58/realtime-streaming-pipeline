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
Olist E-Commerce Adapter
------------------------
Normalizes the Brazilian Olist marketplace dataset into the standard
streaming event schema. No drift injection — drift is applied at
runtime by DriftInjector in the producer.

Primary source: olist_orders_dataset.csv
Joined with: olist_order_payments_dataset.csv (payment_value)

Columns used:
  order_id, customer_id, order_status, order_purchase_timestamp,
  order_delivered_customer_date, order_estimated_delivery_date,
  payment_value

Realistic drift types for this domain:
  - cancellation_spike  : order_status "canceled" rate spikes
  - delivery_delay      : actual delivery > estimated delivery (SLA breach)
  - payment_shift       : payment value distribution shifts
  - status_distribution : new order status values appear
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


def load_olist(
    orders_path: str | Path,
    payments_path: str | Path | None = None,
    target_events: int = 15000,
    duration_days: int = 60,
) -> pd.DataFrame:
    """
    Load and normalize Olist orders to the standard streaming schema.

    Returns a DataFrame sorted by timestamp, ready for streaming.
    All events have anomaly_label=0 — drift is injected at runtime.

    Normalization rules:
      - order_status in {canceled, unavailable} → error_flag=True, severity=WARN
      - delivery_delay_minutes = actual_delivery - estimated_delivery (positive = late)
      - sla_breach = True if delivered after estimated date
      - latency_ms = delivery_delay_minutes * 60000 (proxy)
    """
    orders_path = Path(orders_path)
    df = pd.read_csv(orders_path)
    df.columns = [c.strip() for c in df.columns]

    # Parse timestamps
    for col in ["order_purchase_timestamp", "order_delivered_customer_date",
                "order_estimated_delivery_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    df = df.dropna(subset=["order_purchase_timestamp", "order_status"]).sort_values(
        "order_purchase_timestamp"
    ).reset_index(drop=True)

    start = df["order_purchase_timestamp"].iloc[0].floor("1D")
    df = df[df["order_purchase_timestamp"] <= start + pd.Timedelta(days=duration_days)]
    df = _evenly_sample(df, target_events)

    # Join payments if available
    if payments_path is not None:
        pay = pd.read_csv(Path(payments_path))
        pay_agg = pay.groupby("order_id")["payment_value"].sum().reset_index()
        df = df.merge(pay_agg, on="order_id", how="left")
    else:
        df["payment_value"] = 0.0

    df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce").fillna(0.0)

    # Delivery delay (minutes)
    has_actual = df["order_delivered_customer_date"].notna()
    has_estimated = df["order_estimated_delivery_date"].notna()
    delay_min = pd.Series(0.0, index=df.index)
    mask = has_actual & has_estimated
    delay_min[mask] = (
        (df.loc[mask, "order_delivered_customer_date"] -
         df.loc[mask, "order_estimated_delivery_date"]).dt.total_seconds() / 60.0
    )

    error_statuses = {"canceled", "unavailable"}
    is_error = df["order_status"].str.lower().isin(error_statuses)
    sla_breach = delay_min > 0

    return pd.DataFrame({
        "timestamp":              df["order_purchase_timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "event_id":               df["order_id"].astype(str),
        "source_dataset":         "olist",
        "domain":                 "ecommerce",
        "entity_id":              df["customer_id"].astype(str).str[:30],
        "event_type":             df["order_status"].str.lower(),
        "severity":               is_error.map({True: "WARN", False: "INFO"}),
        "status":                 df["order_status"].str.lower(),
        "error_flag":             is_error,
        "anomaly_label":          0,
        "latency_ms":             (delay_min.clip(lower=0) * 60000.0).round(3),
        "amount":                 df["payment_value"].round(2),
        "region":                 "brazil",
        "channel":                "marketplace",
        "category":               df["order_status"].str.lower(),
        "delivery_delay_minutes": delay_min.round(2),
        "sla_breach":             sla_breach,
    }).sort_values("timestamp").reset_index(drop=True)
