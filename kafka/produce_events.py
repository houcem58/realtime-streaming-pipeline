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
Produce events from a CSV file to a Kafka topic.

Usage:
    python kafka/produce_events.py --events data/events_sample.csv
    python kafka/produce_events.py --events data/events_sample.csv --speedup 120 --topic streaming.raw_events
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import pandas as pd

from kafka.ensure_topics import ensure_topics


def _json_default(value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)


def produce_events(
    events_path: str | Path,
    bootstrap_servers: str,
    topic: str,
    speedup: float = 60.0,
    max_events: int | None = None,
) -> dict[str, Any]:
    """
    Replay events from CSV to Kafka, preserving original timestamps at `speedup` factor.

    Parameters
    ----------
    speedup : float
        Time compression factor. speedup=60 means 1 hour of events replays in 1 minute.
    """
    try:
        from confluent_kafka import Producer
    except Exception as exc:
        raise RuntimeError("confluent-kafka is required. Run: pip install confluent-kafka") from exc

    ensure_topics(bootstrap_servers, extra_topics=[topic])

    df = pd.read_csv(events_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")
    df = df.sort_values("timestamp").reset_index(drop=True)
    if max_events is not None:
        df = df.head(max_events)

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    sent = 0
    previous_ts = None
    wall_start = time.perf_counter()

    for _, row in df.iterrows():
        current_ts = row["timestamp"]
        if previous_ts is not None:
            sleep_s = max((current_ts - previous_ts).total_seconds() / max(speedup, 1e-9), 0.0)
            if sleep_s > 0:
                time.sleep(min(sleep_s, 0.2))
        payload = row.where(pd.notna(row), None).to_dict()
        payload["timestamp"] = current_ts.isoformat()
        producer.produce(topic, json.dumps(payload, default=_json_default).encode("utf-8"))
        producer.poll(0)
        previous_ts = current_ts
        sent += 1

    producer.flush(20)
    return {
        "events_sent": sent,
        "topic": topic,
        "bootstrap_servers": bootstrap_servers,
        "speedup": speedup,
        "wall_clock_seconds": round(time.perf_counter() - wall_start, 3),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay CSV events to Kafka.")
    parser.add_argument("--events", required=True, help="Path to events CSV")
    parser.add_argument("--bootstrap_servers", default="localhost:9092")
    parser.add_argument("--topic", default="streaming.raw_events")
    parser.add_argument("--speedup", type=float, default=60.0, help="Time compression factor")
    parser.add_argument("--max_events", type=int, default=None)
    args = parser.parse_args()
    result = produce_events(args.events, args.bootstrap_servers, args.topic, args.speedup, args.max_events)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
