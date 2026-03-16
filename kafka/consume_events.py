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
Consume and inspect events from a Kafka topic.

Usage:
    python kafka/consume_events.py --topic streaming.drift_events --limit 20
    python kafka/consume_events.py --topic streaming.raw_events --limit 100 --out results/sample.jsonl
"""

from __future__ import annotations

import argparse
import json
import time
import uuid
from pathlib import Path


def consume_events(
    bootstrap_servers: str,
    topic: str,
    limit: int,
    timeout_s: int = 20,
) -> list[dict]:
    try:
        from confluent_kafka import Consumer
    except Exception as exc:
        raise RuntimeError("confluent-kafka is required. Run: pip install confluent-kafka") from exc

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"streaming-inspect-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    deadline = time.time() + timeout_s
    rows: list[dict] = []
    try:
        while len(rows) < limit and time.time() < deadline:
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            rows.append(json.loads(msg.value().decode("utf-8")))
    finally:
        consumer.close()
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume events from a Kafka topic.")
    parser.add_argument("--bootstrap_servers", default="localhost:9092")
    parser.add_argument("--topic", default="streaming.raw_events")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--timeout_s", type=int, default=20)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    rows = consume_events(args.bootstrap_servers, args.topic, args.limit, args.timeout_s)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    print(json.dumps({"consumed": len(rows), "topic": args.topic}, indent=2))


if __name__ == "__main__":
    main()
