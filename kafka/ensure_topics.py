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

import argparse
import json
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "kafka_config.json"

DEFAULT_TOPICS = [
    "streaming.raw_events",
    "streaming.processed_events",
    "streaming.drift_events",
    "streaming.contract_events",
    "streaming.metrics",
    "streaming.errors",
]


def _load_topics() -> list[str]:
    if CONFIG_PATH.exists():
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        return cfg.get("topics", DEFAULT_TOPICS)
    return DEFAULT_TOPICS


def ensure_topics(bootstrap_servers: str = "localhost:9092", extra_topics: list[str] | None = None) -> list[str]:
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
    except Exception as exc:
        raise RuntimeError("Kafka topic creation requires confluent-kafka.") from exc

    topics = _load_topics()
    if extra_topics:
        for t in extra_topics:
            if t not in topics:
                topics.append(t)

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    missing = [t for t in topics if t not in existing]
    if missing:
        futures = admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in missing])
        for topic, future in futures.items():
            try:
                future.result(timeout=10)
            except Exception as exc:
                if "already exists" not in str(exc).lower():
                    raise
    return topics


def main() -> None:
    parser = argparse.ArgumentParser(description="Ensure Kafka topics exist.")
    parser.add_argument("--bootstrap_servers", default="localhost:9092")
    args = parser.parse_args()
    topics = ensure_topics(args.bootstrap_servers)
    print(json.dumps({"topics": topics}, indent=2))


if __name__ == "__main__":
    main()
