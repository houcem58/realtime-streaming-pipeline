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
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd


HDFS_LINE_RE = re.compile(r"^(?P<date>\d{6})\s+(?P<time>\d{6})\s+(?P<pid>\d+)\s+(?P<severity>\w+)\s+(?P<component>[^:]+):\s+(?P<message>.*)$")
BLOCK_RE = re.compile(r"(blk_-?\d+)")
IP_RE = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b")
NUM_RE = re.compile(r"\b\d+\b")
PATH_RE = re.compile(r"(/\S+)")
SPACE_RE = re.compile(r"\s+")
ERROR_RE = re.compile(r"(exception|error|fail|killed|corrupt|terminated)", re.IGNORECASE)


def _normalize_message(message: str) -> str:
    normalized = BLOCK_RE.sub("<BLOCK>", message)
    normalized = IP_RE.sub("<IP>", normalized)
    normalized = PATH_RE.sub("<PATH>", normalized)
    normalized = NUM_RE.sub("<NUM>", normalized)
    return SPACE_RE.sub(" ", normalized).strip()


def _severity_from_ratio(ratio: float) -> str:
    if ratio >= 0.15:
        return "high"
    if ratio >= 0.05:
        return "medium"
    return "low"


def _read_label_map(label_path: Path) -> dict[str, str]:
    labels = pd.read_csv(label_path)
    return dict(zip(labels["BlockId"].astype(str), labels["Label"].astype(str)))


def _parse_sequence(text: str) -> list[str]:
    cleaned = str(text).strip()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1]
    if not cleaned.strip():
        return []
    return [part.strip() for part in cleaned.split(",") if part.strip()]


def _parse_float_sequence(text: str) -> list[float]:
    return [float(value) for value in _parse_sequence(text)]


def _evenly_sample(df: pd.DataFrame, target_rows: int) -> pd.DataFrame:
    if target_rows <= 0 or len(df) <= target_rows:
        return df.copy()
    step = len(df) / float(target_rows)
    indices = sorted({min(int(math.floor(i * step)), len(df) - 1) for i in range(target_rows)})
    return df.iloc[indices].copy().reset_index(drop=True)


def build_hdfs_trace_replay_stream(
    event_traces_path: str | Path,
    templates_path: str | Path,
    out_events: str | Path,
    out_labels: str | Path,
    out_schema: str | Path,
    duration_minutes: int = 60,
    window_size_minutes: int = 1,
    sessions_per_window: int = 14,
    anomaly_session_ratio: float = 0.35,
    anomaly_burst_every: int = 6,
    anomaly_burst_length: int = 2,
) -> dict[str, Any]:
    event_traces_path = Path(event_traces_path)
    templates_path = Path(templates_path)
    out_events = Path(out_events)
    out_labels = Path(out_labels)
    out_schema = Path(out_schema)
    out_events.parent.mkdir(parents=True, exist_ok=True)
    out_labels.parent.mkdir(parents=True, exist_ok=True)
    out_schema.parent.mkdir(parents=True, exist_ok=True)

    traces = pd.read_csv(event_traces_path, usecols=["BlockId", "Label", "Features", "TimeInterval"])
    templates = pd.read_csv(templates_path)
    template_map = dict(zip(templates["EventId"].astype(str), templates["EventTemplate"].astype(str)))

    normal_traces = traces[traces["Label"].astype(str).str.lower().isin({"success", "normal"})].reset_index(drop=True)
    anomaly_traces = traces[~traces["Label"].astype(str).str.lower().isin({"success", "normal"})].reset_index(drop=True)
    if normal_traces.empty or anomaly_traces.empty:
        raise SystemExit("HDFS trace replay requires both normal and anomalous traces.")

    start_time = pd.Timestamp("2008-11-09T00:00:00Z")
    end_time = start_time + pd.Timedelta(minutes=duration_minutes)
    window_count = int(duration_minutes / max(window_size_minutes, 1))
    slot_ms = int((window_size_minutes * 60_000) / max(sessions_per_window + 1, 1))

    rows: list[dict[str, Any]] = []
    normal_idx = 0
    anomaly_idx = 0
    session_counter = 0

    for window_idx in range(window_count):
        current_start = start_time + pd.Timedelta(minutes=window_idx * window_size_minutes)
        window_is_anomalous = (window_idx % anomaly_burst_every) >= (anomaly_burst_every - anomaly_burst_length)
        anomaly_sessions = int(math.ceil(sessions_per_window * anomaly_session_ratio)) if window_is_anomalous else 0
        normal_sessions = sessions_per_window - anomaly_sessions
        schedule: list[tuple[pd.Series, int]] = []

        for _ in range(normal_sessions):
            schedule.append((normal_traces.iloc[normal_idx % len(normal_traces)], 0))
            normal_idx += 1
        for _ in range(anomaly_sessions):
            schedule.append((anomaly_traces.iloc[anomaly_idx % len(anomaly_traces)], 1))
            anomaly_idx += 1

        for slot_idx, (trace_row, anomaly_flag) in enumerate(schedule, start=1):
            session_counter += 1
            session_start = current_start + pd.Timedelta(milliseconds=slot_idx * slot_ms)
            features = _parse_sequence(trace_row["Features"])
            intervals = _parse_float_sequence(trace_row["TimeInterval"])
            event_time = session_start
            session_id = str(trace_row["BlockId"])
            for event_idx, event_id in enumerate(features):
                if event_idx > 0:
                    delta_ms = intervals[event_idx - 1] if event_idx - 1 < len(intervals) else 0.0
                    event_time = event_time + pd.Timedelta(milliseconds=max(delta_ms, 0.0))
                if event_time >= end_time:
                    continue
                message = template_map.get(event_id, event_id)
                error_flag = bool(ERROR_RE.search(message))
                severity = "WARN" if error_flag else "INFO"
                rows.append(
                    {
                        "timestamp": event_time,
                        "event_id": f"loghub_hdfs_replay_{session_counter:06d}_{event_idx:03d}",
                        "source_dataset": "loghub_hdfs",
                        "session_id": session_id,
                        "component": "hdfs_trace_replay",
                        "event_type": event_id,
                        "severity": severity,
                        "message": message,
                        "error_flag": bool(error_flag),
                        "template_id": event_id,
                        "anomaly_label": int(anomaly_flag),
                        "domain": "hdfs",
                        "entity_id": session_id,
                        "status": severity,
                        "region": "hdfs",
                        "channel": "loghub",
                        "category": event_id,
                        "latency_ms": float(intervals[event_idx - 1]) if event_idx > 0 and event_idx - 1 < len(intervals) else 0.0,
                        "delivery_delay_minutes": 0.0,
                        "sla_breach": False,
                    }
                )

    events_df = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)
    events_df["timestamp"] = events_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S%z").str.replace(
        r"(\+0000)$", "+00:00", regex=True
    )
    events_df.to_csv(out_events, index=False)

    label_rows: list[dict[str, Any]] = []
    for window_idx in range(window_count):
        current_start = start_time + pd.Timedelta(minutes=window_idx * window_size_minutes)
        current_end = current_start + pd.Timedelta(minutes=window_size_minutes)
        window_df = pd.DataFrame(rows)
        window_df = window_df[(window_df["timestamp"] >= current_start) & (window_df["timestamp"] < current_end)]
        anomaly_count = int(window_df["anomaly_label"].sum()) if not window_df.empty else 0
        total_count = int(len(window_df))
        has_anomaly = anomaly_count > 0
        anomaly_ratio = float(anomaly_count / total_count) if total_count else 0.0
        label_rows.append(
            {
                "window_id": f"hdfs_window_{window_idx:04d}",
                "start_time": current_start.isoformat(),
                "end_time": current_end.isoformat(),
                "label_source": "loghub_hdfs_trace_labels_replayed_into_time_windows",
                "has_anomaly": bool(has_anomaly),
                "anomaly_count": anomaly_count,
                "expected_signal": "anomaly_window" if has_anomaly else "clean_window",
                "drift_type": "anomaly_window" if has_anomaly else "none",
                "severity": _severity_from_ratio(anomaly_ratio),
                "raw_event_count": total_count,
            }
        )
    labels_df = pd.DataFrame(label_rows)
    labels_df.to_csv(out_labels, index=False)

    schema = {
        "dataset": "loghub_hdfs",
        "source_format": "trace_replay_from_public_hdfs_sequences",
        "duration_minutes": duration_minutes,
        "window_size_minutes": window_size_minutes,
        "raw_trace_count_used": int(window_count * sessions_per_window),
        "replayed_event_count": int(len(events_df)),
        "event_columns": list(events_df.columns),
        "label_columns": list(labels_df.columns),
        "clean_windows": int((~labels_df["has_anomaly"]).sum()),
        "anomaly_windows": int(labels_df["has_anomaly"].sum()),
    }
    out_schema.write_text(json.dumps(schema, indent=2), encoding="utf-8")
    return schema


def build_hdfs_public_log_stream(
    raw_log_path: str | Path,
    label_path: str | Path,
    out_events: str | Path,
    out_labels: str | Path,
    out_schema: str | Path,
    duration_minutes: int = 60,
    window_size_minutes: int = 1,
    target_replay_events: int = 15000,
) -> dict[str, Any]:
    raw_log_path = Path(raw_log_path)
    label_path = Path(label_path)
    out_events = Path(out_events)
    out_labels = Path(out_labels)
    out_schema = Path(out_schema)
    out_events.parent.mkdir(parents=True, exist_ok=True)
    out_labels.parent.mkdir(parents=True, exist_ok=True)
    out_schema.parent.mkdir(parents=True, exist_ok=True)

    label_map = _read_label_map(label_path)
    rows: list[dict[str, Any]] = []
    template_lookup: dict[str, str] = {}
    template_counter = 0
    start_time = None
    end_time = None

    with raw_log_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            match = HDFS_LINE_RE.match(line.rstrip("\n"))
            if not match:
                continue
            payload = match.groupdict()
            timestamp = pd.to_datetime(
                f"{payload['date']}{payload['time']}",
                utc=True,
                format="%y%m%d%H%M%S",
            )
            if start_time is None:
                start_time = timestamp
                end_time = start_time + pd.Timedelta(minutes=duration_minutes)
            if timestamp >= end_time:
                break

            message = payload["message"].strip()
            normalized = _normalize_message(message)
            if normalized not in template_lookup:
                template_counter += 1
                template_lookup[normalized] = f"HDFS_TPL_{template_counter:04d}"
            block_match = BLOCK_RE.search(message)
            session_id = block_match.group(1) if block_match else f"line_{line_number:09d}"
            anomaly_label = 1 if label_map.get(session_id) == "Anomaly" else 0
            error_flag = payload["severity"].upper() in {"WARN", "ERROR", "FATAL"} or bool(ERROR_RE.search(message))
            rows.append(
                {
                    "timestamp": timestamp,
                    "event_id": f"loghub_hdfs_{line_number:09d}",
                    "source_dataset": "loghub_hdfs",
                    "session_id": session_id,
                    "component": payload["component"].strip(),
                    "event_type": template_lookup[normalized],
                    "severity": payload["severity"].upper(),
                    "message": message,
                    "error_flag": bool(error_flag),
                    "template_id": template_lookup[normalized],
                    "anomaly_label": int(anomaly_label),
                    "domain": "hdfs",
                    "entity_id": session_id,
                    "status": payload["severity"].upper(),
                    "region": payload["component"].split("$")[0].strip(),
                    "channel": "loghub",
                    "category": template_lookup[normalized],
                    "delivery_delay_minutes": 0.0,
                    "sla_breach": False,
                }
            )

    if not rows or start_time is None or end_time is None:
        raise SystemExit(f"No HDFS rows were parsed from {raw_log_path}")

    raw_df = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)
    sampled_df = _evenly_sample(raw_df, target_replay_events)
    sampled_df["latency_ms"] = (
        sampled_df.groupby("session_id")["timestamp"].diff().dt.total_seconds().fillna(0.0) * 1000.0
    ).round(3)

    sampled_df["timestamp"] = sampled_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S%z").str.replace(
        r"(\+0000)$", "+00:00", regex=True
    )
    sampled_df.to_csv(out_events, index=False)

    window_start = start_time.floor(f"{window_size_minutes}min")
    window_edges = pd.date_range(
        start=window_start,
        end=end_time.ceil(f"{window_size_minutes}min"),
        freq=f"{window_size_minutes}min",
        inclusive="left",
        tz="UTC",
    )
    label_rows: list[dict[str, Any]] = []
    for idx, current_start in enumerate(window_edges):
        current_end = current_start + pd.Timedelta(minutes=window_size_minutes)
        window_df = raw_df[(raw_df["timestamp"] >= current_start) & (raw_df["timestamp"] < current_end)]
        anomaly_count = int(window_df["anomaly_label"].sum()) if not window_df.empty else 0
        total_count = int(len(window_df))
        has_anomaly = anomaly_count > 0
        anomaly_ratio = float(anomaly_count / total_count) if total_count else 0.0
        label_rows.append(
            {
                "window_id": f"hdfs_window_{idx:04d}",
                "start_time": current_start.isoformat(),
                "end_time": current_end.isoformat(),
                "label_source": "loghub_hdfs_block_id_labels_aggregated_to_time_windows",
                "has_anomaly": bool(has_anomaly),
                "anomaly_count": anomaly_count,
                "expected_signal": "anomaly_window" if has_anomaly else "clean_window",
                "drift_type": "anomaly_window" if has_anomaly else "none",
                "severity": _severity_from_ratio(anomaly_ratio),
                "raw_event_count": total_count,
            }
        )
    pd.DataFrame(label_rows).to_csv(out_labels, index=False)

    schema = {
        "dataset": "loghub_hdfs",
        "source_format": "raw_log_plus_block_labels",
        "duration_minutes": duration_minutes,
        "window_size_minutes": window_size_minutes,
        "raw_event_count": int(len(raw_df)),
        "replayed_event_count": int(len(sampled_df)),
        "event_columns": list(sampled_df.columns),
        "label_columns": list(pd.DataFrame(label_rows).columns),
    }
    out_schema.write_text(json.dumps(schema, indent=2), encoding="utf-8")
    return schema


def main() -> None:
    parser = argparse.ArgumentParser(description="Build public-log streaming benchmark files from LogHub HDFS.")
    parser.add_argument("--input_mode", choices=["traces", "raw"], default="traces")
    parser.add_argument("--raw_log", default=None)
    parser.add_argument("--labels", default=None)
    parser.add_argument("--event_traces", default=None)
    parser.add_argument("--templates", default=None)
    parser.add_argument("--out_events", required=True)
    parser.add_argument("--out_labels", required=True)
    parser.add_argument("--out_schema", required=True)
    parser.add_argument("--duration_minutes", type=int, default=60)
    parser.add_argument("--window_size_minutes", type=int, default=1)
    parser.add_argument("--target_replay_events", type=int, default=15000)
    parser.add_argument("--sessions_per_window", type=int, default=14)
    parser.add_argument("--anomaly_session_ratio", type=float, default=0.35)
    parser.add_argument("--anomaly_burst_every", type=int, default=6)
    parser.add_argument("--anomaly_burst_length", type=int, default=2)
    args = parser.parse_args()
    if args.input_mode == "traces":
        result = build_hdfs_trace_replay_stream(
            event_traces_path=args.event_traces,
            templates_path=args.templates,
            out_events=args.out_events,
            out_labels=args.out_labels,
            out_schema=args.out_schema,
            duration_minutes=args.duration_minutes,
            window_size_minutes=args.window_size_minutes,
            sessions_per_window=args.sessions_per_window,
            anomaly_session_ratio=args.anomaly_session_ratio,
            anomaly_burst_every=args.anomaly_burst_every,
            anomaly_burst_length=args.anomaly_burst_length,
        )
    else:
        result = build_hdfs_public_log_stream(
            raw_log_path=args.raw_log,
            label_path=args.labels,
            out_events=args.out_events,
            out_labels=args.out_labels,
            out_schema=args.out_schema,
            duration_minutes=args.duration_minutes,
            window_size_minutes=args.window_size_minutes,
            target_replay_events=args.target_replay_events,
        )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
