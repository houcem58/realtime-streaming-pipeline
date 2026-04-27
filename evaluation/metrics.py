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
Drift Detection Evaluation Metrics
------------------------------------
Computes window-level binary classification metrics:
  F1, Precision, Recall, Accuracy, False-Positive Rate, False-Negative Rate

Ground truth  = DriftInjector.get_schedule() (the injection schedule)
Predicted     = windows where DriftDetector.detect() returned ≥1 event

Usage
-----
    from evaluation.metrics import DriftMetrics
    m = DriftMetrics(y_true=[0,1,0,1,...], y_pred=[0,1,0,0,...])
    print(m.report())
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Sequence


@dataclass
class DriftMetrics:
    """Window-level binary classification metrics for drift detection."""

    tp: int = 0
    fp: int = 0
    tn: int = 0
    fn: int = 0

    def __init__(
        self,
        y_true: Sequence[int | bool],
        y_pred: Sequence[int | bool],
    ):
        if len(y_true) != len(y_pred):
            raise ValueError(
                f"y_true length {len(y_true)} != y_pred length {len(y_pred)}"
            )
        tp = fp = tn = fn = 0
        for t, p in zip(y_true, y_pred):
            t, p = bool(t), bool(p)
            if t and p:
                tp += 1
            elif not t and p:
                fp += 1
            elif not t and not p:
                tn += 1
            else:
                fn += 1
        self.tp = tp
        self.fp = fp
        self.tn = tn
        self.fn = fn

    @property
    def precision(self) -> float:
        denom = self.tp + self.fp
        return self.tp / denom if denom else 0.0

    @property
    def recall(self) -> float:
        denom = self.tp + self.fn
        return self.tp / denom if denom else 0.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if (p + r) else 0.0

    @property
    def accuracy(self) -> float:
        total = self.tp + self.fp + self.tn + self.fn
        return (self.tp + self.tn) / total if total else 0.0

    @property
    def fpr(self) -> float:
        """False positive rate (fall-out)."""
        denom = self.fp + self.tn
        return self.fp / denom if denom else 0.0

    @property
    def fnr(self) -> float:
        """False negative rate (miss rate)."""
        denom = self.fn + self.tp
        return self.fn / denom if denom else 0.0

    @property
    def n_windows(self) -> int:
        return self.tp + self.fp + self.tn + self.fn

    def report(self, dataset: str = "", indent: bool = True) -> str:
        sep = "  " if indent else ""
        lines = []
        if dataset:
            lines.append(f"Dataset: {dataset}")
        lines += [
            f"{sep}Windows evaluated : {self.n_windows}",
            f"{sep}True Positives    : {self.tp}",
            f"{sep}False Positives   : {self.fp}",
            f"{sep}True Negatives    : {self.tn}",
            f"{sep}False Negatives   : {self.fn}",
            f"{sep}Precision         : {self.precision:.4f}",
            f"{sep}Recall            : {self.recall:.4f}",
            f"{sep}F1 Score          : {self.f1:.4f}",
            f"{sep}Accuracy          : {self.accuracy:.4f}",
            f"{sep}FPR               : {self.fpr:.4f}",
            f"{sep}FNR               : {self.fnr:.4f}",
        ]
        return "\n".join(lines)

    def to_dict(self, dataset: str = "") -> dict:
        d = asdict(self)
        d.update({
            "dataset": dataset,
            "n_windows": self.n_windows,
            "precision": round(self.precision, 4),
            "recall": round(self.recall, 4),
            "f1": round(self.f1, 4),
            "accuracy": round(self.accuracy, 4),
            "fpr": round(self.fpr, 4),
            "fnr": round(self.fnr, 4),
        })
        return d

    def to_json(self, dataset: str = "") -> str:
        return json.dumps(self.to_dict(dataset), indent=2)
