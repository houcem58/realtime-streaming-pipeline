"""Unit tests for DriftMetrics."""
import pytest
from evaluation.metrics import DriftMetrics


def test_perfect_detection():
    m = DriftMetrics([1, 0, 1, 0], [1, 0, 1, 0])
    assert m.tp == 2
    assert m.tn == 2
    assert m.fp == 0
    assert m.fn == 0
    assert m.precision == 1.0
    assert m.recall == 1.0
    assert m.f1 == 1.0
    assert m.accuracy == 1.0


def test_all_false_positives():
    m = DriftMetrics([0, 0, 0], [1, 1, 1])
    assert m.fp == 3
    assert m.tp == 0
    assert m.precision == 0.0
    assert m.fpr == 1.0


def test_all_false_negatives():
    m = DriftMetrics([1, 1, 1], [0, 0, 0])
    assert m.fn == 3
    assert m.recall == 0.0
    assert m.fnr == 1.0


def test_f1_harmonic_mean():
    m = DriftMetrics([1, 1, 0, 0], [1, 0, 1, 0])
    assert m.tp == 1
    assert m.fp == 1
    assert m.fn == 1
    assert pytest.approx(m.f1, abs=1e-4) == 0.5


def test_mismatched_lengths_raises():
    with pytest.raises(ValueError):
        DriftMetrics([1, 0], [1])


def test_empty_inputs():
    m = DriftMetrics([], [])
    assert m.n_windows == 0
    assert m.f1 == 0.0
    assert m.accuracy == 0.0


def test_report_contains_dataset_name():
    m = DriftMetrics([1], [1])
    report = m.report(dataset="nasa_http")
    assert "nasa_http" in report


def test_to_dict_keys():
    m = DriftMetrics([1, 0], [1, 0])
    d = m.to_dict(dataset="test")
    for key in ("tp", "fp", "tn", "fn", "precision", "recall", "f1", "accuracy", "fpr", "fnr"):
        assert key in d
