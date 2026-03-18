"""Tests for daily_summary.py query and formatting logic."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from daily_summary import format_summary, compute_delta


def test_format_summary_basic():
    stats = {
        "date": "2026-03-17",
        "total_rows": 112450,
        "cycles": 218,
        "first_cycle": "05:02 AM",
        "last_cycle": "11:58 PM",
        "incidents": [("Green Line", 2), ("Red Line", 1)],
        "prev_rows": 108250,
    }
    body = format_summary(stats)
    assert "112,450" in body
    assert "218" in body
    assert "Green Line" in body
    assert "+4,200" in body  # delta vs yesterday


def test_format_summary_no_data():
    stats = {
        "date": "2026-03-16",
        "total_rows": 0,
        "cycles": 0,
        "first_cycle": None,
        "last_cycle": None,
        "incidents": [],
        "prev_rows": 0,
    }
    body = format_summary(stats)
    assert "Sin datos" in body


def test_compute_delta_positive():
    assert compute_delta(112450, 108250) == (4200, 3.88)


def test_compute_delta_zero_prev():
    """Avoid division by zero when no previous data."""
    delta, pct = compute_delta(1000, 0)
    assert delta == 1000
    assert pct is None
