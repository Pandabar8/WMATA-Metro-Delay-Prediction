"""Tests for monitor.py state machine logic."""
import json
import pytest
from datetime import datetime, timezone, timedelta


# We test the logic functions in isolation — not the DB or email calls.
# Import after path setup:
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from monitor import load_state, save_state, should_alert, build_alert_subject


# ── load_state / save_state ──────────────────────────────────────────────────

def test_load_state_returns_default_when_file_missing(tmp_path):
    state = load_state(str(tmp_path / "missing.json"))
    assert state["status"] == "ok"
    assert state["last_alert"] is None


def test_save_and_reload_state(tmp_path):
    path = str(tmp_path / "state.json")
    original = {"status": "failing", "since": "2026-03-17T10:00:00+00:00", "last_alert": None}
    save_state(path, original)
    loaded = load_state(path)
    assert loaded == original


# ── should_alert ─────────────────────────────────────────────────────────────

def test_should_alert_when_transition_from_ok_to_failing():
    """First failure: always alert."""
    old = {"status": "ok", "since": "2026-03-17T09:00:00+00:00", "last_alert": None}
    new = {"status": "failing", "since": "2026-03-17T10:00:00+00:00", "last_alert": None}
    assert should_alert(old, new) is True


def test_should_not_alert_when_still_failing_within_one_hour():
    """Already failing, last alert < 1 hour ago: no email."""
    now = datetime.now(timezone.utc)
    last_alert = (now - timedelta(minutes=30)).isoformat()
    old = {"status": "failing", "since": last_alert, "last_alert": last_alert}
    new = {"status": "failing", "since": last_alert, "last_alert": last_alert}
    assert should_alert(old, new) is False


def test_should_alert_when_still_failing_after_one_hour():
    """Already failing, last alert > 1 hour ago: reminder email."""
    now = datetime.now(timezone.utc)
    last_alert = (now - timedelta(minutes=70)).isoformat()
    old = {"status": "failing", "since": last_alert, "last_alert": last_alert}
    new = {"status": "failing", "since": last_alert, "last_alert": last_alert}
    assert should_alert(old, new) is True


def test_should_alert_on_recovery():
    """Pipeline recovered: always alert."""
    old = {"status": "failing", "since": "2026-03-17T10:00:00+00:00", "last_alert": "2026-03-17T10:00:00+00:00"}
    new = {"status": "ok", "since": "2026-03-17T10:18:00+00:00", "last_alert": None}
    assert should_alert(old, new) is True


def test_no_alert_when_ok_and_was_ok():
    """Normal operation: no email."""
    old = {"status": "ok", "since": "2026-03-17T09:00:00+00:00", "last_alert": None}
    new = {"status": "ok", "since": "2026-03-17T09:00:00+00:00", "last_alert": None}
    assert should_alert(old, new) is False


# ── build_alert_subject ───────────────────────────────────────────────────────

def test_build_subject_for_new_failure():
    state = {"status": "failing", "since": "2026-03-17T10:34:00+00:00", "last_alert": None}
    prev = {"status": "ok"}
    subject = build_alert_subject(state, prev)
    assert "[WMATA ALERT]" in subject
    assert "Pipeline" in subject


def test_build_subject_for_recovery():
    state = {"status": "ok", "since": "2026-03-17T10:52:00+00:00", "last_alert": None}
    prev = {"status": "failing", "since": "2026-03-17T10:34:00+00:00"}
    subject = build_alert_subject(state, prev)
    assert "[WMATA RECOVERY]" in subject
