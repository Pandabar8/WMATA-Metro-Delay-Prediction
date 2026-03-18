"""
Pipeline health monitor. Run every 10 minutes via cron.
Alerts via email only for failures — covers both missed runs and DB write errors.
"""
import sys
import os
import json
import sqlite3

sys.path.insert(0, os.path.dirname(__file__))

from datetime import datetime, timezone, timedelta
from config import DB_PATH, STATE_FILE


WINDOW_MINUTES = 10
REMINDER_HOURS = 1


# ── State helpers ─────────────────────────────────────────────────────────────

def load_state(path: str = STATE_FILE) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"status": "ok", "since": datetime.now(timezone.utc).isoformat(), "last_alert": None}


def save_state(path: str, state: dict) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f)


def should_alert(old_state: dict, new_state: dict) -> bool:
    """Return True if an email should be sent given the state transition."""
    # Recovery: always alert
    if old_state["status"] == "failing" and new_state["status"] == "ok":
        return True
    # New failure: always alert
    if old_state["status"] == "ok" and new_state["status"] == "failing":
        return True
    # Ongoing failure: alert only if last alert was > REMINDER_HOURS ago
    if new_state["status"] == "failing":
        last = new_state.get("last_alert")
        if last:
            last_dt = datetime.fromisoformat(last)
            if datetime.now(timezone.utc) - last_dt < timedelta(hours=REMINDER_HOURS):
                return False
        return True
    return False


def build_alert_subject(new_state: dict, old_state: dict) -> str:
    if new_state["status"] == "ok":
        now_str = datetime.now(timezone.utc).strftime("%I:%M %p")
        return f"[WMATA RECOVERY] Pipeline restored — {now_str} UTC"
    else:
        since = datetime.fromisoformat(new_state["since"]).strftime("%I:%M %p")
        return f"[WMATA ALERT] Pipeline down — no data since {since} UTC"


def build_alert_body(new_state: dict, old_state: dict) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if new_state["status"] == "ok":
        start = datetime.fromisoformat(old_state["since"])
        duration = datetime.now(timezone.utc) - start
        mins = int(duration.total_seconds() / 60)
        return f"Pipeline recovered at {now}.\nOutage duration: {mins} minutes."
    else:
        return (
            f"No data collected in the last {WINDOW_MINUTES} minutes.\n\n"
            f"Detected at: {now}\n"
            f"Check: /tmp/wmata_pipeline_error.log on the collection host."
        )


# ── Main check ────────────────────────────────────────────────────────────────

def check_pipeline() -> dict:
    """Query DB for recent rows. Returns new state dict."""
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(minutes=WINDOW_MINUTES)).isoformat()

    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM predictions WHERE collected_at >= ?", (cutoff,))
        count = c.fetchone()[0]
        conn.close()
    except Exception as e:
        print(f"[monitor] DB error: {e}")
        return {"status": "failing", "since": now.isoformat(), "last_alert": None}

    if count == 0:
        return {"status": "failing", "since": now.isoformat(), "last_alert": None}
    return {"status": "ok", "since": now.isoformat(), "last_alert": None}


def run():
    old_state = load_state()
    new_state = check_pipeline()

    # Preserve "since" timestamp if status hasn't changed
    if new_state["status"] == old_state["status"]:
        new_state["since"] = old_state["since"]
        new_state["last_alert"] = old_state.get("last_alert")

    print(f"[monitor] status={new_state['status']} rows_check={'ok' if new_state['status'] == 'ok' else 'NO DATA'}")

    if should_alert(old_state, new_state):
        from email_utils import send_email
        subject = build_alert_subject(new_state, old_state)
        body = build_alert_body(new_state, old_state)
        if send_email(subject, body):
            new_state["last_alert"] = datetime.now(timezone.utc).isoformat()

    save_state(STATE_FILE, new_state)


if __name__ == "__main__":
    run()
