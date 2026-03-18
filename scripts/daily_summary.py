"""
Daily collection summary. Run at 8:00 AM UTC via cron.
Queries yesterday's data and sends a summary email.
"""
import sys
import os
import sqlite3

sys.path.insert(0, os.path.dirname(__file__))

from datetime import datetime, timezone, timedelta
from config import DB_PATH
from email_utils import send_email


# WMATA returns raw line codes (e.g. "RD; BL;") — map to readable names
LINE_NAMES = {
    "RD": "Red Line", "BL": "Blue Line", "OR": "Orange Line",
    "SV": "Silver Line", "GR": "Green Line", "YL": "Yellow Line",
    "--": "All Lines",
}

def _readable_lines(raw: str) -> str:
    """Convert 'RD; BL;' → 'Red Line, Blue Line'."""
    if not raw:
        return raw
    codes = [c.strip().rstrip(";") for c in raw.split(";") if c.strip().rstrip(";")]
    return ", ".join(LINE_NAMES.get(c, c) for c in codes)


def _fmt_time(ts: str) -> str | None:
    """Format ISO timestamp to 12-hour time string, or None if ts is falsy."""
    if not ts:
        return None
    return datetime.fromisoformat(ts).strftime("%I:%M %p")


def get_yesterday() -> str:
    """Return yesterday's date string in UTC (YYYY-MM-DD)."""
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


def query_stats(date: str) -> dict:
    """Query the DB for collection stats for the given date."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT collected_at) as cycles,
                MIN(collected_at) as first_cycle,
                MAX(collected_at) as last_cycle
            FROM predictions
            WHERE DATE(collected_at) = ?
        """, (date,)).fetchone()
        total_rows, cycles, first_cycle, last_cycle = row

        incident_rows = conn.execute("""
            SELECT lines_affected, COUNT(*) as cnt
            FROM incidents
            WHERE DATE(collected_at) = ?
            GROUP BY lines_affected
            ORDER BY cnt DESC
        """, (date,)).fetchall()
        incidents = [(_readable_lines(r[0]), r[1]) for r in incident_rows]

        prev_date = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_rows = conn.execute(
            "SELECT COUNT(*) FROM predictions WHERE DATE(collected_at) = ?", (prev_date,)
        ).fetchone()[0]

    return {
        "date": date,
        "total_rows": total_rows,
        "cycles": cycles,
        "first_cycle": _fmt_time(first_cycle),
        "last_cycle": _fmt_time(last_cycle),
        "incidents": incidents,
        "prev_rows": prev_rows,
    }


def compute_delta(today: int, yesterday: int) -> tuple:
    """Return (absolute_delta, percent_delta). percent is None if yesterday was 0."""
    delta = today - yesterday
    if yesterday == 0:
        return delta, None
    pct = round((delta / yesterday) * 100, 2)
    return delta, pct


def format_summary(stats: dict) -> str:
    date = stats["date"]
    total = stats["total_rows"]
    cycles = stats["cycles"]
    first = stats["first_cycle"] or "—"
    last = stats["last_cycle"] or "—"
    incidents = stats["incidents"]
    delta, pct = compute_delta(total, stats["prev_rows"])

    if total == 0:
        return f"Resumen {date}\n\nSin datos recopilados este día."

    lines = [
        f"Resumen {date}",
        "",
        "Recolección:",
        f"  • Total filas: {total:,}",
        f"  • Ciclos completados: {cycles}",
        f"  • Primer ciclo: {first} UTC | Último: {last} UTC",
        "",
    ]

    if pct is not None:
        sign = "+" if delta >= 0 else ""
        lines += [f"Comparación vs ayer: {sign}{delta:,} ({sign}{pct}%)", ""]
    else:
        lines += ["Comparación vs ayer: sin datos del día anterior", ""]

    if incidents:
        lines.append(f"Incidentes detectados: {sum(c for _, c in incidents)}")
        for line, count in incidents:
            lines.append(f"  • {line}: {count}")
    else:
        lines.append("Incidentes detectados: 0")

    return "\n".join(lines)


def run():
    date = get_yesterday()
    stats = query_stats(date)
    subject = f"[WMATA] Resumen {date}"
    body = format_summary(stats)
    print(f"[daily_summary] Sending summary for {date} ({stats['total_rows']:,} rows)")
    send_email(subject, body)


if __name__ == "__main__":
    run()
