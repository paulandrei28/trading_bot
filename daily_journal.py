"""
daily_journal.py
----------------
Append-only daily trading journal for the FVG Live Trader.
Called at the end of every live_trader.py run to record one row per day.

CSV path: logs/daily_journal.csv

Columns:
  date          | YYYY-MM-DD
  symbol        | e.g. QQQ
  mode          | PAPER / LIVE
  status        | TRADE / SKIP / LATE_START / ERROR
  skip_reason   | why no trade was taken (empty if trade placed)
  direction     | LONG / SHORT / -
  entry_time    | HH:MM NY (empty if no trade)
  entry         | float (empty if no trade)
  stop          | float
  target        | float
  qty           | int
  result        | WIN / LOSS / BE / OPEN / -
  result_r      | float R (empty if no trade or still open)
  pnl_usd       | dollar P&L (empty if no trade)
  or_width      | opening range width
  atr_14        | daily ATR(14) value
  or_filter     | PASS / SKIP
  atr_filter    | PASS / SKIP
  started_at    | HH:MM NY when script launched
  note          | free-text extra info
"""

from __future__ import annotations

import csv
import logging
import os
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger("daily_journal")

JOURNAL_PATH = Path("logs/daily_journal.csv")

COLUMNS = [
    "date",
    "symbol",
    "mode",
    "status",
    "skip_reason",
    "direction",
    "entry_time",
    "entry",
    "stop",
    "target",
    "qty",
    "result",
    "result_r",
    "pnl_usd",
    "or_width",
    "atr_14",
    "or_filter",
    "atr_filter",
    "started_at",
    "note",
]


@dataclass
class JournalEntry:
    # --- always filled ---
    date: str = ""  # YYYY-MM-DD
    symbol: str = ""
    mode: str = ""  # PAPER / LIVE
    status: str = ""  # TRADE / SKIP / LATE_START / ERROR
    started_at: str = ""  # HH:MM NY

    # --- filter results ---
    or_filter: str = ""  # PASS / SKIP
    atr_filter: str = ""  # PASS / SKIP
    or_width: str = ""  # float as string
    atr_14: str = ""  # float as string

    # --- skip details ---
    skip_reason: str = ""

    # --- trade details (filled only when status == TRADE) ---
    direction: str = ""
    entry_time: str = ""  # HH:MM NY
    entry: str = ""
    stop: str = ""
    target: str = ""
    qty: str = ""

    # --- outcome (filled after session ends) ---
    result: str = ""  # WIN / LOSS / BE / OPEN / -
    result_r: str = ""
    pnl_usd: str = ""

    # --- misc ---
    note: str = ""


def _ensure_file() -> None:
    """Create the CSV with headers if it does not exist yet."""
    JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not JOURNAL_PATH.exists():
        with JOURNAL_PATH.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=COLUMNS)
            writer.writeheader()
        log.info(f"[JOURNAL] Created new journal at {JOURNAL_PATH}")


def _today_str() -> str:
    return date.today().isoformat()


def _row_exists(date_str: str, symbol: str) -> bool:
    """Return True if a row for this date+symbol already exists."""
    if not JOURNAL_PATH.exists():
        return False
    with JOURNAL_PATH.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("date") == date_str and row.get("symbol") == symbol:
                return True
    return False


def _overwrite_row(entry: JournalEntry) -> None:
    """Replace an existing row (same date+symbol) with updated data."""
    if not JOURNAL_PATH.exists():
        return
    rows = []
    with JOURNAL_PATH.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("date") == entry.date and row.get("symbol") == entry.symbol:
                rows.append({col: getattr(entry, col, "") for col in COLUMNS})
            else:
                rows.append(row)
    with JOURNAL_PATH.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def append_or_update(entry: JournalEntry) -> None:
    """
    Write entry to the journal CSV.
    - If no row exists for today+symbol: append.
    - If a row already exists: overwrite it (e.g. updating result after trade closes).
    """
    _ensure_file()
    if _row_exists(entry.date, entry.symbol):
        _overwrite_row(entry)
        log.info(
            f"[JOURNAL] Updated row for {entry.date} {entry.symbol} -> {entry.status} {entry.result}"
        )
    else:
        with JOURNAL_PATH.open("a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=COLUMNS)
            writer.writerow({col: getattr(entry, col, "") for col in COLUMNS})
        log.info(
            f"[JOURNAL] Appended row for {entry.date} {entry.symbol} -> {entry.status} {entry.result}"
        )


# ---------------------------------------------------------------------------
# Convenience builders -- call these from live_trader.py
# ---------------------------------------------------------------------------


def record_skip(
    symbol: str,
    mode: str,
    reason: str,
    started_at: str,
    or_filter: str = "",
    atr_filter: str = "",
    or_width: float | None = None,
    atr_14: float | None = None,
    note: str = "",
) -> None:
    """Record a day where no trade was taken due to a filter or late-start."""
    entry = JournalEntry(
        date=_today_str(),
        symbol=symbol,
        mode=mode,
        status="SKIP",
        started_at=started_at,
        skip_reason=reason,
        or_filter=or_filter,
        atr_filter=atr_filter,
        or_width=f"{or_width:.4f}" if or_width is not None else "",
        atr_14=f"{atr_14:.4f}" if atr_14 is not None else "",
        result="-",
        direction="-",
        note=note,
    )
    append_or_update(entry)


def record_late_start(
    symbol: str,
    mode: str,
    started_at: str,
    fired_at: str,
    direction: str,
    note: str = "",
) -> None:
    """Record a day skipped because the signal already fired before script start."""
    entry = JournalEntry(
        date=_today_str(),
        symbol=symbol,
        mode=mode,
        status="LATE_START",
        started_at=started_at,
        skip_reason=f"Signal ({direction}) already fired at {fired_at} before script started",
        direction=direction,
        entry_time=fired_at,
        result="-",
        note=note,
    )
    append_or_update(entry)


def record_trade_open(
    symbol: str,
    mode: str,
    started_at: str,
    direction: str,
    entry_time: str,
    entry: float,
    stop: float,
    target: float,
    qty: int,
    or_filter: str = "PASS",
    atr_filter: str = "PASS",
    or_width: float | None = None,
    atr_14: float | None = None,
    note: str = "",
) -> None:
    """Record the moment a trade is placed (result unknown yet)."""
    entry_obj = JournalEntry(
        date=_today_str(),
        symbol=symbol,
        mode=mode,
        status="TRADE",
        started_at=started_at,
        or_filter=or_filter,
        atr_filter=atr_filter,
        or_width=f"{or_width:.4f}" if or_width is not None else "",
        atr_14=f"{atr_14:.4f}" if atr_14 is not None else "",
        direction=direction,
        entry_time=entry_time,
        entry=str(entry),
        stop=str(stop),
        target=str(target),
        qty=str(qty),
        result="OPEN",
        note=note,
    )
    append_or_update(entry_obj)


def record_trade_result(
    symbol: str,
    result: str,  # WIN / LOSS / BE
    result_r: float,
    pnl_usd: float,
    note: str = "",
) -> None:
    """
    Update today's row with the final trade outcome.
    Call this once the trade closes (stop or target hit, or session end).
    """
    if not JOURNAL_PATH.exists():
        log.warning("[JOURNAL] Cannot update result -- journal file not found.")
        return

    date_str = _today_str()
    rows = []
    updated = False

    with JOURNAL_PATH.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("date") == date_str and row.get("symbol") == symbol:
                row["result"] = result
                row["result_r"] = f"{result_r:.2f}"
                row["pnl_usd"] = f"{pnl_usd:.2f}"
                if note:
                    row["note"] = note
                updated = True
            rows.append(row)

    if updated:
        with JOURNAL_PATH.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        log.info(
            f"[JOURNAL] Result recorded for {date_str} {symbol}: "
            f"{result}  {result_r:+.2f}R  ${pnl_usd:+.2f}"
        )
    else:
        log.warning(
            f"[JOURNAL] No open trade row found for {date_str} {symbol} to update."
        )
