"""
live_trader.py
--------------
FVG Live Trader — integrates with IBKR TWS via ibapi + ib_insync.
Supports paper and live trading, late-start detection, OR/ATR filters,
detailed phase logging, and daily journal CSV.

Usage:
    python live_trader.py              # paper mode (default)
    python live_trader.py --live       # live mode (port 7496)
    python live_trader.py --symbol SPY --risk 200
"""

from __future__ import annotations

import sys
import io
import argparse
import logging
import math
import os
import time
from datetime import datetime, date, timedelta
from typing import Optional
import pandas as pd
import pytz

# Force UTF-8 on Windows console so arrow/dash characters don't crash
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ── project imports ────────────────────────────────────────────────────────────
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(__file__), "tws"))

from tws.ib_history import IBKRHistoryClient, IBHistoryConfig
from tws.ib_execution import IBExecution
from strategy.fvg_strategy import FVGConfig, generate_trades
from daily_journal import (
    record_skip,
    record_late_start,
    record_trade_open,
    record_trade_result,
)

NY = pytz.timezone("America/New_York")

# ══════════════════════════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════════════════════════


class LiveConfig:
    SYMBOL = "QQQ"
    PAPER = True  # overridden by --live flag
    RISK_USD = 100.0  # dollars risked per trade
    HOST = "127.0.0.1"
    PORT_PAPER = 7497
    PORT_LIVE = 7496
    CLIENT_ID = 1  # for execution (order placement)
    CLIENT_ID_HIST = 11  # for history (warm-up + intraday fetches)

    SESSION_START = "09:30"  # NY market open
    OPENING_END = "09:35"  # end of opening range window
    TRADE_START = "10:00"  # earliest entry time
    TRADE_END = "15:00"    # latest NEW entry — scan loop exits here
    HARD_CLOSE = "16:00"   # monitoring hard stop — cancel remaining legs here
    RR = 3.0  # reward:risk ratio
    WARMUP_DAYS = 30  # days of history for OR/ATR filters

    SCAN_INTERVAL = 15  # seconds between bar fetches when live
    OR_Q_LO = 0.20  # Q2 skip band — lower percentile
    OR_Q_HI = 0.40  # Q2 skip band — upper percentile
    ATR_Q_THRESH = 0.50  # minimum ATR percentile to trade

    @property
    def port(self) -> int:
        return self.PORT_PAPER if self.PAPER else self.PORT_LIVE

    @property
    def mode_str(self) -> str:
        return "PAPER" if self.PAPER else "LIVE"


# ══════════════════════════════════════════════════════════════════════════════
# Logging setup
# ══════════════════════════════════════════════════════════════════════════════


def setup_logging(symbol: str, paper: bool) -> logging.Logger:
    os.makedirs("logs", exist_ok=True)
    today = date.today().isoformat()
    mode = "paper" if paper else "live"
    log_path = f"logs/live_{symbol}_{today}_{mode}.log"

    fmt = "%(asctime)s  %(levelname)-8s %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(fmt, datefmt))
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(fmt, datefmt))
    console_handler.setLevel(logging.INFO)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(file_handler)
    root.addHandler(console_handler)

    # Silence noisy IB library loggers
    logging.getLogger("ib_insync").setLevel(logging.WARNING)
    logging.getLogger("ibapi").setLevel(logging.WARNING)

    log = logging.getLogger("live_trader")
    log.info(f"[LOGGING] Log file: {log_path}")
    return log


# ══════════════════════════════════════════════════════════════════════════════
# Time helpers
# ══════════════════════════════════════════════════════════════════════════════


def now_ny() -> datetime:
    return datetime.now(tz=NY)


def to_time(hhmm: str):
    return datetime.strptime(hhmm, "%H:%M").time()


def ny_hhmm(dt: datetime) -> str:
    return dt.astimezone(NY).strftime("%H:%M")


def seconds_until(target_hhmm: str) -> float:
    t = to_time(target_hhmm)
    n = now_ny()
    target_dt = n.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
    if target_dt <= n:
        return 0.0
    return (target_dt - n).total_seconds()


# ══════════════════════════════════════════════════════════════════════════════
# Bar fetching
# ══════════════════════════════════════════════════════════════════════════════


def fetch_bars(
    symbol: str,
    start: datetime,
    end: datetime,
    cfg: LiveConfig,
    client_id_offset: int = 0,
) -> pd.DataFrame:
    hist_cfg = IBHistoryConfig(
        host=cfg.HOST,
        port=cfg.port,
        client_id=cfg.CLIENT_ID_HIST + client_id_offset,
    )
    client = IBKRHistoryClient(hist_cfg)
    try:
        bars = client.fetch_1m_bars(symbol, start, end)
    finally:
        client.disconnect()
    return bars


# ══════════════════════════════════════════════════════════════════════════════
# Daily OR / ATR filter computation
# ══════════════════════════════════════════════════════════════════════════════


def compute_daily_or_atr(bars: pd.DataFrame, cfg: LiveConfig) -> pd.DataFrame:
    """
    From warmup bars compute per-day opening_range and ATR(14).
    Returns a DataFrame indexed by date with columns: or_width, atr_14.
    """
    bars = bars.copy()
    bars["ts_ny"] = pd.to_datetime(bars["timestamp"], utc=True).dt.tz_convert(NY)
    bars["date"] = bars["ts_ny"].dt.date

    session_start = to_time(cfg.SESSION_START)
    opening_end = to_time(cfg.OPENING_END)

    rows = []
    for day, grp in bars.groupby("date"):
        grp = grp.sort_values("ts_ny")
        opening = grp[
            (grp["ts_ny"].dt.time >= session_start)
            & (grp["ts_ny"].dt.time < opening_end)
        ]
        if len(opening) < 2:
            continue
        or_width = float(opening["high"].max() - opening["low"].min())

        # True Range for ATR(14)
        grp = grp.reset_index(drop=True)
        prev_close = grp["close"].shift(1)
        tr = pd.concat(
            [
                grp["high"] - grp["low"],
                (grp["high"] - prev_close).abs(),
                (grp["low"] - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr_14 = (
            float(tr.rolling(14).mean().dropna().iloc[-1])
            if len(tr) >= 14
            else float(tr.mean())
        )

        rows.append({"date": day, "or_width": or_width, "atr_14": atr_14})

    return pd.DataFrame(rows).set_index("date")


def get_today_or_atr(
    daily: pd.DataFrame,
    today: date,
    symbol: str,
    cfg: LiveConfig,
    log: logging.Logger,
) -> tuple[Optional[float], Optional[float]]:
    """Fetch today's bars (09:30-09:35) to compute today's OR width and ATR."""
    today_start = NY.localize(datetime.combine(today, to_time(cfg.SESSION_START)))
    today_end = now_ny()

    log.debug("[FILTER] Fetching today's bars for OR/ATR computation...")
    today_bars = fetch_bars(symbol, today_start, today_end, cfg, client_id_offset=1)

    if today_bars.empty:
        log.warning("[FILTER] No bars for today — cannot compute OR/ATR.")
        return None, None

    today_bars["ts_ny"] = pd.to_datetime(
        today_bars["timestamp"], utc=True
    ).dt.tz_convert(NY)

    session_start = to_time(cfg.SESSION_START)
    opening_end = to_time(cfg.OPENING_END)
    opening = today_bars[
        (today_bars["ts_ny"].dt.time >= session_start)
        & (today_bars["ts_ny"].dt.time < opening_end)
    ]
    if len(opening) < 2:
        log.warning("[FILTER] Not enough opening range bars yet.")
        return None, None

    or_width = float(opening["high"].max() - opening["low"].min())

    prev_close = today_bars["close"].shift(1)
    tr = pd.concat(
        [
            today_bars["high"] - today_bars["low"],
            (today_bars["high"] - prev_close).abs(),
            (today_bars["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr_today = (
        float(tr.rolling(14).mean().dropna().iloc[-1]) if len(tr) >= 14 else None
    )

    return or_width, atr_today


# ══════════════════════════════════════════════════════════════════════════════
# Signal detection (wraps generate_trades for live use)
# ══════════════════════════════════════════════════════════════════════════════


def detect_signal(
    bars: pd.DataFrame,
    cfg: LiveConfig,
    or_high: float,
    or_low: float,
    cutoff_time: Optional[datetime],
    log: logging.Logger,
) -> Optional[dict]:
    """
    Run generate_trades on today's bars and return the first signal found,
    or None if no setup has printed yet.
    """
    fvg_cfg = FVGConfig(
        tick_size=0.01,
        rr=cfg.RR,
        session_start=cfg.SESSION_START,
        opening_end=cfg.OPENING_END,
        trade_start=cfg.TRADE_START,
        trade_end=cfg.TRADE_END,
        cutoff_time=cfg.TRADE_END,
        one_trade_per_day=True,
        retest_mode="close",
        use_profit_lock=True,
        lock1_trigger_r=1.5,
        lock1_stop_r=0.5,
        lock2_trigger_r=2.5,
        lock2_stop_r=1.0,
    )

    trades = generate_trades(bars, fvg_cfg)
    if not trades:
        return None
    return trades[0]


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="FVG Live Trader")
    p.add_argument(
        "--live", action="store_true", help="Use live port 7496 instead of paper 7497"
    )
    p.add_argument("--symbol", default="QQQ", help="Ticker symbol (default: QQQ)")
    p.add_argument(
        "--risk", type=float, default=100.0, help="Dollar risk per trade (default: 100)"
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    config = LiveConfig()
    config.PAPER = not args.live
    config.SYMBOL = args.symbol.upper()
    config.RISK_USD = args.risk

    log = setup_logging(config.SYMBOL, config.PAPER)

    started_at_dt = now_ny()
    started_at_str = ny_hhmm(started_at_dt)
    today = started_at_dt.date()

    # ── Banner ─────────────────────────────────────────────────────────────────
    log.info("=" * 65)
    log.info(f"  FVG Live Trader  --  {config.mode_str} MODE")
    log.info(f"  Symbol    : {config.SYMBOL}")
    log.info(
        f"  Port      : {config.port}  (paper={config.PORT_PAPER} / live={config.PORT_LIVE})"
    )
    log.info(f"  Risk/trade: ${config.RISK_USD}")
    log.info(f"  Session   : {config.SESSION_START} - {config.TRADE_END} NY  (entries)")
    log.info(f"  Monitoring: until {config.HARD_CLOSE} NY  (open trade hard close)")
    log.info(f"  Trade win : {config.TRADE_START} - {config.TRADE_END} NY")
    log.info(f"  Script started at: {started_at_str} NY")
    log.info("=" * 65)

    session_start_t = to_time(config.SESSION_START)
    opening_end_t = to_time(config.OPENING_END)
    trade_start_t = to_time(config.TRADE_START)
    trade_end_t = to_time(config.TRADE_END)
    hard_close_t = to_time(config.HARD_CLOSE)

    # ── Weekend guard ──────────────────────────────────────────────────────────
    if started_at_dt.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        day_name = started_at_dt.strftime("%A")
        log.info(f"[WEEKEND] Today is {day_name}. US equity markets are closed. Exiting.")
        _session_summary(log, config, trade_placed=False)
        return

    # ── PRE-MARKET wait ────────────────────────────────────────────────────────
    while now_ny().time() < session_start_t:
        secs = seconds_until(config.SESSION_START)
        log.info(
            f"[PRE-MARKET] Waiting for market open ({config.SESSION_START} NY). "
            f"{int(secs // 60)}m {int(secs % 60)}s remaining..."
        )
        time.sleep(min(30, secs))

    log.info(f"[MARKET OPEN] Market is open. Starting warm-up.")

    # ── WARM-UP: fetch history for filters ────────────────────────────────────
    log.info(
        f"[WARM-UP] Fetching {config.WARMUP_DAYS} days of 1m bars for OR/ATR filters..."
    )

    warmup_end = NY.localize(datetime.combine(today, to_time(config.SESSION_START)))
    warmup_start = warmup_end - timedelta(days=config.WARMUP_DAYS)

    warmup_df = fetch_bars(config.SYMBOL, warmup_start, warmup_end, config)
    log.info(
        f"[WARM-UP] {len(warmup_df)} bars loaded "
        f"({warmup_start.date()} to {warmup_end.date()})"
    )

    # ── FILTER: compute historical OR/ATR distribution ────────────────────────
    log.info("[FILTER] Computing daily OR and ATR filters from warm-up data...")
    daily_stats = compute_daily_or_atr(warmup_df, config)

    or_q_lo = daily_stats["or_width"].quantile(config.OR_Q_LO)
    or_q_hi = daily_stats["or_width"].quantile(config.OR_Q_HI)
    atr_min = daily_stats["atr_14"].quantile(config.ATR_Q_THRESH)

    or_skipped = (
        (daily_stats["or_width"] > or_q_lo) & (daily_stats["or_width"] <= or_q_hi)
    ).sum()
    atr_skipped = (daily_stats["atr_14"] < atr_min).sum()
    total_days = len(daily_stats)

    log.info(f"[FILTER] OR skip band      : ({or_q_lo:.3f}, {or_q_hi:.3f}]")
    log.info(f"[FILTER] ATR min threshold : {atr_min:.3f}")
    log.info(f"[FILTER] OR  would skip    : {or_skipped}/{total_days} historical days")
    log.info(f"[FILTER] ATR would skip    : {atr_skipped}/{total_days} historical days")

    # ── OPENING RANGE wait ────────────────────────────────────────────────────
    while now_ny().time() < opening_end_t:
        secs = seconds_until(config.OPENING_END)
        log.info(
            f"[OPENING RANGE] Waiting for opening range to complete ({config.OPENING_END} NY). "
            f"{int(secs // 60)}m {int(secs % 60)}s remaining..."
        )
        time.sleep(min(15, max(1, secs)))

    log.info("[OPENING RANGE] Window closed. Fetching today's OR and ATR...")

    # ── Today's OR + ATR ──────────────────────────────────────────────────────
    or_width_today, atr_today = get_today_or_atr(
        daily_stats, today, config.SYMBOL, config, log
    )

    if or_width_today is not None:
        log.info(
            f"[OPENING RANGE] OR width today: {or_width_today:.4f}  "
            f"ATR today: {atr_today if atr_today else 'n/a (using warmup)'}"
        )
    else:
        log.warning(
            "[OPENING RANGE] Could not compute OR width — proceeding without OR filter."
        )

    # ── Apply filters ─────────────────────────────────────────────────────────
    or_filter_result = "UNKNOWN"
    atr_filter_result = "UNKNOWN"

    # OR filter
    if or_width_today is not None:
        if or_q_lo < or_width_today <= or_q_hi:
            or_filter_result = "SKIP"
            log.info(
                f"[FILTER] TODAY SKIPPED - Opening range width {or_width_today:.4f} "
                f"is in the Q2 indecision band ({or_q_lo:.3f}, {or_q_hi:.3f}]. "
                f"No edge expected. Exiting cleanly."
            )
            record_skip(
                symbol=config.SYMBOL,
                mode=config.mode_str,
                reason=f"OR width {or_width_today:.4f} in Q2 indecision band ({or_q_lo:.3f}, {or_q_hi:.3f}]",
                started_at=started_at_str,
                or_filter="SKIP",
                atr_filter="UNKNOWN",
                or_width=or_width_today,
                atr_14=atr_today,
            )
            _session_summary(log, config, trade_placed=False)
            return
        else:
            or_filter_result = "PASS"
            log.info(
                f"[FILTER] OR filter PASS - width {or_width_today:.4f} is outside indecision band."
            )
    else:
        or_filter_result = "PASS"
        log.warning("[FILTER] OR filter SKIPPED (no data) - treating as PASS.")

    # ATR filter (use today's ATR if available, else last warmup day)
    effective_atr = atr_today
    if effective_atr is None and len(daily_stats) > 0:
        effective_atr = float(daily_stats["atr_14"].iloc[-1])
        log.info(f"[FILTER] Using last warmup ATR: {effective_atr:.4f}")

    if effective_atr is not None:
        if effective_atr < atr_min:
            atr_filter_result = "SKIP"
            log.info(
                f"[FILTER] TODAY SKIPPED - ATR {effective_atr:.4f} is below "
                f"the {int(config.ATR_Q_THRESH*100)}th percentile threshold {atr_min:.4f}. "
                f"Low volatility day — no edge expected. Exiting cleanly."
            )
            record_skip(
                symbol=config.SYMBOL,
                mode=config.mode_str,
                reason=f"ATR {effective_atr:.4f} below threshold {atr_min:.4f}",
                started_at=started_at_str,
                or_filter=or_filter_result,
                atr_filter="SKIP",
                or_width=or_width_today,
                atr_14=effective_atr,
            )
            _session_summary(log, config, trade_placed=False)
            return
        else:
            atr_filter_result = "PASS"
            log.info(
                f"[FILTER] ATR filter PASS - {effective_atr:.4f} >= threshold {atr_min:.4f}."
            )
    else:
        atr_filter_result = "PASS"
        log.warning("[FILTER] ATR filter SKIPPED (no data) - treating as PASS.")

    log.info(f"[FILTER] Both filters passed. Today is a valid trading day.")

    # ── LATE START check ───────────────────────────────────────────────────────
    is_late = started_at_dt.time() > opening_end_t
    if is_late:
        log.info(
            f"[LATE START] Script started at {started_at_str} -- after opening range window "
            f"({config.OPENING_END}). Checking if today's setup already fired in missed bars..."
        )

    # ── Connect execution client ───────────────────────────────────────────────
    log.info(f"[CONNECTION] Connecting to IBKR at {config.HOST}:{config.port}...")
    exec_client = IBExecution()
    exec_client.connect_and_run(config.HOST, config.port, config.CLIENT_ID)
    log.info(f"[CONNECTION] Connected to TWS successfully.")

    # ── Main scan loop ─────────────────────────────────────────────────────────
    log.info(f"[LOOP] Entering intraday scanning loop.")
    log.info(
        f"[LOOP] New entries allowed from {config.TRADE_START} to {config.TRADE_END} NY."
    )
    log.info(
        f"[LOOP] Open trades monitored until hard close at {config.HARD_CLOSE} NY."
    )

    trade_placed = False
    trade_result = None
    loop_count = 0
    or_high = None
    or_low = None

    try:
        while True:
            now = now_ny()
            now_t = now.time()

            # 15:00 — no new entries allowed; scan loop exits here
            if now_t >= trade_end_t:
                log.info(
                    f"[SESSION END] {config.TRADE_END} NY reached. "
                    f"No new entries. Scan loop ending."
                )
                break

            # TRADE WINDOW wait
            if now_t < trade_start_t:
                secs = seconds_until(config.TRADE_START)
                log.info(
                    f"[TRADE WINDOW] Waiting for trade window ({config.TRADE_START} NY). "
                    f"{int(secs // 60)}m {int(secs % 60)}s remaining..."
                )
                time.sleep(min(30, max(1, secs)))
                continue

            loop_count += 1
            log.info(f"[FETCH] Fetching today's 1m bars (loop #{loop_count})...")

            today_start = NY.localize(
                datetime.combine(today, to_time(config.SESSION_START))
            )
            today_bars = fetch_bars(
                config.SYMBOL,
                today_start,
                now_ny(),
                config,
                client_id_offset=loop_count,
            )

            if today_bars.empty:
                log.warning("[FETCH] No bars returned. Will retry.")
                time.sleep(config.SCAN_INTERVAL)
                continue

            today_bars["ts_ny"] = pd.to_datetime(
                today_bars["timestamp"], utc=True
            ).dt.tz_convert(NY)
            latest_bar_time = ny_hhmm(today_bars["ts_ny"].iloc[-1])
            log.info(
                f"[FETCH] {len(today_bars)} bars loaded. Latest bar: {latest_bar_time} NY"
            )

            # Compute OR high/low from today's bars
            opening_bars = today_bars[
                (today_bars["ts_ny"].dt.time >= to_time(config.SESSION_START))
                & (today_bars["ts_ny"].dt.time < to_time(config.OPENING_END))
            ]
            if not opening_bars.empty and or_high is None:
                or_high = float(opening_bars["high"].max())
                or_low = float(opening_bars["low"].min())
                log.info(
                    f"[OPENING RANGE] Computed -- high={or_high:.2f}  "
                    f"low={or_low:.2f}  width={or_high - or_low:.3f}"
                )

            log.info(
                f"[SCAN] Scanning {len(today_bars)} bars for FVG + retest + engulfing setup..."
            )
            signal = detect_signal(today_bars, config, or_high, or_low, None, log)

            if signal is None:
                log.info("[SCAN] No setup found yet. Will scan again.")
                time.sleep(config.SCAN_INTERVAL)
                continue

            signal_time = pd.Timestamp(signal["entry_time"]).astimezone(NY)
            signal_hhmm = signal_time.strftime("%H:%M")
            log.info(
                f"[SIGNAL] Setup detected -- {signal['direction']} at {signal_hhmm} NY  "
                f"entry={signal['entry']}  stop={signal['stop']}  target={signal['target']}"
            )

            # Late start: signal fired before we launched
            if is_late and signal_time <= started_at_dt:
                log.info(
                    f"[LATE START] NOTE: Skipping late-start: {signal['direction']} signal "
                    f"at {signal_hhmm} already fired before script started."
                )
                log.info(
                    "[LATE START] Setup found but already fired before script started. "
                    "One trade per day rule -- no more entries today. Exiting."
                )
                record_late_start(
                    symbol=config.SYMBOL,
                    mode=config.mode_str,
                    started_at=started_at_str,
                    fired_at=signal_hhmm,
                    direction=signal["direction"],
                )
                break

            # Calculate quantity from risk
            risk_per_share = abs(signal["entry"] - signal["stop"])
            if risk_per_share <= 0:
                log.warning("[SIGNAL] Risk per share is 0 -- skipping signal.")
                time.sleep(config.SCAN_INTERVAL)
                continue

            qty = max(1, math.floor(config.RISK_USD / risk_per_share))
            log.info(
                f"[SIGNAL] Risk: ${risk_per_share:.2f}/share  "
                f"Qty: {qty}  Max loss: ${qty * risk_per_share:.2f}  "
                f"Target gain: ${qty * risk_per_share * config.RR:.2f}"
            )

            # Place bracket order
            log.info(f"[ORDER] Submitting bracket order to TWS...")
            entry_id, stop_id, target_id = exec_client.place_bracket(
                symbol=config.SYMBOL,
                direction=signal["direction"],
                entry=signal["entry"],
                stop=signal["stop"],
                target=signal["target"],
                quantity=qty,
            )
            log.info(
                f"[ORDER] Bracket placed -- entry_id={entry_id}  "
                f"stop_id={stop_id}  target_id={target_id}"
            )

            trade_placed = True

            # Record trade open in journal
            record_trade_open(
                symbol=config.SYMBOL,
                mode=config.mode_str,
                started_at=started_at_str,
                direction=signal["direction"],
                entry_time=signal_hhmm,
                entry=signal["entry"],
                stop=signal["stop"],
                target=signal["target"],
                qty=qty,
                or_filter=or_filter_result,
                atr_filter=atr_filter_result,
                or_width=or_width_today,
                atr_14=effective_atr,
            )

            # ── Monitoring loop ────────────────────────────────────────────────
            # 15:00 only blocks NEW entries (scan loop above).
            # This loop keeps watching an already-live trade until:
            #   - target fills  -> WIN
            #   - stop fills    -> LOSS
            #   - 16:00 reached -> cancel remaining legs, mark OPEN
            log.info(
                f"[MONITORING] Trade is live. Monitoring until fill or "
                f"hard close at {config.HARD_CLOSE} NY..."
            )
            past_cutoff_logged = False

            while True:
                now = now_ny()
                now_t = now.time()

                # Inform once when we cross 15:00 — but keep watching
                if now_t >= trade_end_t and not past_cutoff_logged:
                    log.info(
                        f"[MONITORING] Past {config.TRADE_END} NY -- trade entered before "
                        f"cutoff, continuing to monitor until fill or hard close "
                        f"at {config.HARD_CLOSE} NY."
                    )
                    past_cutoff_logged = True

                # Hard close at 16:00 — cancel remaining legs and exit
                if now_t >= hard_close_t:
                    log.info(
                        f"[MONITORING] Hard close at {config.HARD_CLOSE} NY reached. "
                        f"Cancelling remaining open legs..."
                    )
                    try:
                        exec_client.cancel_order(stop_id)
                        log.info(f"[MONITORING] Cancelled stop order {stop_id}.")
                    except Exception as e:
                        log.warning(f"[MONITORING] Could not cancel stop {stop_id}: {e}")
                    try:
                        exec_client.cancel_order(target_id)
                        log.info(f"[MONITORING] Cancelled target order {target_id}.")
                    except Exception as e:
                        log.warning(f"[MONITORING] Could not cancel target {target_id}: {e}")

                    record_trade_result(
                        symbol=config.SYMBOL,
                        result="OPEN",
                        result_r=0.0,
                        pnl_usd=0.0,
                        note=f"Hard close at {config.HARD_CLOSE} -- trade still open at market close",
                    )
                    log.info(
                        "[MONITORING] Trade marked OPEN "
                        "(not resolved before market close)."
                    )
                    trade_result = "OPEN"
                    break

                # Check order fills via status callbacks
                stop_status = exec_client._orders.get(stop_id, "")
                target_status = exec_client._orders.get(target_id, "")

                log.debug(
                    f"[MONITORING] stop={stop_status or 'pending'}  "
                    f"target={target_status or 'pending'}"
                )

                if target_status == "Filled":
                    result_r = config.RR
                    pnl_usd = qty * risk_per_share * config.RR
                    log.info(
                        f"[RESULT] TARGET HIT -- +{result_r:.2f}R  +${pnl_usd:.2f}"
                    )
                    record_trade_result(config.SYMBOL, "WIN", result_r, pnl_usd)
                    trade_result = "WIN"
                    break

                if stop_status == "Filled":
                    result_r = -1.0
                    pnl_usd = -qty * risk_per_share
                    log.info(
                        f"[RESULT] STOP HIT -- {result_r:.2f}R  ${pnl_usd:.2f}"
                    )
                    record_trade_result(config.SYMBOL, "LOSS", result_r, pnl_usd)
                    trade_result = "LOSS"
                    break

                time.sleep(config.SCAN_INTERVAL)

            break  # one trade per day

    except KeyboardInterrupt:
        log.info("[INTERRUPTED] KeyboardInterrupt received. Shutting down.")

    finally:
        try:
            exec_client.disconnect_safe()
        except Exception:
            pass

    _session_summary(log, config, trade_placed=trade_placed, trade_result=trade_result)


def _session_summary(
    log: logging.Logger,
    config: LiveConfig,
    trade_placed: bool,
    trade_result: Optional[str] = None,
) -> None:
    log.info("-" * 65)
    log.info(f"[SESSION SUMMARY]  Symbol      : {config.SYMBOL}")
    log.info(f"[SESSION SUMMARY]  Mode        : {config.mode_str}")
    log.info(f"[SESSION SUMMARY]  Trade placed: {'YES' if trade_placed else 'NO'}")
    if trade_result:
        log.info(f"[SESSION SUMMARY]  Result      : {trade_result}")
    log.info(f"[SESSION SUMMARY]  Journal     : logs/daily_journal.csv")
    log.info(f"[SESSION SUMMARY]  Log dir     : logs/")
    log.info("-" * 65)


# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()
