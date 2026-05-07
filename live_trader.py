"""
live_trader.py
--------------
FVG Live Trader -- integrates with IBKR TWS via ibapi + ib_insync.
Supports paper and live trading, late-start detection, OR/ATR filters,
detailed phase logging, and daily journal CSV.

Usage:
    python live_trader.py              # paper mode (default)
    python live_trader.py --live       # live mode (port 7496)
    python live_trader.py --symbol SPY --risk 200
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd
import pytz

from strategy.fvg_strategy import FVGConfig, generate_trades
from tws.ib_history import IBKRHistoryClient, IBHistoryConfig
from daily_journal import record_skip, record_trade_open, record_trade_result

NY = pytz.timezone("America/New_York")


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------


def setup_logging(symbol: str, paper: bool) -> logging.Logger:
    mode = "paper" if paper else "live"
    today = datetime.now(NY).strftime("%Y-%m-%d")
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/live_{symbol}_{today}_{mode}.log"
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("live_trader")
    log.info(f"[LOGGING] Log file: {log_file}")
    return log


# -----------------------------------------------------------------------------
# Bar fetching  (uses 30-second timeout to prevent hangs)
# -----------------------------------------------------------------------------


def fetch_bars(
    symbol: str,
    start: datetime,
    end: datetime,
    port: int,
    client_id: int = 21,
) -> pd.DataFrame:
    """Fetch 1-min bars from IBKR with a 30-second per-request timeout."""
    cfg = IBHistoryConfig(port=port, client_id=client_id, request_timeout=30)
    client = IBKRHistoryClient(cfg)
    try:
        df = client.fetch_1m_bars(symbol, start, end)
        return df if df is not None else pd.DataFrame()
    except Exception as exc:
        logging.getLogger("live_trader").warning(f"[DATA] fetch_bars error: {exc}")
        return pd.DataFrame()
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


# -----------------------------------------------------------------------------
# Daily OR / ATR helpers  (called once at startup, not in the scan loop)
# -----------------------------------------------------------------------------


def _daily_table(bars: pd.DataFrame) -> pd.DataFrame:
    """Return per-day DataFrame with or_width and atr14 columns."""
    import datetime as _dt

    df = bars.copy()
    df["ts_ny"] = pd.to_datetime(df["timestamp"]).dt.tz_convert(NY)
    df["date"] = df["ts_ny"].dt.date
    df["t"] = df["ts_ny"].dt.time

    or_mask = (df["t"] >= _dt.time(9, 30)) & (df["t"] < _dt.time(9, 35))
    daily_or = (
        df[or_mask]
        .groupby("date")
        .agg(or_high=("high", "max"), or_low=("low", "min"))
        .assign(or_width=lambda x: x["or_high"] - x["or_low"])
        .reset_index()
    )

    daily_p = (
        df.groupby("date")
        .agg(high=("high", "max"), low=("low", "min"), close=("close", "last"))
        .reset_index()
    )
    daily_p["prev_close"] = daily_p["close"].shift(1)
    daily_p["tr"] = daily_p.apply(
        lambda r: max(
            r["high"] - r["low"],
            abs(r["high"] - r["prev_close"]) if pd.notna(r["prev_close"]) else 0.0,
            abs(r["low"] - r["prev_close"]) if pd.notna(r["prev_close"]) else 0.0,
        ),
        axis=1,
    )
    daily_p["atr14"] = daily_p["tr"].rolling(14, min_periods=1).mean()
    return daily_or.merge(daily_p[["date", "atr14"]], on="date", how="left")


def compute_filters(
    warmup_df: pd.DataFrame,
    log: logging.Logger,
) -> Tuple[Optional[float], Optional[float], Optional[float], int]:
    """Compute OR skip band (Q2) and ATR threshold (median) once at startup."""
    if warmup_df is None or warmup_df.empty:
        log.warning("[FILTER] Empty warm-up -- filters disabled.")
        return None, None, None, 0

    daily = _daily_table(warmup_df)
    n_days = len(daily)

    if n_days < 5:
        log.warning(f"[FILTER] Only {n_days} days in warm-up -- filters disabled.")
        return None, None, None, n_days

    q20 = float(daily["or_width"].quantile(0.20))
    q40 = float(daily["or_width"].quantile(0.40))
    atr_threshold = float(daily["atr14"].median())

    or_skip = int(((daily["or_width"] > q20) & (daily["or_width"] <= q40)).sum())
    atr_skip = int((daily["atr14"] < atr_threshold).sum())

    log.info(f"[FILTER] OR skip band       : ({q20:.3f}, {q40:.3f}]")
    log.info(f"[FILTER] ATR min threshold  : {atr_threshold:.3f}")
    log.info(f"[FILTER] OR would skip      : {or_skip}/{n_days} historical days")
    log.info(f"[FILTER] ATR would skip     : {atr_skip}/{n_days} historical days")
    return q20, q40, atr_threshold, n_days


def get_today_atr(warmup_df: pd.DataFrame, today) -> Optional[float]:
    if warmup_df is None or warmup_df.empty:
        return None
    daily = _daily_table(warmup_df)
    row = daily[daily["date"] == today]
    return float(row.iloc[-1]["atr14"]) if not row.empty else None


def should_skip_day(
    or_width: float,
    atr_today: Optional[float],
    q20: Optional[float],
    q40: Optional[float],
    atr_threshold: Optional[float],
    log: logging.Logger,
) -> bool:
    skip_or = (q20 is not None) and (q20 < or_width <= q40)
    skip_atr = (atr_threshold is not None and atr_today is not None) and (
        atr_today < atr_threshold
    )
    atr_str = f"{atr_today:.3f}" if atr_today is not None else "N/A"

    if skip_or:
        log.info(f"[FILTER] SKIP -- OR {or_width:.3f} in Q2 band ({q20:.3f}, {q40:.3f}]")
        return True
    if skip_atr:
        log.info(f"[FILTER] SKIP -- ATR {atr_str} < threshold {atr_threshold:.3f}")
        return True

    log.info(f"[FILTER] PASS -- OR={or_width:.3f}  ATR={atr_str}")
    return False


# -----------------------------------------------------------------------------
# Order placement + trade monitoring
# -----------------------------------------------------------------------------


def place_bracket_order(
    symbol: str,
    direction: str,
    entry: float,
    stop_px: float,
    target: float,
    qty: int,
    port: int,
    log: logging.Logger,
) -> Optional[int]:
    try:
        from tws.ib_execution import IBKRExecutionClient

        client = IBKRExecutionClient(port=port, client_id=20)
        client.connect()
        action = "BUY" if direction == "LONG" else "SELL"
        log.info(
            f"[ORDER] Placing bracket: {action} {qty}x{symbol} | "
            f"entry={entry:.2f}  stop={stop_px:.2f}  target={target:.2f}"
        )
        parent_id = client.place_bracket_order(
            symbol=symbol,
            action=action,
            quantity=qty,
            entry_price=entry,
            stop_price=stop_px,
            target_price=target,
        )
        log.info(f"[ORDER] Bracket submitted -- parentId={parent_id}")
        client.disconnect()
        return parent_id
    except ImportError:
        log.error("[ORDER] tws/ib_execution.py not found -- cannot place orders.")
        return None
    except Exception as exc:
        log.error(f"[ORDER] Placement failed: {exc}")
        return None


def monitor_open_trade(
    parent_id: int,
    direction: str,
    entry: float,
    stop_px: float,
    target: float,
    risk: float,
    hard_close_dt: datetime,
    port: int,
    log: logging.Logger,
) -> Tuple[str, float]:
    try:
        from tws.ib_execution import IBKRExecutionClient

        client = IBKRExecutionClient(port=port, client_id=25)
        client.connect()
    except Exception as exc:
        log.error(f"[MONITORING] Cannot connect execution client: {exc}")
        return "BE", 0.0

    log.info(
        f"[MONITORING] Watching parentId={parent_id} | stop={stop_px:.2f}  target={target:.2f}"
    )

    try:
        while True:
            if datetime.now(NY) >= hard_close_dt:
                log.info("[MONITORING] Hard close 16:00 -- cancelling open bracket.")
                try:
                    client.cancel_all_children(parent_id)
                except Exception as exc:
                    log.warning(f"[MONITORING] Cancel error: {exc}")
                return "BE", 0.0

            try:
                status = client.get_order_status(parent_id)
                fill_price = client.get_fill_price(parent_id)
            except Exception as exc:
                log.warning(f"[MONITORING] Status check error: {exc}")
                time.sleep(10)
                continue

            if status in ("Filled", "Cancelled", "Inactive"):
                result_r = 0.0
                if fill_price is not None and risk > 0:
                    result_r = round(
                        (
                            (fill_price - entry) / risk
                            if direction == "LONG"
                            else (entry - fill_price) / risk
                        ),
                        2,
                    )
                outcome = (
                    "WIN" if result_r >= 2.5 else ("LOSS" if result_r <= -0.9 else "BE")
                )
                log.info(
                    f"[RESULT] {outcome}  result_r={result_r:+.2f}R  "
                    f"fill={fill_price}  status={status}"
                )
                return outcome, result_r

            time.sleep(10)
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


# -----------------------------------------------------------------------------
# Session summary
# -----------------------------------------------------------------------------


def _session_summary(
    log: logging.Logger,
    args: argparse.Namespace,
    trade_placed: bool,
    outcome: str = "N/A",
    result_r: float = 0.0,
) -> None:
    log.info("=" * 65)
    log.info("[SESSION SUMMARY]")
    log.info(f"  Symbol : {args.symbol}")
    log.info(f"  Mode   : {'PAPER' if args.paper else 'LIVE'}")
    log.info(f"  Risk   : ${args.risk:.2f} / trade")
    log.info(f"  Trade  : {'YES -- ' + outcome if trade_placed else 'NO TRADE'}")
    if trade_placed:
        log.info(f"  Result : {result_r:+.2f}R  (${result_r * args.risk:+.2f})")
    log.info("=" * 65)


# -----------------------------------------------------------------------------
# Scanning loop  --  KEY FIX: incremental bar fetching, no repeated full pulls
# -----------------------------------------------------------------------------


def run_scanning_loop(
    args: argparse.Namespace,
    cfg: FVGConfig,
    market_open_dt: datetime,
    trade_start_dt: datetime,
    cutoff_dt: datetime,
    hard_close_dt: datetime,
    log: logging.Logger,
) -> Tuple[bool, str, float]:
    """
    Incremental bar fetching strategy to avoid IBKR pacing violations:
      - First iteration: fetch all bars from market open -> now  (one big request)
      - Subsequent iterations: fetch only last 3 minutes of new bars, append
      - Result: 1 large request at start + 1 tiny request every 30s
        instead of 1 large request every 15s (which hits pacing at ~7.5 min)
    """
    today = datetime.now(NY).date()
    today_str = str(today)

    log.info("[SCANNING] Starting incremental scan loop (10:00 -> 15:00 NY)...")
    log.info("[SCANNING] First fetch: full day bars from market open...")

    # -- Initial full-day fetch ------------------------------------------------
    scan_end = datetime.now(NY).replace(second=0, microsecond=0)
    all_bars = fetch_bars(
        args.symbol, market_open_dt, scan_end, args.port, client_id=24
    )

    if all_bars.empty:
        log.warning("[SCANNING] Initial bar fetch returned empty -- cannot scan.")
        return False, "N/A", 0.0

    log.info(f"[SCANNING] Loaded {len(all_bars)} bars for today. Entering scan loop...")

    known_signal_time = None

    while datetime.now(NY) < cutoff_dt:
        now = datetime.now(NY)

        if now < trade_start_dt:
            remaining = (trade_start_dt - now).total_seconds()
            mins = int(remaining // 60)
            secs = int(remaining % 60)
            log.info(
                f"[SCANNING] Waiting for trade window (10:00 NY). {mins}m {secs:02d}s remaining..."
            )
            time.sleep(30)
            continue

        # -- Incremental fetch: only the last 3 minutes ------------------------
        # This is just 1 tiny request every 30s instead of a full-day request.
        # IBKR counts this as 1 pacing unit regardless of size.
        new_end = now.replace(second=0, microsecond=0)
        new_start = new_end - timedelta(minutes=3)

        new_bars = fetch_bars(args.symbol, new_start, new_end, args.port, client_id=24)

        if new_bars is not None and not new_bars.empty:
            # Append only truly new bars (deduplicate on timestamp)
            combined = pd.concat([all_bars, new_bars], ignore_index=True)
            all_bars = (
                combined.drop_duplicates(subset="timestamp")
                .sort_values("timestamp")
                .reset_index(drop=True)
            )

        # -- Run FVG strategy on full day bars ---------------------------------
        trades = generate_trades(all_bars, cfg, skip_filters=True)
        today_trades = [t for t in trades if str(t.get("date", "")) == today_str]

        if not today_trades:
            time.sleep(30)
            continue

        signal = today_trades[-1]
        sig_time = signal.get("entry_time")

        if sig_time == known_signal_time:
            time.sleep(30)
            continue

        known_signal_time = sig_time
        direction = signal["direction"]
        entry = signal["entry"]
        stop_px = signal["stop"]
        target = signal["target"]
        risk = abs(entry - stop_px)

        if risk <= 0:
            log.warning(f"[SIGNAL] Zero-risk signal skipped at {sig_time}")
            time.sleep(30)
            continue

        qty = max(1, math.floor(args.risk / risk))
        usd_risk = round(qty * risk, 2)
        log.info(
            f"[SIGNAL] {direction} @ {entry:.2f} | stop={stop_px:.2f}  target={target:.2f} | "
            f"qty={qty}  risk=${usd_risk}  target=${round(usd_risk * args.rr, 2)}"
        )

        parent_id = place_bracket_order(
            args.symbol, direction, entry, stop_px, target, qty, args.port, log
        )

        if parent_id is None:
            log.error("[SIGNAL] Order placement failed -- will not retry same signal.")
            time.sleep(30)
            continue

        # Journal: trade opened
        mode_str = "PAPER" if args.paper else "LIVE"
        entry_time_str = str(sig_time)[-8:-3] if sig_time else ""
        record_trade_open(
            symbol=args.symbol,
            mode=mode_str,
            started_at=datetime.now(NY).strftime("%H:%M"),
            direction=direction,
            entry_time=entry_time_str,
            entry=entry,
            stop=stop_px,
            target=target,
            qty=qty,
        )

        outcome, result_r = monitor_open_trade(
            parent_id,
            direction,
            entry,
            stop_px,
            target,
            risk,
            hard_close_dt,
            args.port,
            log,
        )

        # Journal: trade result
        pnl_usd = round(result_r * qty * risk, 2)
        record_trade_result(
            symbol=args.symbol,
            result=outcome,
            result_r=result_r,
            pnl_usd=pnl_usd,
        )

        return True, outcome, result_r

    log.info("[SCANNING] Session ended -- no trade taken today.")
    record_skip(
        symbol=args.symbol,
        mode="PAPER" if args.paper else "LIVE",
        reason="No signal detected",
        started_at=datetime.now(NY).strftime("%H:%M"),
    )
    return False, "N/A", 0.0


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main() -> None:
    from config import (
        SYMBOL,
        RISK_DOLLARS,
        IB_PORT,
        TRADING_MODE,
        WARMUP_DAYS,
        STRATEGY_CFG,
    )

    parser = argparse.ArgumentParser(description="FVG Live Trader")
    parser.add_argument("--symbol", default=SYMBOL)
    parser.add_argument("--port", type=int, default=IB_PORT)
    parser.add_argument("--risk", type=float, default=RISK_DOLLARS)
    parser.add_argument("--rr", type=float, default=STRATEGY_CFG.rr)
    parser.add_argument(
        "--live", action="store_true", help="Use live trading port (7496)"
    )
    parser.add_argument("--paper", action="store_true", help="Force paper mode (7497)")
    parser.add_argument("--warmup-days", type=int, default=WARMUP_DAYS)
    args = parser.parse_args()

    # --live overrides port; --paper overrides --live; default is paper
    if args.live and not args.paper:
        args.port = 7496
    elif args.paper:
        args.port = 7497

    paper = args.port == 7497
    args.paper = paper  # normalize for downstream use

    log = setup_logging(args.symbol, paper)
    now_ny = datetime.now(NY)
    today = now_ny.date()

    log.info("=" * 65)
    log.info(f"FVG Live Trader -- {'PAPER' if paper else 'LIVE'} MODE")
    log.info(f"  Symbol    : {args.symbol}")
    log.info(f"  Port      : {args.port} (paper={7497} / live={7496})")
    log.info(f"  Risk/trade: ${args.risk}")
    log.info(f"  Session   : 09:30 - 15:00 NY (entries)")
    log.info(f"  Monitoring: until 16:00 NY (open trade hard close)")
    log.info(f"  Trade win : 10:00 - 15:00 NY")
    log.info(f"  Script started at: {now_ny.strftime('%H:%M')} NY")
    log.info("=" * 65)

    # -- Weekend guard ----------------------------------------------------------
    if now_ny.weekday() >= 5:
        log.info(
            f"[WEEKEND] Today is {now_ny.strftime('%A')}. Markets closed. Exiting."
        )
        _session_summary(log, args, trade_placed=False)
        return

    market_open_dt = NY.localize(datetime(today.year, today.month, today.day, 9, 30))
    or_end_dt = NY.localize(datetime(today.year, today.month, today.day, 9, 35))
    trade_start_dt = NY.localize(datetime(today.year, today.month, today.day, 10, 0))
    cutoff_dt = NY.localize(datetime(today.year, today.month, today.day, 15, 0))
    hard_close_dt = NY.localize(datetime(today.year, today.month, today.day, 16, 0))

    # -- Pre-market wait --------------------------------------------------------
    while datetime.now(NY) < market_open_dt:
        remaining = (market_open_dt - datetime.now(NY)).total_seconds()
        mins = int(remaining // 60)
        secs = int(remaining % 60)
        log.info(
            f"[PRE-MARKET] Waiting for market open (09:30 NY). {mins}m {secs:02d}s remaining..."
        )
        time.sleep(30)

    log.info("[MARKET OPEN] Market is open. Starting warm-up.")

    # -- Warm-up (one request, done once) --------------------------------------
    warmup_start = market_open_dt - timedelta(days=args.warmup_days)
    log.info(
        f"[WARM-UP] Fetching {args.warmup_days} days of 1m bars for OR/ATR filters..."
    )
    warmup_df = fetch_bars(
        args.symbol, warmup_start, market_open_dt, args.port, client_id=21
    )

    if not warmup_df.empty:
        d_min = pd.to_datetime(warmup_df["timestamp"]).min().date()
        d_max = pd.to_datetime(warmup_df["timestamp"]).max().date()
        log.info(f"[WARM-UP] {len(warmup_df)} bars loaded ({d_min} to {d_max})")
    else:
        log.warning("[WARM-UP] No warm-up data -- proceeding without filters.")

    # -- Filters (computed ONCE here, never again in the scan loop) ------------
    log.info("[FILTER] Computing daily OR and ATR filters from warm-up data...")
    q20, q40, atr_threshold, _ = compute_filters(warmup_df, log)

    # -- Wait for opening range to close ---------------------------------------
    while datetime.now(NY) < or_end_dt:
        remaining = (or_end_dt - datetime.now(NY)).total_seconds()
        mins = int(remaining // 60)
        secs = int(remaining % 60)
        log.info(
            f"[OPENING RANGE] Waiting for 09:35 NY. {mins}m {secs:02d}s remaining..."
        )
        time.sleep(min(15, max(remaining, 1)))

    log.info("[OPENING RANGE] Window closed. Fetching today's OR and ATR...")

    # -- Today's OR -------------------------------------------------------------
    today_bars = fetch_bars(
        args.symbol, market_open_dt, or_end_dt, args.port, client_id=22
    )

    if today_bars is None or today_bars.empty:
        log.warning("[FILTER] No intraday bars today (holiday?). Exiting.")
        _session_summary(log, args, trade_placed=False)
        return

    or_width_today = float(today_bars["high"].max()) - float(today_bars["low"].min())
    atr_today = get_today_atr(warmup_df, today)
    log.info(
        f"[FILTER] Today OR={or_width_today:.3f}  ATR={'%.3f' % atr_today if atr_today else 'N/A'}"
    )

    if should_skip_day(or_width_today, atr_today, q20, q40, atr_threshold, log):
        log.info("[SESSION END] Day filtered. No trades today.")
        or_f = "SKIP" if (q20 is not None and q20 < or_width_today <= q40) else "PASS"
        atr_f = (
            "SKIP"
            if (atr_threshold and atr_today and atr_today < atr_threshold)
            else "PASS"
        )
        record_skip(
            symbol=args.symbol,
            mode="PAPER" if paper else "LIVE",
            reason=f"OR={or_f} ATR={atr_f}",
            started_at=now_ny.strftime("%H:%M"),
            or_filter=or_f,
            atr_filter=atr_f,
            or_width=or_width_today,
            atr_14=atr_today,
        )
        _session_summary(log, args, trade_placed=False)
        return

    # -- Strategy config --------------------------------------------------------
    cfg = FVGConfig(
        tick_size=STRATEGY_CFG.tick_size,
        rr=args.rr,
        session_start=STRATEGY_CFG.session_start,
        opening_end=STRATEGY_CFG.opening_end,
        trade_start=STRATEGY_CFG.trade_start,
        cutoff_time=STRATEGY_CFG.cutoff_time,
        one_trade_per_day=STRATEGY_CFG.one_trade_per_day,
        retest_mode=STRATEGY_CFG.retest_mode,
        use_or_filter=False,  # filters already applied above
        use_atr_filter=False,  # filters already applied above
    )

    # -- Scan + trade -----------------------------------------------------------
    trade_placed, outcome, result_r = run_scanning_loop(
        args, cfg, market_open_dt, trade_start_dt, cutoff_dt, hard_close_dt, log
    )

    _session_summary(
        log, args, trade_placed=trade_placed, outcome=outcome, result_r=result_r
    )


if __name__ == "__main__":
    main()

