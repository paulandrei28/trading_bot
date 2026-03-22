import pandas as pd
import pytz
from dataclasses import dataclass
from typing import List, Dict, Optional

NY = pytz.timezone("America/New_York")


@dataclass
class FVGConfig:
    tick_size: float = 0.25
    rr: float = 3.0
    session_start: str = "09:30"
    opening_end: str = "09:35"
    cutoff_time: Optional[str] = None  # e.g. "12:00"
    one_trade_per_day: bool = True
    retest_mode: str = "close"  # "close" or "wick"

    # Trading window (entry candle must be inside)
    trade_start: str = "10:00"
    trade_end: str = "12:00"

    # 2-stage profit lock
    use_profit_lock: bool = True
    lock1_trigger_r: float = 1.5   # when price reaches +1.5R
    lock1_stop_r: float = 0.5      # move stop to +0.5R
    lock2_trigger_r: float = 2.5   # when price reaches +2.5R
    lock2_stop_r: float = 1.0      # move stop to +1.0R


def is_bullish_engulfing(prev, curr) -> bool:
    return (
        curr["close"] > curr["open"]
        and curr["open"] < prev["close"]
        and curr["close"] > prev["open"]
    )


def is_bearish_engulfing(prev, curr) -> bool:
    return (
        curr["close"] < curr["open"]
        and curr["open"] > prev["close"]
        and curr["close"] < prev["open"]
    )


def detect_fvg(df: pd.DataFrame, i: int, direction: str) -> bool:
    # Strict 3-candle FVG definition using wicks
    if direction == "LONG":
        return df.iloc[i + 1]["low"] > df.iloc[i - 1]["high"]
    else:
        return df.iloc[i + 1]["high"] < df.iloc[i - 1]["low"]


def in_fvg(curr, fvg_low: float, fvg_high: float, mode: str) -> bool:
    if mode == "wick":
        # any wick/body overlap with the gap
        return (curr["low"] <= fvg_high) and (curr["high"] >= fvg_low)
    # default: close-in-gap
    return fvg_low <= curr["close"] <= fvg_high


def generate_trades(df: pd.DataFrame, cfg: FVGConfig = FVGConfig()) -> List[Dict]:
    """
    First 5-min range break + FVG retest + engulfing strategy.
    Adds 2-stage profit lock:
      - at +1.5R, stop -> +0.5R
      - at +2.5R, stop -> +1.0R
    Returns trade list with: date, direction, entry_time, entry, stop, target, result_r, outcome
    """

    if df is None or df.empty:
        return []

    # Ensure timestamp is tz-aware in NY
    df = df.copy()
    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["timestamp"] = ts.dt.tz_convert(NY)
    df = df.dropna(subset=["timestamp"])
    df["date"] = df["timestamp"].dt.date

    session_start_t = pd.to_datetime(cfg.session_start).time()
    opening_end_t = pd.to_datetime(cfg.opening_end).time()
    cutoff_t = pd.to_datetime(cfg.cutoff_time).time() if cfg.cutoff_time else None
    trade_start_t = pd.to_datetime(cfg.trade_start).time()
    trade_end_t = pd.to_datetime(cfg.trade_end).time()

    trades: List[Dict] = []

    for day, day_df in df.groupby("date"):
        day_df = day_df.sort_values("timestamp").reset_index(drop=True)

        # Opening range 09:30–09:35
        opening = day_df[
            (day_df["timestamp"].dt.time >= session_start_t)
            & (day_df["timestamp"].dt.time < opening_end_t)
        ]
        if len(opening) < 5:
            continue

        or_high = float(opening["high"].max())
        or_low = float(opening["low"].min())

        trade_taken = False

        # scan for first breakout + fvg
        for i in range(6, len(day_df) - 2):
            candle = day_df.iloc[i]
            t = candle["timestamp"].time()

            if t < opening_end_t:
                continue
            if cutoff_t and t > cutoff_t:
                break

            # Breakout (close-based)
            if candle["close"] > or_high:
                direction = "LONG"
            elif candle["close"] < or_low:
                direction = "SHORT"
            else:
                continue

            # FVG must exist on the 3-candle structure around i
            if not detect_fvg(day_df, i, direction):
                continue

            # FVG boundaries
            if direction == "LONG":
                fvg_low = float(day_df.iloc[i - 1]["high"])
                fvg_high = float(day_df.iloc[i + 1]["low"])
            else:
                fvg_high = float(day_df.iloc[i - 1]["low"])
                fvg_low = float(day_df.iloc[i + 1]["high"])

            # Retest + engulfing entry
            for j in range(i + 2, len(day_df) - 1):
                curr = day_df.iloc[j]
                prev = day_df.iloc[j - 1]
                tt = curr["timestamp"].time()

                if cutoff_t and tt > cutoff_t:
                    break

                # Entry time filter
                if tt < trade_start_t or tt > trade_end_t:
                    continue

                if not in_fvg(curr, fvg_low, fvg_high, cfg.retest_mode):
                    continue

                # Confirmation: engulfing candle
                if direction == "LONG" and is_bullish_engulfing(prev, curr):
                    entry = float(curr["close"])
                    stop = float(curr["low"]) - cfg.tick_size
                    risk = entry - stop
                    if risk <= 0:
                        continue
                    target = entry + cfg.rr * risk

                elif direction == "SHORT" and is_bearish_engulfing(prev, curr):
                    entry = float(curr["close"])
                    stop = float(curr["high"]) + cfg.tick_size
                    risk = stop - entry
                    if risk <= 0:
                        continue
                    target = entry - cfg.rr * risk

                else:
                    continue

                # ----------------------------
                # Outcome scan forward (2-stage profit lock)
                # ----------------------------
                result_r = 0.0
                outcome = "BE"
                stage = 0  # 0=orig stop, 1=stop@+0.5R, 2=stop@+1R

                if direction == "LONG":
                    lock1_trigger = entry + cfg.lock1_trigger_r * risk
                    lock2_trigger = entry + cfg.lock2_trigger_r * risk
                    lock1_stop = entry + cfg.lock1_stop_r * risk
                    lock2_stop = entry + cfg.lock2_stop_r * risk
                else:
                    lock1_trigger = entry - cfg.lock1_trigger_r * risk
                    lock2_trigger = entry - cfg.lock2_trigger_r * risk
                    lock1_stop = entry - cfg.lock1_stop_r * risk
                    lock2_stop = entry - cfg.lock2_stop_r * risk

                for k in range(j + 1, len(day_df)):
                    price = day_df.iloc[k]

                    if direction == "LONG":
                        if cfg.use_profit_lock:
                            if stage < 1 and price["high"] >= lock1_trigger:
                                stage = 1
                            if stage < 2 and price["high"] >= lock2_trigger:
                                stage = 2

                        active_stop = stop if stage == 0 else (lock1_stop if stage == 1 else lock2_stop)

                        # stop hit
                        if price["low"] <= active_stop:
                            if stage == 0:
                                result_r = -1.0
                                outcome = "LOSS"
                            elif stage == 1:
                                result_r = float(cfg.lock1_stop_r)
                                outcome = "LOCK1"
                            else:
                                result_r = float(cfg.lock2_stop_r)
                                outcome = "LOCK2"
                            break

                        # target hit
                        if price["high"] >= target:
                            result_r = float(cfg.rr)
                            outcome = "WIN"
                            break

                    else:  # SHORT
                        if cfg.use_profit_lock:
                            if stage < 1 and price["low"] <= lock1_trigger:
                                stage = 1
                            if stage < 2 and price["low"] <= lock2_trigger:
                                stage = 2

                        active_stop = stop if stage == 0 else (lock1_stop if stage == 1 else lock2_stop)

                        if price["high"] >= active_stop:
                            if stage == 0:
                                result_r = -1.0
                                outcome = "LOSS"
                            elif stage == 1:
                                result_r = float(cfg.lock1_stop_r)
                                outcome = "LOCK1"
                            else:
                                result_r = float(cfg.lock2_stop_r)
                                outcome = "LOCK2"
                            break

                        if price["low"] <= target:
                            result_r = float(cfg.rr)
                            outcome = "WIN"
                            break

                trades.append({
                    "date": day,
                    "direction": direction,
                    "entry_time": curr["timestamp"],
                    "entry": round(entry, 2),
                    "stop": round(stop, 2),
                    "target": round(target, 2),
                    "result_r": float(result_r),
                    "outcome": outcome,
                })

                trade_taken = True
                break

            if trade_taken and cfg.one_trade_per_day:
                break

    return trades