import warnings
import pandas as pd
import pytz
from dataclasses import dataclass
from typing import List, Dict, Optional

NY = pytz.timezone("America/New_York")


@dataclass
class FVGConfig:
    tick_size: float = 0.01
    rr: float = 3.0
    session_start: str = "09:30"
    opening_end: str = "09:35"
    cutoff_time: Optional[str] = "15:00"  # FILTER 1: no late session
    one_trade_per_day: bool = True
    retest_mode: str = "close"

    # FILTER 1: earliest entry time
    trade_start: str = "10:00"

    # FILTER 2: skip OR Q2 band (medium indecision days)
    use_or_filter: bool = True
    or_skip_pct_low: float = 0.20
    or_skip_pct_high: float = 0.40

    # FILTER 3: ATR floor (only trade above-median volatility)
    use_atr_filter: bool = True
    atr_min_pct: float = 0.50


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
    if direction == "LONG":
        return df.iloc[i + 1]["low"] > df.iloc[i - 1]["high"]
    else:
        return df.iloc[i + 1]["high"] < df.iloc[i - 1]["low"]


def in_fvg(curr, fvg_low: float, fvg_high: float, mode: str) -> bool:
    if mode == "wick":
        return (curr["low"] <= fvg_high) and (curr["high"] >= fvg_low)
    return fvg_low <= curr["close"] <= fvg_high


def _build_daily_filters(df: pd.DataFrame, cfg: "FVGConfig") -> dict:
    """
    Pre-compute OR width and ATR(14) for every day in df,
    then derive filter thresholds from the full distribution.
    Returns dict: date -> {"or_ok": bool, "atr_ok": bool}
    """
    df = df.copy()
    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["ts_ny"] = ts.dt.tz_convert(NY)
    df["date"] = df["ts_ny"].dt.date

    s_t = pd.to_datetime(cfg.session_start).time()
    e_t = pd.to_datetime(cfg.opening_end).time()

    # Opening range per day
    or_mask = (df["ts_ny"].dt.time >= s_t) & (df["ts_ny"].dt.time < e_t)
    or_df = (
        df[or_mask].groupby("date").agg(or_high=("high", "max"), or_low=("low", "min"))
    )
    or_df["or_width"] = or_df["or_high"] - or_df["or_low"]

    # Daily OHLC + Wilder ATR(14)
    daily = df.groupby("date").agg(
        high=("high", "max"), low=("low", "min"), close=("close", "last")
    )
    prev_close = daily["close"].shift(1)
    tr = pd.concat(
        [
            daily["high"] - daily["low"],
            (daily["high"] - prev_close).abs(),
            (daily["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    daily["atr_14"] = tr.ewm(span=27, min_periods=14, adjust=False).mean()

    combined = or_df.join(daily[["atr_14"]], how="left")

    or_lo = combined["or_width"].quantile(cfg.or_skip_pct_low)
    or_hi = combined["or_width"].quantile(cfg.or_skip_pct_high)
    atr_th = combined["atr_14"].quantile(cfg.atr_min_pct)

    print(f"[FILTER] OR skip band:     ({or_lo:.3f}, {or_hi:.3f}]")
    print(f"[FILTER] ATR min threshold: {atr_th:.3f}")

    day_filter = {}
    skipped_or = skipped_atr = 0
    for date, row in combined.iterrows():
        or_ok = atr_ok = True
        if cfg.use_or_filter and not pd.isna(row["or_width"]):
            if or_lo < row["or_width"] <= or_hi:
                or_ok = False
                skipped_or += 1
        if cfg.use_atr_filter and not pd.isna(row["atr_14"]):
            if row["atr_14"] < atr_th:
                atr_ok = False
                skipped_atr += 1
        day_filter[date] = {"or_ok": or_ok, "atr_ok": atr_ok}

    total = len(combined)
    print(f"[FILTER] OR  skipped: {skipped_or}/{total} days")
    print(f"[FILTER] ATR skipped: {skipped_atr}/{total} days")
    return day_filter


def generate_trades(df: pd.DataFrame, cfg: FVGConfig = FVGConfig()) -> List[Dict]:
    """
    Opening range breakout + FVG retest + engulfing confirmation.
    Fixed RR (default 3:1). Outcomes: WIN / LOSS / BE.

    Active filters (all toggleable in FVGConfig):
      1. Time window  : entries only trade_start <= t < cutoff_time
      2. OR skip band : skip days where OR width is in the Q2 percentile band
      3. ATR floor    : skip days where ATR(14) < atr_min_pct percentile
    """
    if df is None or df.empty:
        return []

    day_filter = _build_daily_filters(df, cfg)

    df = df.copy()
    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["timestamp"] = ts.dt.tz_convert(NY)
    df = df.dropna(subset=["timestamp"])
    df["date"] = df["timestamp"].dt.date

    session_start_t = pd.to_datetime(cfg.session_start).time()
    opening_end_t = pd.to_datetime(cfg.opening_end).time()
    cutoff_t = pd.to_datetime(cfg.cutoff_time).time() if cfg.cutoff_time else None
    trade_start_t = pd.to_datetime(cfg.trade_start).time()

    trades: List[Dict] = []
    days_traded = days_filtered = 0

    for day, day_df in df.groupby("date"):
        day_df = day_df.sort_values("timestamp").reset_index(drop=True)

        # Day-level filters
        filt = day_filter.get(day, {"or_ok": True, "atr_ok": True})
        if not filt["or_ok"] or not filt["atr_ok"]:
            days_filtered += 1
            continue

        opening = day_df[
            (day_df["timestamp"].dt.time >= session_start_t)
            & (day_df["timestamp"].dt.time < opening_end_t)
        ]
        if len(opening) < 5:
            continue

        or_high = float(opening["high"].max())
        or_low = float(opening["low"].min())

        trade_taken = False
        days_traded += 1

        for i in range(2, len(day_df) - 2):
            candle = day_df.iloc[i]
            t = candle["timestamp"].time()

            if t < opening_end_t:
                continue
            if cutoff_t and t >= cutoff_t:
                break

            if candle["close"] > or_high:
                direction = "LONG"
            elif candle["close"] < or_low:
                direction = "SHORT"
            else:
                continue

            if not detect_fvg(day_df, i, direction):
                continue

            if direction == "LONG":
                fvg_low = float(day_df.iloc[i - 1]["high"])
                fvg_high = float(day_df.iloc[i + 1]["low"])
            else:
                fvg_high = float(day_df.iloc[i - 1]["low"])
                fvg_low = float(day_df.iloc[i + 1]["high"])

            for j in range(i + 2, len(day_df) - 1):
                curr = day_df.iloc[j]
                prev = day_df.iloc[j - 1]
                tt = curr["timestamp"].time()

                if cutoff_t and tt >= cutoff_t:
                    break
                if tt < trade_start_t:
                    continue
                if not in_fvg(curr, fvg_low, fvg_high, cfg.retest_mode):
                    continue

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

                result_r = 0.0
                outcome = "BE"

                for k in range(j + 1, len(day_df)):
                    price = day_df.iloc[k]
                    if direction == "LONG":
                        if price["low"] <= stop:
                            result_r = -1.0
                            outcome = "LOSS"
                            break
                        if price["high"] >= target:
                            result_r = float(cfg.rr)
                            outcome = "WIN"
                            break
                    else:
                        if price["high"] >= stop:
                            result_r = -1.0
                            outcome = "LOSS"
                            break
                        if price["low"] <= target:
                            result_r = float(cfg.rr)
                            outcome = "WIN"
                            break

                trades.append(
                    {
                        "date": day,
                        "direction": direction,
                        "entry_time": curr["timestamp"],
                        "entry": round(entry, 4),
                        "stop": round(stop, 4),
                        "target": round(target, 4),
                        "risk": round(risk, 4),
                        "result_r": float(result_r),
                        "outcome": outcome,
                    }
                )

                trade_taken = True
                break

            if trade_taken and cfg.one_trade_per_day:
                break

    print(f"[FILTER] Days traded: {days_traded} | Days filtered out: {days_filtered}")
    return trades
