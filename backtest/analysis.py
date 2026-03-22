import pandas as pd
import numpy as np


def _ensure_datetime(trades_df: pd.DataFrame) -> pd.DataFrame:
    df = trades_df.copy()
    df["entry_time"] = pd.to_datetime(df["entry_time"], errors="coerce")
    return df.dropna(subset=["entry_time"])


def profit_by_time_buckets(trades_df: pd.DataFrame, bucket_minutes: int = 60) -> pd.DataFrame:
    """
    Groups performance by time bucket of entry_time.
    bucket_minutes=60 -> hourly
    bucket_minutes=30 -> half-hour buckets
    Returns a table with trades, win_rate, avg_r, total_r.
    """
    df = _ensure_datetime(trades_df)
    if df.empty:
        return pd.DataFrame()

    # bucket label: e.g. 10:00, 10:30 etc.
    minutes = (df["entry_time"].dt.hour * 60) + df["entry_time"].dt.minute
    bucket = (minutes // bucket_minutes) * bucket_minutes

    df["bucket_start_min"] = bucket
    df["bucket_label"] = df["bucket_start_min"].apply(
        lambda m: f"{m//60:02d}:{m%60:02d}"
    )

    g = df.groupby("bucket_label", sort=True)

    out = g.agg(
        trades=("result_r", "count"),
        wins=("result_r", lambda x: (x > 0).sum()),
        losses=("result_r", lambda x: (x < 0).sum()),
        be=("result_r", lambda x: (x == 0).sum()),
        avg_r=("result_r", "mean"),
        total_r=("result_r", "sum"),
    ).reset_index()

    out["win_rate_%"] = np.where(out["trades"] > 0, out["wins"] / out["trades"] * 100, 0.0)
    out = out.sort_values("bucket_label").reset_index(drop=True)
    return out


def add_daily_volatility_features(bars_df: pd.DataFrame) -> pd.DataFrame:
    """
    From 1m bars, compute per-day volatility features:
    - opening_range_5m (09:30-09:35 high-low)
    - atr_14_daily (approx using daily OHLC derived from 1m bars)
    - day_range (daily high-low)
    Returns daily_df indexed by date with these columns.
    """
    df = bars_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["timestamp_ny"] = df["timestamp"].dt.tz_convert("America/New_York")
    df["date"] = df["timestamp_ny"].dt.date

    # daily OHLC from 1m
    daily = df.groupby("date").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    )

    daily["day_range"] = daily["high"] - daily["low"]

    # True Range + ATR(14)
    prev_close = daily["close"].shift(1)
    tr = pd.concat([
        daily["high"] - daily["low"],
        (daily["high"] - prev_close).abs(),
        (daily["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)

    daily["tr"] = tr
    daily["atr_14"] = daily["tr"].rolling(14).mean()

    # opening range 09:30-09:35 (5 minutes)
    df["time_ny"] = df["timestamp_ny"].dt.time
    opening = df[
        (df["timestamp_ny"].dt.time >= pd.to_datetime("09:30").time()) &
        (df["timestamp_ny"].dt.time < pd.to_datetime("09:35").time())
    ].groupby("date").agg(
        or_high=("high", "max"),
        or_low=("low", "min")
    )
    opening["opening_range_5m"] = opening["or_high"] - opening["or_low"]
    opening = opening[["opening_range_5m"]]

    daily = daily.join(opening, how="left")
    return daily.reset_index()


def attach_volatility_to_trades(trades_df: pd.DataFrame, daily_vol_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds columns to trades_df: opening_range_5m, atr_14, day_range
    by matching on date.
    """
    t = trades_df.copy()
    t["date"] = pd.to_datetime(t["date"]).dt.date

    dv = daily_vol_df.copy()
    dv["date"] = pd.to_datetime(dv["date"]).dt.date

    merged = t.merge(dv[["date", "opening_range_5m", "atr_14", "day_range"]], on="date", how="left")
    return merged


def profit_by_volatility_bins(trades_df: pd.DataFrame, col: str, bins: int = 5) -> pd.DataFrame:
    """
    Bin trades by a volatility feature (e.g. opening_range_5m, atr_14)
    and compute performance per bin.
    """
    df = trades_df.copy()
    df = df.dropna(subset=[col])
    if df.empty:
        return pd.DataFrame()

    df["bin"] = pd.qcut(df[col], q=bins, duplicates="drop")

    g = df.groupby("bin", observed=True)
    out = g.agg(
        trades=("result_r", "count"),
        wins=("result_r", lambda x: (x > 0).sum()),
        losses=("result_r", lambda x: (x < 0).sum()),
        be=("result_r", lambda x: (x == 0).sum()),
        avg_r=("result_r", "mean"),
        total_r=("result_r", "sum"),
        vol_min=(col, "min"),
        vol_max=(col, "max"),
    ).reset_index(drop=True)

    out["win_rate_%"] = out["wins"] / out["trades"] * 100
    return out