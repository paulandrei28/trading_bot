import warnings
import pandas as pd
import numpy as np


def _ensure_datetime(trades_df: pd.DataFrame) -> pd.DataFrame:
    df = trades_df.copy()
    df["entry_time"] = pd.to_datetime(df["entry_time"], errors="coerce")
    dropped = df["entry_time"].isna().sum()
    if dropped > 0:
        warnings.warn(
            f"[analysis] _ensure_datetime: dropped {dropped} rows with invalid entry_time"
        )
    return df.dropna(subset=["entry_time"])


def profit_by_time_buckets(
    trades_df: pd.DataFrame, bucket_minutes: int = 60
) -> pd.DataFrame:
    """
    Groups performance by time bucket of entry_time.
    bucket_minutes=60 -> hourly, bucket_minutes=30 -> half-hour.
    Returns a table with trades, win_rate, avg_r, total_r.
    win_rate_% is computed as wins / (wins + losses), excluding BE trades.
    """
    df = _ensure_datetime(trades_df)
    if df.empty:
        return pd.DataFrame()

    minutes = (df["entry_time"].dt.hour * 60) + df["entry_time"].dt.minute
    bucket = (minutes // bucket_minutes) * bucket_minutes

    df["bucket_label"] = bucket.apply(lambda m: f"{m // 60:02d}:{m % 60:02d}")
    # FIX: removed redundant bucket_start_min column

    g = df.groupby("bucket_label", sort=True)

    out = g.agg(
        trades=("result_r", "count"),
        wins=("result_r", lambda x: (x > 0).sum()),
        losses=("result_r", lambda x: (x < 0).sum()),
        be=("result_r", lambda x: (x == 0).sum()),
        avg_r=("result_r", "mean"),
        total_r=("result_r", "sum"),
    ).reset_index()

    # FIX: win rate excludes BE trades (wins / (wins + losses)), which is standard in trading analysis
    decisive = out["wins"] + out["losses"]
    out["win_rate_%"] = np.where(decisive > 0, out["wins"] / decisive * 100, 0.0)

    return out.reset_index(drop=True)


def add_daily_volatility_features(bars_df: pd.DataFrame) -> pd.DataFrame:
    """
    From 1m bars, compute per-day volatility features:
    - opening_range_5m (09:30-09:35 high-low)
    - atr_14 (Wilder's ATR using EWM, period=14)
    - day_range (daily high-low)
    Returns daily_df indexed by date with these columns.
    """
    df = bars_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"])

    # FIX: guard against naive timestamps
    if df["timestamp"].dt.tz is None:
        warnings.warn(
            "[analysis] add_daily_volatility_features: timestamps appear tz-naive, localising as UTC"
        )
        df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")

    df["timestamp_ny"] = df["timestamp"].dt.tz_convert("America/New_York")
    df["date"] = df["timestamp_ny"].dt.date

    daily = df.groupby("date").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    )

    daily["day_range"] = daily["high"] - daily["low"]

    prev_close = daily["close"].shift(1)
    tr = pd.concat(
        [
            daily["high"] - daily["low"],
            (daily["high"] - prev_close).abs(),
            (daily["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    daily["tr"] = tr

    # FIX: use Wilder's EMA (span = 2*period - 1) instead of simple rolling mean
    daily["atr_14"] = daily["tr"].ewm(span=27, min_periods=14, adjust=False).mean()

    # Opening range 09:30-09:35 (first 5 minutes)
    opening = (
        df[
            (df["timestamp_ny"].dt.time >= pd.to_datetime("09:30").time())
            & (df["timestamp_ny"].dt.time < pd.to_datetime("09:35").time())
        ]
        .groupby("date")
        .agg(
            or_high=("high", "max"),
            or_low=("low", "min"),
        )
    )
    opening["opening_range_5m"] = opening["or_high"] - opening["or_low"]
    opening = opening[["opening_range_5m"]]
    # FIX: removed dead "time_ny" column

    daily = daily.join(opening, how="left")
    return daily.reset_index()


def attach_volatility_to_trades(
    trades_df: pd.DataFrame, daily_vol_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Adds columns to trades_df: opening_range_5m, atr_14, day_range
    by matching on date.
    """
    # FIX: validate required column exists
    if "date" not in trades_df.columns:
        raise ValueError("trades_df must contain a 'date' column")

    t = trades_df.copy()
    dv = daily_vol_df.copy()

    t["date"] = pd.to_datetime(t["date"]).dt.date
    dv["date"] = pd.to_datetime(dv["date"]).dt.date

    return t.merge(
        dv[["date", "opening_range_5m", "atr_14", "day_range"]],
        on="date",
        how="left",
    )


def profit_by_volatility_bins(
    trades_df: pd.DataFrame, col: str, bins: int = 5
) -> pd.DataFrame:
    """
    Bin trades by a volatility feature (e.g. opening_range_5m, atr_14)
    and compute performance per bin.
    win_rate_% excludes BE trades.
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
    ).reset_index(drop=True)

    # FIX: guard against division by zero; exclude BE from win rate
    decisive = out["wins"] + out["losses"]
    out["win_rate_%"] = np.where(decisive > 0, out["wins"] / decisive * 100, 0.0)

    return out
