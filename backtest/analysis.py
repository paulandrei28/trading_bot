import pandas as pd
import numpy as np
import warnings


def _ensure_datetime(trades_df: pd.DataFrame) -> pd.DataFrame:
    df = trades_df.copy()
    df["entry_time"] = pd.to_datetime(df["entry_time"], errors="coerce")
    dropped = df["entry_time"].isna().sum()
    if dropped > 0:
        warnings.warn(
            f"_ensure_datetime: dropped {dropped} rows with invalid entry_time",
            stacklevel=2,
        )
    return df.dropna(subset=["entry_time"])


def profit_by_time_buckets(
    trades_df: pd.DataFrame, bucket_minutes: int = 60
) -> pd.DataFrame:
    """
    Groups performance by time bucket of entry_time.
    bucket_minutes=60  -> hourly
    bucket_minutes=30  -> half-hour buckets
    """
    df = _ensure_datetime(trades_df)
    if df.empty:
        return pd.DataFrame()

    minutes = (df["entry_time"].dt.hour * 60) + df["entry_time"].dt.minute
    bucket = (minutes // bucket_minutes) * bucket_minutes

    df = df.copy()
    df["bucket_label"] = bucket.apply(lambda m: f"{m // 60:02d}:{m % 60:02d}")

    g = df.groupby("bucket_label")

    out = g.agg(
        trades=("result_r", "count"),
        wins=("result_r", lambda x: (x > 0).sum()),
        losses=("result_r", lambda x: (x < 0).sum()),
        be=("result_r", lambda x: (x == 0).sum()),
        avg_r=("result_r", "mean"),
        total_r=("result_r", "sum"),
    ).reset_index()

    contested = out["wins"] + out["losses"]
    out["win_rate_%"] = np.where(
        contested > 0,
        out["wins"] / contested * 100,
        0.0,
    )

    return out.sort_values("bucket_label").reset_index(drop=True)


def add_daily_volatility_features(bars_df: pd.DataFrame) -> pd.DataFrame:
    """
    From 1m bars, compute per-day volatility features:
      - opening_range_5m  (09:30–09:35 high-low)
      - atr_14            (rolling 14-day ATR, simple MA — note: not Wilder EMA)
      - day_range         (daily high-low)
    Returns daily DataFrame indexed by date.
    """
    df = bars_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["timestamp_ny"] = df["timestamp"].dt.tz_convert("America/New_York")
    df["date"] = df["timestamp_ny"].dt.date

    # Daily OHLC from 1m bars
    daily = df.groupby("date").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    )

    daily["day_range"] = daily["high"] - daily["low"]

    # True Range + ATR(14) simple rolling mean
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
    daily["atr_14"] = daily["tr"].rolling(14).mean()

    # Opening range 09:30–09:35
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

    daily = daily.join(opening[["opening_range_5m"]], how="left")
    return daily.reset_index()


def attach_volatility_to_trades(
    trades_df: pd.DataFrame, daily_vol_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Adds opening_range_5m, atr_14, day_range columns to trades_df by date.
    """
    if "date" not in trades_df.columns:
        raise ValueError("trades_df must contain a 'date' column")

    t = trades_df.copy()
    t["date"] = pd.to_datetime(t["date"]).dt.date

    dv = daily_vol_df.copy()
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
    Bin trades by a volatility feature (opening_range_5m, atr_14, …)
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

    contested = out["wins"] + out["losses"]
    out["win_rate_%"] = np.where(
        contested > 0,
        out["wins"] / contested * 100,
        0.0,
    )

    return out
