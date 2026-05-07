import os
import sys
import glob
from pathlib import Path

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from backtest.backtest import backtest
from strategy.fvg_strategy import FVGConfig
from backtest.metrics import performance_report
from backtest.analysis import (
    profit_by_time_buckets,
    add_daily_volatility_features,
    attach_volatility_to_trades,
    profit_by_volatility_bins,
)

# ------------------------------------------------
# Strategy config -- filtered version
# ------------------------------------------------
symbol = "QQQ"

cfg = FVGConfig(
    tick_size=0.01,
    rr=3.0,
    session_start="09:30",
    opening_end="09:35",
    one_trade_per_day=True,
    retest_mode="close",
    # Filter 1: time window
    trade_start="10:00",
    cutoff_time="15:00",
    # Filter 2: skip Q2 OR band (20th-40th pct = indecision days)
    use_or_filter=True,
    or_skip_pct_low=0.20,
    or_skip_pct_high=0.40,
    # Filter 3: skip low-ATR days (below median)
    use_atr_filter=True,
    atr_min_pct=0.50,
)

# ------------------------------------------------
# Data file selection
# ------------------------------------------------
DATA_FILE = ""


def pick_latest_data_file(sym: str) -> str:
    pattern = f"data/{sym}_1m_*_ibkr.csv"
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No saved data found for {sym}. Expected: {pattern}")
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


data_path = (
    DATA_FILE
    if DATA_FILE and os.path.exists(DATA_FILE)
    else pick_latest_data_file(symbol)
)
print(f"[INFO] Using saved data file: {data_path}")

parts = Path(data_path).stem.split("_")
start_str = parts[2] if len(parts) >= 5 else "start"
end_str = parts[3] if len(parts) >= 5 else "end"

# ------------------------------------------------
# Output paths
# ------------------------------------------------
os.makedirs("logs", exist_ok=True)

TRADES_OUT = f"logs/trades_{symbol}_{start_str}_{end_str}.csv"
EQUITY_OUT = f"logs/equity_curve_{symbol}_{start_str}_{end_str}.csv"
PROFIT_HOUR_OUT = f"logs/profit_by_hour_{symbol}_{start_str}_{end_str}.csv"
PROFIT_30M_OUT = f"logs/profit_by_30min_{symbol}_{start_str}_{end_str}.csv"
DAILY_VOL_OUT = f"logs/daily_volatility_{symbol}_{start_str}_{end_str}.csv"
TRADES_VOL_OUT = f"logs/trades_with_volatility_{symbol}_{start_str}_{end_str}.csv"
OR_BINS_OUT = f"logs/perf_by_opening_range_bins_{symbol}_{start_str}_{end_str}.csv"
ATR_BINS_OUT = f"logs/perf_by_atr_bins_{symbol}_{start_str}_{end_str}.csv"

# ------------------------------------------------
# Load & validate data
# ------------------------------------------------
df = pd.read_csv(data_path)

required_cols = {"timestamp", "open", "high", "low", "close", "volume"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Missing columns: {missing}. Found: {list(df.columns)}")

print(f"[INFO] Rows loaded: {len(df)}")

# ------------------------------------------------
# Backtest
# ------------------------------------------------
trades = backtest(df, cfg)
trades_df = pd.DataFrame(trades)
trades_df.to_csv(TRADES_OUT, index=False)
print(f"[OK] Saved trades -> {TRADES_OUT}  ({len(trades)} trades)")

if trades_df.empty:
    print("\nNo trades -- check data range and config.")
    sys.exit(0)

# ------------------------------------------------
# Performance report
# ------------------------------------------------
report = performance_report(trades_df)

print("\n===== BACKTEST REPORT =====")
for key, value in report.items():
    if key != "equity_curve":
        print(f"  {key}: {value}")

report["equity_curve"].to_csv(EQUITY_OUT, index=False)
print(f"[OK] Equity curve -> {EQUITY_OUT}")

# ------------------------------------------------
# Time & volatility analysis
# ------------------------------------------------
hourly = profit_by_time_buckets(trades_df, bucket_minutes=60)
half_hour = profit_by_time_buckets(trades_df, bucket_minutes=30)
hourly.to_csv(PROFIT_HOUR_OUT, index=False)
half_hour.to_csv(PROFIT_30M_OUT, index=False)

daily_vol = add_daily_volatility_features(df)
trades_with_vol = attach_volatility_to_trades(trades_df, daily_vol)
daily_vol.to_csv(DAILY_VOL_OUT, index=False)
trades_with_vol.to_csv(TRADES_VOL_OUT, index=False)

or_bins = profit_by_volatility_bins(trades_with_vol, "opening_range_5m", bins=5)
atr_bins = profit_by_volatility_bins(trades_with_vol, "atr_14", bins=5)
or_bins.to_csv(OR_BINS_OUT, index=False)
atr_bins.to_csv(ATR_BINS_OUT, index=False)

print(f"[OK] All analysis CSVs saved to logs/")

# ------------------------------------------------
# Console summary
# ------------------------------------------------
wins_n = (trades_df["result_r"] > 0).sum()
losses_n = (trades_df["result_r"] < 0).sum()
bes_n = (trades_df["result_r"] == 0).sum()
decisive = wins_n + losses_n
win_rate = wins_n / decisive * 100 if decisive > 0 else 0.0
expectancy = trades_df["result_r"].mean()

print(f"\nWins: {wins_n} | Losses: {losses_n} | BE: {bes_n}")
print(f"Win rate (excl. BE): {win_rate:.2f}%")
print(f"Expectancy: {expectancy:.2f} R")

print("\n[Profit by hour]")
print(hourly.to_string(index=False))

print("\n[Opening range bins]")
print(or_bins.to_string(index=False))

print("\n[ATR(14) bins]")
print(atr_bins.to_string(index=False))

print("\nSample trades:")
print(trades_df.head(10).to_string(index=False))
