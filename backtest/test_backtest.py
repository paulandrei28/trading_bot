import os
import sys
import glob
from pathlib import Path

import pandas as pd

# ensure project root on path when running directly
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

# ------------------------
# Settings
# ------------------------
symbol = "SPY"

cfg = FVGConfig(
    tick_size=0.01,     # stocks
    rr=3.0,
    session_start="09:30",
    opening_end="09:35",
    cutoff_time=None,
    one_trade_per_day=True,
    retest_mode="close",
    trade_start="10:00",
    trade_end="19:00",

    use_profit_lock=True,
    lock1_trigger_r=1.5,
    lock1_stop_r=0.5,
    lock2_trigger_r=2.5,
    lock2_stop_r=1.0,
)

# ------------------------
# Choose saved data file
# ------------------------
DATA_FILE = ""  # optional exact path e.g. "data/SPY_1m_2025-03-04_2026-03-04_ibkr.csv"

def pick_latest_data_file(sym: str) -> str:
    pattern = f"data/{sym}_1m_*_ibkr.csv"
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No saved data found for {sym}. Expected like: {pattern}")
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]

if DATA_FILE and os.path.exists(DATA_FILE):
    data_path = DATA_FILE
else:
    data_path = pick_latest_data_file(symbol)

print(f"[INFO] Using saved data file: {data_path}")

start_str = "start"
end_str = "end"
parts = Path(data_path).stem.split("_")
if len(parts) >= 5:
    start_str = parts[2]
    end_str = parts[3]

# ------------------------
# Output naming
# ------------------------
os.makedirs("logs", exist_ok=True)

TRADES_OUT = f"logs/trades_{symbol}_{start_str}_{end_str}.csv"
EQUITY_OUT = f"logs/equity_curve_{symbol}_{start_str}_{end_str}.csv"

PROFIT_HOUR_OUT = f"logs/profit_by_hour_{symbol}_{start_str}_{end_str}.csv"
PROFIT_30M_OUT = f"logs/profit_by_30min_{symbol}_{start_str}_{end_str}.csv"
DAILY_VOL_OUT = f"logs/daily_volatility_{symbol}_{start_str}_{end_str}.csv"
TRADES_VOL_OUT = f"logs/trades_with_volatility_{symbol}_{start_str}_{end_str}.csv"
OR_BINS_OUT = f"logs/perf_by_opening_range_bins_{symbol}_{start_str}_{end_str}.csv"
ATR_BINS_OUT = f"logs/perf_by_atr_bins_{symbol}_{start_str}_{end_str}.csv"

# ------------------------
# Load data
# ------------------------
df = pd.read_csv(data_path)

required_cols = {"timestamp", "open", "high", "low", "close", "volume"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Saved data is missing columns: {missing}. Found: {list(df.columns)}")

print(f"[INFO] Rows loaded: {len(df)}")

# ------------------------
# Run backtest
# ------------------------
trades = backtest(df, cfg)
print("Trades:", len(trades))

trades_df = pd.DataFrame(trades)
trades_df.to_csv(TRADES_OUT, index=False)
print(f"[OK] Saved trades -> {TRADES_OUT}")

if trades_df.empty:
    print("\nNo trades found.")
    sys.exit(0)

# ------------------------
# Performance report
# ------------------------
report = performance_report(trades_df)

print("\n===== BACKTEST REPORT =====")
for key, value in report.items():
    if key != "equity_curve":
        print(f"{key}: {value}")

report["equity_curve"].to_csv(EQUITY_OUT, index=False)
print(f"[OK] Saved equity curve -> {EQUITY_OUT}")

# ------------------------
# Profit by hour / time buckets
# ------------------------
hourly = profit_by_time_buckets(trades_df, bucket_minutes=60)
hourly.to_csv(PROFIT_HOUR_OUT, index=False)
print(f"[OK] Saved profit by hour -> {PROFIT_HOUR_OUT}")

half_hour = profit_by_time_buckets(trades_df, bucket_minutes=30)
half_hour.to_csv(PROFIT_30M_OUT, index=False)
print(f"[OK] Saved profit by 30min -> {PROFIT_30M_OUT}")

# ------------------------
# Volatility features + bins
# ------------------------
daily_vol = add_daily_volatility_features(df)
daily_vol.to_csv(DAILY_VOL_OUT, index=False)
print(f"[OK] Saved daily volatility -> {DAILY_VOL_OUT}")

trades_with_vol = attach_volatility_to_trades(trades_df, daily_vol)
trades_with_vol.to_csv(TRADES_VOL_OUT, index=False)
print(f"[OK] Saved trades with volatility -> {TRADES_VOL_OUT}")

or_bins = profit_by_volatility_bins(trades_with_vol, "opening_range_5m", bins=5)
or_bins.to_csv(OR_BINS_OUT, index=False)
print(f"[OK] Saved perf by opening range bins -> {OR_BINS_OUT}")

atr_bins = profit_by_volatility_bins(trades_with_vol, "atr_14", bins=5)
atr_bins.to_csv(ATR_BINS_OUT, index=False)
print(f"[OK] Saved perf by ATR bins -> {ATR_BINS_OUT}")

# ------------------------
# Quick console summary
# ------------------------
wins = (trades_df["result_r"] > 0).sum()
losses = (trades_df["result_r"] < 0).sum()
bes = (trades_df["result_r"] == 0).sum()
win_rate = wins / len(trades_df) * 100
expectancy = trades_df["result_r"].mean()

print(f"\nWins: {wins} | Losses: {losses} | BE: {bes}")
print(f"Win rate: {win_rate:.2f}%")
print(f"Expectancy: {expectancy:.2f} R")

print("\n[Profit by hour] (top rows)")
print(hourly.head(10))

print("\n[Opening range bins]")
print(or_bins)

print("\n[ATR(14) bins]")
print(atr_bins)

print("\nSample trades:")
print(trades_df.head(10))