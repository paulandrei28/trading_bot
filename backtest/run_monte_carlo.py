import os
import sys
import glob
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from backtest.monte_carlo import run_monte_carlo, summarize

# ----------------------------------------
# Settings -- edit as needed
# ----------------------------------------
TRADES_CSV = ""  # leave empty to auto-pick latest from logs/
SYMBOL = "QQQ"
N_SIMS = 10_000
WITH_REPLACEMENT = True  # True=bootstrap, False=shuffle-only


def pick_latest_trades(sym: str) -> str:
    pattern = f"logs/trades_{sym}_*.csv"
    files = [f for f in glob.glob(pattern) if "mc_" not in f]
    if not files:
        raise FileNotFoundError(f"No trades CSV found matching: {pattern}")
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]


csv_path = (
    TRADES_CSV
    if TRADES_CSV and os.path.exists(TRADES_CSV)
    else pick_latest_trades(SYMBOL)
)
print(f"[INFO] Running Monte Carlo on: {csv_path}")

df = pd.read_csv(csv_path)

if "result_r" not in df.columns:
    raise ValueError("CSV must contain a 'result_r' column.")

results = df["result_r"].astype(float).to_numpy()
print(f"[INFO] Trades in sample: {len(results)}")

mc = run_monte_carlo(
    results_r=results,
    n_sims=N_SIMS,
    sample_with_replacement=WITH_REPLACEMENT,
    seed=42,
)

summary = summarize(mc)

out_summary = csv_path.replace(".csv", f"_mc_summary_{N_SIMS}.csv")
out_sims = csv_path.replace(".csv", f"_mc_sims_{N_SIMS}.csv")
summary.to_csv(out_summary)
mc.to_csv(out_sims, index=False)

print(f"\nMonte Carlo sims: {N_SIMS} | replacement={WITH_REPLACEMENT}")
print("\n===== MONTE CARLO SUMMARY (percentiles) =====")
print(summary)
print(f"\n[OK] Saved summary -> {out_summary}")
print(f"[OK] Saved sims    -> {out_sims}")
