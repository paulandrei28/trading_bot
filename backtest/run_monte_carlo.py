import os
import sys
import pandas as pd

# ensure project root on path when running directly
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from backtest.monte_carlo import run_monte_carlo, summarize


# Trades file here:
TRADES_CSV = "logs/trades_SPY_2025-03-04_2026-03-04.csv"

N_SIMS = 10000
WITH_REPLACEMENT = True   # True=bootstrap, False=shuffle-only


df = pd.read_csv(TRADES_CSV)

if "result_r" not in df.columns:
    raise ValueError("CSV must contain a 'result_r' column.")

results = df["result_r"].astype(float).to_numpy()

mc = run_monte_carlo(
    results_r=results,
    n_sims=N_SIMS,
    sample_with_replacement=WITH_REPLACEMENT,
    seed=42,
)

summary = summarize(mc)

out_summary = TRADES_CSV.replace(".csv", f"_mc_summary_{N_SIMS}.csv")
out_sims = TRADES_CSV.replace(".csv", f"_mc_sims_{N_SIMS}.csv")

summary.to_csv(out_summary)
mc.to_csv(out_sims, index=False)

print(f"\nMonte Carlo sims: {N_SIMS} | replacement={WITH_REPLACEMENT}")
print(f"Trades in sample: {len(results)}")
print("\n===== MONTE CARLO SUMMARY (percentiles) =====")
print(summary)

print(f"\n[OK] Saved summary -> {out_summary}")
print(f"[OK] Saved sims    -> {out_sims}")