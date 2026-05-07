import numpy as np
import pandas as pd


def max_drawdown(equity: np.ndarray) -> float:
    if len(equity) == 0:
        return 0.0
    # FIX: vectorised NumPy instead of Python loop
    running_max = np.maximum.accumulate(equity)
    return float((equity - running_max).min())


def longest_losing_streak(results: np.ndarray) -> int:
    # treat result < 0 as loss; 0 (BE) resets streak
    streak = 0
    best = 0
    for r in results:
        if r < 0:
            streak += 1
            best = max(best, streak)
        else:
            streak = 0
    return best


def run_monte_carlo(
    results_r: np.ndarray,
    n_sims: int = 10_000,
    sample_with_replacement: bool = True,
    seed: int | None = 42,
) -> pd.DataFrame:
    """
    Monte Carlo over trade sequence.
    - with replacement : bootstrap (assumes IID trades)
    - without replacement: random permutation (same trades, different order)
    """
    if len(results_r) == 0:
        raise ValueError("results_r is empty -- nothing to simulate")

    rng = np.random.default_rng(seed)
    n = len(results_r)
    out = []

    for _ in range(n_sims):
        if sample_with_replacement:
            sim = rng.choice(results_r, size=n, replace=True)
        else:
            sim = results_r.copy()
            rng.shuffle(sim)

        equity = np.insert(np.cumsum(sim), 0, 0.0)  # include starting 0
        mdd = max_drawdown(equity)
        final_r = float(equity[-1])
        ll = longest_losing_streak(sim)

        out.append((final_r, mdd, ll))

    return pd.DataFrame(out, columns=["final_r", "max_drawdown_r", "max_losing_streak"])


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    qs = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]
    summary = pd.DataFrame(
        {
            "final_r": df["final_r"].quantile(qs),
            "max_drawdown_r": df["max_drawdown_r"].quantile(qs),
            "max_losing_streak": df["max_losing_streak"].quantile(qs),
        }
    )
    summary.index = [f"p{int(q * 100):02d}" for q in qs]
    return summary
