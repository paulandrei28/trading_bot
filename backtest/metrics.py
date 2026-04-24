import pandas as pd
import numpy as np


def build_equity_curve(
    trades_df: pd.DataFrame, starting_r: float = 0.0
) -> pd.DataFrame:
    """Build cumulative equity curve in R units."""
    df = trades_df.copy()
    df["equity_r"] = (
        starting_r + df["result_r"].cumsum()
    )  # FIX: vectorised, no Python loop
    return df


def max_drawdown(equity_series: pd.Series) -> float:
    """Calculate max drawdown in R units."""
    # FIX: handle empty series gracefully
    if equity_series.empty:
        return 0.0
    running_max = equity_series.cummax()  # FIX: vectorised, no Python loop
    drawdown = equity_series - running_max
    return float(drawdown.min())


def performance_report(trades_df: pd.DataFrame) -> dict:
    """Generate performance statistics."""
    if trades_df.empty:
        return {"error": "No trades to report"}

    total_trades = len(trades_df)
    wins = trades_df[trades_df["result_r"] > 0]
    losses = trades_df[trades_df["result_r"] < 0]
    breakeven = trades_df[trades_df["result_r"] == 0]

    # FIX: win rate excludes BE trades (standard trading convention)
    decisive = len(wins) + len(losses)
    win_rate = len(wins) / decisive * 100 if decisive > 0 else 0.0

    expectancy = float(trades_df["result_r"].mean())

    gross_profit = float(wins["result_r"].sum())
    gross_loss = float(abs(losses["result_r"].sum()))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    trades_df = build_equity_curve(trades_df)
    max_dd = max_drawdown(trades_df["equity_r"])

    # FIX: guard against empty equity curve
    total_return = float(trades_df["equity_r"].iloc[-1]) if total_trades > 0 else 0.0

    return {
        "Trades": total_trades,
        "Wins": len(wins),
        "Losses": len(losses),
        "BE": len(breakeven),
        "Win Rate": round(win_rate, 2),
        "Expectancy": round(expectancy, 2),
        "Profit Factor": round(profit_factor, 2),
        "Max Drawdown (R)": round(max_dd, 2),
        "Total Return (R)": round(total_return, 2),
        "equity_curve": trades_df,
    }
