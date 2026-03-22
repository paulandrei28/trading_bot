import pandas as pd


def build_equity_curve(trades_df, starting_r=0):
    """
    Build cumulative equity curve in R units.
    """
    equity = []
    current = starting_r

    for r in trades_df["result_r"]:
        current += r
        equity.append(current)

    trades_df["equity_r"] = equity
    return trades_df


def max_drawdown(equity_series):
    """
    Calculate max drawdown in R units.
    """
    peak = equity_series.iloc[0]
    max_dd = 0

    for value in equity_series:
        if value > peak:
            peak = value

        drawdown = value - peak

        if drawdown < max_dd:
            max_dd = drawdown

    return max_dd


def performance_report(trades_df):
    """
    Generate performance statistics.
    """

    total_trades = len(trades_df)

    wins = trades_df[trades_df["result_r"] > 0]
    losses = trades_df[trades_df["result_r"] < 0]
    breakeven = trades_df[trades_df["result_r"] == 0]

    win_rate = len(wins) / total_trades * 100 if total_trades else 0

    expectancy = trades_df["result_r"].mean()

    gross_profit = wins["result_r"].sum()
    gross_loss = abs(losses["result_r"].sum())

    profit_factor = gross_profit / gross_loss if gross_loss != 0 else 0

    trades_df = build_equity_curve(trades_df)

    max_dd = max_drawdown(trades_df["equity_r"])

    return {
        "Trades": total_trades,
        "Wins": len(wins),
        "Losses": len(losses),
        "BE": len(breakeven),
        "Win Rate": round(win_rate, 2),
        "Expectancy": round(expectancy, 2),
        "Profit Factor": round(profit_factor, 2),
        "Max Drawdown (R)": round(max_dd, 2),
        "Total Return (R)": round(trades_df["equity_r"].iloc[-1], 2),
        "equity_curve": trades_df
    }