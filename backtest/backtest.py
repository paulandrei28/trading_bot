import pandas as pd
from strategy.fvg_strategy import generate_trades, FVGConfig

def backtest(df: pd.DataFrame, cfg: FVGConfig = FVGConfig()):
    return generate_trades(df, cfg)