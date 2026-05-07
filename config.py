# =============================================================================
#  config.py  --  single file to configure ALL trading parameters
#  Switch between paper and live by changing TRADING_MODE below
# =============================================================================

# ---------------------------------------------
#  TRADING MODE  <-- change this line only
#  "paper" -> TWS paper account (port 7497)
#  "live"  -> TWS live account  (port 7496)
# ---------------------------------------------
TRADING_MODE = "paper"  # <-- "paper" or "live"

# ---------------------------------------------
#  CONNECTION (auto-set from TRADING_MODE)
# ---------------------------------------------
_PORTS = {"paper": 7497, "live": 7496}

IB_HOST = "127.0.0.1"
IB_PORT = _PORTS[TRADING_MODE]
IB_CLIENT_ID = 1  # must be unique per running script

# ---------------------------------------------
#  SYMBOL  <-- change to QQQ, IWM, etc.
# ---------------------------------------------
SYMBOL = "QQQ"
EXCHANGE = "SMART"
CURRENCY = "USD"

# ---------------------------------------------
#  POSITION SIZING
#  risk_dollars = how many $ you risk per trade
#  shares = risk_dollars / (entry - stop)
# ---------------------------------------------
RISK_DOLLARS = 100.0  # $ at risk per trade (adjust for account size)

# ---------------------------------------------
#  STRATEGY PARAMETERS  (must match backtest)
# ---------------------------------------------
from strategy.fvg_strategy import FVGConfig

STRATEGY_CFG = FVGConfig(
    tick_size=0.01,
    rr=3.0,
    session_start="09:30",
    opening_end="09:35",
    one_trade_per_day=True,
    retest_mode="close",
    trade_start="10:00",
    cutoff_time="15:00",
    use_or_filter=True,
    or_skip_pct_low=0.20,
    or_skip_pct_high=0.40,
    use_atr_filter=True,
    atr_min_pct=0.50,
)

# ---------------------------------------------
#  WARM-UP DATA
#  How many calendar days of 1m bars to fetch
#  at startup to compute OR/ATR filters
# ---------------------------------------------
WARMUP_DAYS = 30  # needs >= 14 trading days for ATR(14)

# ---------------------------------------------
#  SAFETY LIMITS
# ---------------------------------------------
MAX_DAILY_LOSS_R = 3  # halt today if cumulative R hits -3
MAX_TRADES_PER_DAY = 1  # enforced by one_trade_per_day in strategy
