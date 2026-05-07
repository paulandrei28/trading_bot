# FVG Trading Bot

Automated FVG (Fair Value Gap) breakout trading system for Interactive Brokers.
Supports paper and live trading with configurable filters and risk management.

## Features

- Opening range breakout + FVG retest + engulfing confirmation
- OR width and ATR(14) day filters (skip low-quality days)
- Bracket order execution (entry + stop + target)
- Daily journal CSV (auto-logged every session)
- Web dashboard for monitoring (Flask + Socket.IO)
- Backtest engine with Monte Carlo analysis

## Project structure

- `config.py` - **single source of truth** for all trading parameters
- `live_trader.py` - live/paper trading engine
- `daily_journal.py` - append-only daily trade journal
- `dashboard_app.py` + `dashboard.html` - web monitoring dashboard
- `strategy/fvg_strategy.py` - strategy logic and config dataclass
- `backtest/backtest.py` - backtest entry point
- `backtest/test_backtest.py` - main backtest test runner
- `backtest/metrics.py` - performance metrics
- `backtest/analysis.py` - time/volatility analysis
- `backtest/monte_carlo.py` - Monte Carlo engine
- `backtest/run_monte_carlo.py` - Monte Carlo runner
- `tws/ib_execution.py` - bracket order placement and monitoring (ib_insync)
- `tws/ib_history.py` - IBKR historical data downloader (ib_insync)

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Paper Trading

1. Open TWS and log in to your **paper trading** account
2. Enable API access (port 7497)
3. Run:

```bash
python live_trader.py
```

Or with custom options:

```bash
python live_trader.py --symbol SPY --risk 200 --rr 3.0
```

All parameters default from `config.py`. The bot will:
- Wait for market open (09:30 NY)
- Fetch warm-up data for filter computation
- Apply OR/ATR filters for the day
- Scan for FVG signals (10:00–15:00 NY)
- Place bracket orders automatically
- Log results to `logs/daily_journal.csv`

## Live Trading

```bash
python live_trader.py --live
```

Or set `TRADING_MODE = "live"` in `config.py`.

## Web Dashboard

```bash
python dashboard_app.py
# Open http://localhost:5000
```

## Run Backtest

```bash
python -m backtest.test_backtest
```

## Run Monte Carlo

```bash
python -m backtest.run_monte_carlo
```

## TWS Configuration

In Trader Workstation:
- `File` → `Global Configuration` → `API` → `Settings`
- Enable `Enable ActiveX and Socket Clients`
- Port `7497` for Paper, `7496` for Live
- Add `127.0.0.1` to trusted IPs

This script connects to TWS on `127.0.0.1:7497` and submits a paper-test bracket order example.
Use only on paper trading unless you intentionally change it.

## Download historical data from IBKR

Example usage in Python:

```python
from datetime import datetime
from tws.ib_history import IBKRHistoryClient, IBHistoryConfig

client = IBKRHistoryClient(IBHistoryConfig(port=7497, client_id=21))
df = client.fetch_1m_bars(
    symbol="SPY",
    start=datetime(2025, 3, 4, 9, 30),
    end=datetime(2026, 3, 4, 16, 0),
)
df.to_csv("data/SPY_1m_custom_ibkr.csv", index=False)
```

## Notes

- `logs/` is generated when you run tests and is ignored by git.
- The sample data file is kept so the project can be tested immediately.
