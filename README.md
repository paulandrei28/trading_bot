# Trading Backtest Project

Minimal cleaned version of the project for:
- running the backtest
- generating reports
- running Monte Carlo analysis
- testing Interactive Brokers / Trader Workstation connectivity
- fetching historical data from IBKR

## Project structure

- `strategy/fvg_strategy.py` - strategy logic and config
- `backtest/backtest.py` - backtest entry point
- `backtest/test_backtest.py` - main backtest test runner
- `backtest/metrics.py` - performance metrics
- `backtest/analysis.py` - extra analysis outputs
- `backtest/monte_carlo.py` - Monte Carlo engine
- `backtest/run_monte_carlo.py` - Monte Carlo runner
- `tws/ib_api.py` - basic TWS order API client
- `tws/ib_history.py` - IBKR historical data downloader
- `tws/test_ibkr.py` - TWS connectivity and paper-order test
- `data/SPY_1m_2025-03-04_2026-03-04_ibkr.csv` - sample dataset

## Setup

Create and activate a virtual environment.

### Windows

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Run the backtest test

From the project root:

```bash
python -m backtest.test_backtest
```

This will:
- load the latest matching `SPY` dataset from `data/`
- run the strategy backtest
- create output files inside `logs/`
- print a summary report in the console

## Run Monte Carlo on saved trades

After generating a trades file in `logs/`, run:

```bash
python -m backtest.run_monte_carlo logs/trades_SPY_2025-03-04_2026-03-04.csv 10000
```

## Using Trader Workstation (TWS)

### 1. Install and open TWS
Use the paper trading account if you want to test safely.

### 2. Enable API access in TWS
In Trader Workstation:

- `File` -> `Global Configuration`
- `API` -> `Settings`
- enable `Enable ActiveX and Socket Clients`
- keep the socket port as:
  - `7497` for Paper Trading
  - `7496` for Live Trading
- optionally add `127.0.0.1` to trusted IPs

### 3. Test TWS connection
With TWS open and logged in:

```bash
python tws/test_ibkr.py
```

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
