from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import pytz
from ib_insync import IB, Stock, util

NY = pytz.timezone("America/New_York")


@dataclass
class IBHistoryConfig:
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 21
    use_rth: bool = True
    bar_size: str = "1 min"
    what_to_show: str = "TRADES"
    chunk_days: int = 1


class IBKRHistoryClient:
    """
    Historical data client (separate from trading/execution client).
    Uses ib_insync to fetch historical OHLCV bars from IBKR.
    """

    def __init__(self, cfg: IBHistoryConfig = IBHistoryConfig()):
        self.cfg = cfg
        self.ib = IB()

    def connect(self) -> None:
        if not self.ib.isConnected():
            self.ib.connect(
                self.cfg.host,
                self.cfg.port,
                clientId=self.cfg.client_id,
                timeout=20,
            )

    def disconnect(self) -> None:
        if self.ib.isConnected():
            self.ib.disconnect()

    def fetch_1m_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> pd.DataFrame:
        """
        Fetch 1-minute bars between [start, end] by walking backwards in chunk_days.
        Returns DataFrame with columns: timestamp (UTC), open, high, low, close, volume.
        endDateTime is sent as explicit UTC (yyyymmdd-HH:MM:SS) to avoid IBKR warning 2174.
        """
        if start.tzinfo is None:
            start = NY.localize(start)
        else:
            start = start.astimezone(NY)

        if end.tzinfo is None:
            end = NY.localize(end)
        else:
            end = end.astimezone(NY)

        if end <= start:
            return pd.DataFrame(
                columns=["timestamp", "open", "high", "low", "close", "volume"]
            )

        self.connect()

        contract = Stock(symbol, exchange, currency)
        self.ib.qualifyContracts(contract)

        chunks: List[pd.DataFrame] = []
        cur_end = end

        while cur_end > start:
            cur_start = max(start, cur_end - timedelta(days=self.cfg.chunk_days))
            dur_days = max((cur_end - cur_start).days, 1)
            duration_str = f"{dur_days} D"

            # Use explicit UTC format to suppress IBKR warning 2174
            cur_end_utc = cur_end.astimezone(pytz.UTC)
            end_str = cur_end_utc.strftime("%Y%m%d-%H:%M:%S")

            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime=end_str,
                durationStr=duration_str,
                barSizeSetting=self.cfg.bar_size,
                whatToShow=self.cfg.what_to_show,
                useRTH=self.cfg.use_rth,
                formatDate=1,
                keepUpToDate=False,
            )

            df = util.df(bars)
            if not df.empty:
                df = df.rename(columns={"date": "timestamp"})
                chunks.append(df)

            cur_end = cur_start

        if not chunks:
            return pd.DataFrame(
                columns=["timestamp", "open", "high", "low", "close", "volume"]
            )

        out = pd.concat(chunks, ignore_index=True)
        out = out.dropna(subset=["timestamp"])

        ts = pd.to_datetime(out["timestamp"], errors="coerce")

        if getattr(ts.dt, "tz", None) is None:
            ts = ts.dt.tz_localize(NY)
        else:
            ts = ts.dt.tz_convert(NY)

        out["timestamp"] = ts.dt.tz_convert(pytz.UTC)
        out = out[["timestamp", "open", "high", "low", "close", "volume"]]
        out = (
            out.drop_duplicates(subset=["timestamp"])
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

        return out
