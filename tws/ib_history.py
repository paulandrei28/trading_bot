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
    request_timeout: int = 30  # seconds before reqHistoricalData gives up


class IBKRHistoryClient:
    """Fetch 1-min OHLCV bars from IBKR via ib_insync.

    Key improvements:
    - timeout=30 on every reqHistoricalData -> never hangs forever
    - Returns empty DataFrame (never None) on any error or no-data response
    """

    def __init__(self, cfg: IBHistoryConfig = IBHistoryConfig()) -> None:
        self.cfg = cfg
        self.ib = IB()

    def connect(self) -> None:
        if not self.ib.isConnected():
            self.ib.connect(self.cfg.host, self.cfg.port, clientId=self.cfg.client_id)

    def disconnect(self) -> None:
        if self.ib.isConnected():
            self.ib.disconnect()

    def _empty(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )

    def fetch_1m_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> pd.DataFrame:
        """Fetch 1-minute bars between start..end.

        - Walks backwards in chunk_days increments (safest for IBKR pacing).
        - timeout=self.cfg.request_timeout prevents hanging on throttled requests.
        - Returns empty DataFrame on weekend / holiday / Error 162 / timeout.
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
            return self._empty()

        self.connect()
        contract = Stock(symbol, exchange, currency)
        self.ib.qualifyContracts(contract)

        chunks: List[pd.DataFrame] = []
        cur_end = end

        while cur_end > start:
            cur_start = max(start, cur_end - timedelta(days=self.cfg.chunk_days))
            dur_days = max((cur_end - cur_start).days, 1)
            duration_str = f"{dur_days} D"
            end_str = cur_end.strftime("%Y%m%d %H:%M:%S") + " UTC"

            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime=end_str,
                durationStr=duration_str,
                barSizeSetting=self.cfg.bar_size,
                whatToShow=self.cfg.what_to_show,
                useRTH=self.cfg.use_rth,
                formatDate=1,
                keepUpToDate=False,
                timeout=self.cfg.request_timeout,  # <-- prevents hanging forever
            )

            # util.df() returns None on Error 162, pacing, timeout, holiday
            df = util.df(bars)
            if df is None or df.empty:
                cur_end = cur_start
                continue

            df = df.rename(columns={"date": "timestamp"})
            chunks.append(df[["timestamp", "open", "high", "low", "close", "volume"]])
            cur_end = cur_start

        if not chunks:
            return self._empty()

        out = pd.concat(chunks, ignore_index=True)

        ts = pd.to_datetime(out["timestamp"], errors="coerce")
        out = out.dropna(subset=["timestamp"])
        ts = pd.to_datetime(out["timestamp"], errors="coerce")
        if getattr(ts.dt, "tz", None) is None:
            ts = ts.dt.tz_localize(NY)
        else:
            ts = ts.dt.tz_convert(NY)
        out["timestamp"] = ts.dt.tz_convert(pytz.UTC)

        return (
            out[["timestamp", "open", "high", "low", "close", "volume"]]
            .drop_duplicates(subset="timestamp")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
