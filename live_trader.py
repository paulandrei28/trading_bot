"""
live_trader.py
--------------
FVG Live Trader — integrates with IBKR TWS via ibapi + ib_insync.
Supports paper and live trading, late-start detection, OR/ATR filters,
detailed phase logging, and daily journal CSV.

Usage:
    python live_trader.py              # paper mode (default)
    python live_trader.py --live       # live mode (port 7496)
    python live_trader.py --symbol SPY --risk 200
"""

import csv
import datetime
import logging
import os
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from ib_insync import IB, Order, Stock

from backtest.analysis import add_daily_volatility_features
from strategy.fvg_strategy import FVGConfig, generate_trades
from tws.ib_history import IBHistoryConfig, IBKRHistoryClient

NY = ZoneInfo("America/New_York")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────


def _setup_logging(log_path: str) -> None:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    fmt = "%(asctime)s %(levelname)-5s %(message)s"
    datefmt = "%Y-%m-%d %H%M%S"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


# ─────────────────────────────────────────────────────────────────────────────


class FVGLiveTrader:
    def __init__(
        self,
        symbol: str,
        cfg: FVGConfig,
        risk_per_trade: float = 100.0,
        paper: bool = True,
        host: str = "127.0.0.1",
    ) -> None:
        self.symbol = symbol
        self.cfg = cfg
        self.risk_per_trade = risk_per_trade
        self.paper = paper
        self.host = host
        self.port = 7497 if paper else 7496
        self._mode = "PAPER" if paper else "LIVE"  # BUG-3 fix

        self.trade_open: bool = False
        self.trade_taken: bool = False

        self.entry_order_id: int | None = None
        self.stop_order_id: int | None = None
        self.target_order_id: int | None = None

        self.direction: str | None = None
        self.entry_price: float | None = None
        self.stop_price: float | None = None
        self.target_price: float | None = None
        self.risk: float | None = None
        self.qty: int | None = None

        self.result_r: float | None = None
        self.outcome: str | None = None
        self.entry_time: datetime.datetime | None = None
        self.exit_time: datetime.datetime | None = None
        self.exit_price: float | None = None

        # ── Filter thresholds ──────────────────────────────────────────────
        self.or_skip_lo: float | None = None
        self.or_skip_hi: float | None = None
        self.atr_min: float | None = None
        self.today_or: float | None = None
        self.today_atr: float | None = None
        self.filter_pass: bool = False

    # ── Helpers ───────────────────────────────────────────────────────────

    def _now_ny(self) -> datetime.datetime:
        return datetime.datetime.now(tz=NY)

    def _today(self) -> datetime.date:
        return self._now_ny().date()

    def _connect(self, client_id: int) -> IB:
        ib = IB()
        ib.connect(self.host, self.port, clientId=client_id)
        log.info(f"Connecting to {self.host}:{self.port} with clientId {client_id}...")
        log.info("Connected")
        return ib

    def _journal_path(self) -> Path:
        return Path("logs") / f"journal_{self.symbol}.csv"

    # ── Warm-up ───────────────────────────────────────────────────────────

    def _warmup(self) -> None:
        log.info("WARM-UP Fetching 30 days of 1m bars for OR/ATR filters...")
        hist_cfg = IBHistoryConfig(host=self.host, port=self.port, client_id=21)
        client = IBKRHistoryClient(hist_cfg)
        end = self._now_ny()
        start = end - datetime.timedelta(days=30)
        bars = client.fetch_1m_bars(self.symbol, start, end)
        client.disconnect()

        if bars.empty:
            log.warning("WARM-UP No bars loaded — filters disabled")
            return

        log.info(
            f"WARM-UP {len(bars)} bars loaded "
            f"{bars['timestamp'].min().date()} to {bars['timestamp'].max().date()}"
        )
        log.info("FILTER Computing daily OR and ATR filters from warm-up data...")

        daily = add_daily_volatility_features(bars)
        daily = daily.dropna(subset=["opening_range_5m", "atr_14"])
        if daily.empty:
            log.warning("FILTER Not enough data to compute filters — disabled")
            return

        or_vals = daily["opening_range_5m"].dropna()
        self.or_skip_lo = float(or_vals.quantile(0.20))
        self.or_skip_hi = float(or_vals.quantile(0.40))
        self.atr_min = float(daily["atr_14"].median())

        log.info(f"FILTER OR skip band {self.or_skip_lo:.3f}, {self.or_skip_hi:.3f}")
        log.info(f"FILTER ATR min threshold {self.atr_min:.3f}")

        or_skip_n = int(
            ((or_vals >= self.or_skip_lo) & (or_vals <= self.or_skip_hi)).sum()
        )
        atr_skip_n = int((daily["atr_14"] < self.atr_min).sum())
        log.info(f"FILTER OR would skip {or_skip_n} historical days")
        log.info(f"FILTER ATR would skip {atr_skip_n} historical days")

    # ── Today filters ─────────────────────────────────────────────────────

    def _fetch_today_filters(self) -> None:
        log.info("OPENING RANGE Window closed. Fetching today's OR and ATR...")
        hist_cfg = IBHistoryConfig(host=self.host, port=self.port, client_id=22)
        client = IBKRHistoryClient(hist_cfg)
        today_start = datetime.datetime.combine(
            self._today(), datetime.time(9, 30), tzinfo=NY
        )
        bars = client.fetch_1m_bars(self.symbol, today_start, self._now_ny())
        client.disconnect()

        if bars.empty:
            log.warning(
                "FILTER Today OR=NA ATR=NA (no bars fetched) — ATR filter inactive"
            )
            self.filter_pass = True
            return

        daily = add_daily_volatility_features(bars)
        today_row = daily[daily["date"] == self._today()]

        if today_row.empty:
            log.warning("FILTER Today OR=NA ATR=NA — ATR filter inactive")
            self.filter_pass = True
            return

        r = today_row.iloc[0]
        self.today_or = (
            float(r["opening_range_5m"]) if pd.notna(r["opening_range_5m"]) else None
        )
        self.today_atr = float(r["atr_14"]) if pd.notna(r["atr_14"]) else None

        or_str = f"{self.today_or:.3f}" if self.today_or is not None else "NA"
        atr_str = f"{self.today_atr:.3f}" if self.today_atr is not None else "NA"
        log.info(f"FILTER Today OR={or_str} ATR={atr_str}")

        if self.today_atr is None:
            log.warning("FILTER ATR=NA — ATR filter inactive today")

        or_pass = True
        atr_pass = True

        if self.or_skip_lo is not None and self.today_or is not None:
            if self.or_skip_lo <= self.today_or <= self.or_skip_hi:
                or_pass = False
                log.info(
                    f"FILTER SKIP OR={or_str} in skip band "
                    f"[{self.or_skip_lo:.3f}, {self.or_skip_hi:.3f}]"
                )

        if self.atr_min is not None and self.today_atr is not None:
            if self.today_atr < self.atr_min:
                atr_pass = False
                log.info(
                    f"FILTER SKIP ATR={atr_str} below threshold {self.atr_min:.3f}"
                )

        self.filter_pass = or_pass and atr_pass
        if self.filter_pass:
            log.info(f"FILTER PASS OR={or_str} ATR={atr_str}")
        else:
            log.info("FILTER SKIP day — no trades today")

    # ── Bracket placement ─────────────────────────────────────────────────

    def _place_bracket(
        self,
        ib: IB,
        direction: str,
        entry: float,
        stop: float,
        target: float,
        qty: int,
    ) -> tuple[int, int, int]:
        contract = Stock(self.symbol, "SMART", "USD")
        ib.qualifyContracts(contract)

        action = "BUY" if direction == "LONG" else "SELL"
        close_action = "SELL" if direction == "LONG" else "BUY"

        parent_id = ib.client.getReqId()

        entry_o = Order()
        entry_o.orderId = parent_id
        entry_o.action = action
        entry_o.orderType = "LMT"
        entry_o.lmtPrice = round(entry, 2)
        entry_o.totalQuantity = qty
        entry_o.transmit = False
        entry_o.tif = "DAY"

        stop_o = Order()
        stop_o.orderId = parent_id + 1
        stop_o.action = close_action
        stop_o.orderType = "STP"
        stop_o.auxPrice = round(stop, 2)
        stop_o.totalQuantity = qty
        stop_o.parentId = parent_id
        stop_o.transmit = False
        stop_o.tif = "DAY"

        target_o = Order()
        target_o.orderId = parent_id + 2
        target_o.action = close_action
        target_o.orderType = "LMT"
        target_o.lmtPrice = round(target, 2)
        target_o.totalQuantity = qty
        target_o.parentId = parent_id
        target_o.transmit = True
        target_o.tif = "DAY"

        ib.placeOrder(contract, entry_o)
        ib.placeOrder(contract, stop_o)
        ib.placeOrder(contract, target_o)
        ib.sleep(1)

        log.info(
            f"BRACKET PLACED {direction} {qty}x {self.symbol} "
            f"entry={entry:.2f} stop={stop:.2f} target={target:.2f} "
            f"orderIds={parent_id}/{parent_id+1}/{parent_id+2}"
        )
        return parent_id, parent_id + 1, parent_id + 2

    # ── Reconcile ─────────────────────────────────────────────────────────

    def _reconcile(self, ib: IB) -> None:
        """
        Check executions to update trade_taken and trade_open flags.

        BUG-1 fix: trade_taken is set HERE (when entry fill is confirmed in
        executions), not when the signal fires.  trade_open is cleared when
        the stop or target fill is confirmed.
        """
        if self.entry_order_id is None:
            return

        for ex in ib.executions():
            oid = ex.execution.orderId
            price = ex.execution.price

            # Entry fill confirmed
            if oid == self.entry_order_id and not self.trade_taken:
                self.trade_taken = True
                log.info(
                    f"ENTRY FILLED orderId={oid} "
                    f"{self.direction} {self.qty}x @ {price:.2f} [trade_taken=True]"
                )

            # Exit fill (stop or target)
            if oid in (self.stop_order_id, self.target_order_id) and self.trade_open:
                self.trade_open = False
                self.exit_price = price
                self.exit_time = self._now_ny()

                if oid == self.stop_order_id:
                    self.result_r = -1.0
                    self.outcome = "LOSS"
                else:
                    self.result_r = float(self.cfg.rr)
                    self.outcome = "WIN"

                log.info(
                    f"RESULT {self.outcome} result_r={self.result_r:.2f}R "
                    f"fill={self.exit_price:.2f} orderId={oid}"
                )

    # ── Hard close ────────────────────────────────────────────────────────

    def _hard_close(self, ib: IB) -> None:
        if not self.trade_open:
            return
        log.info("HARD CLOSE Cancelling bracket and flattening position...")

        for oid in (self.entry_order_id, self.stop_order_id, self.target_order_id):
            if oid is not None:
                try:
                    ib.cancelOrder(Order(orderId=oid))
                except Exception:
                    pass

        contract = Stock(self.symbol, "SMART", "USD")
        ib.qualifyContracts(contract)
        close_action = "SELL" if self.direction == "LONG" else "BUY"
        mkt = Order()
        mkt.action = close_action
        mkt.orderType = "MKT"
        mkt.totalQuantity = self.qty
        mkt.tif = "DAY"
        ib.placeOrder(contract, mkt)
        ib.sleep(2)

        self.trade_open = False
        if self.result_r is None:
            self.result_r = 0.0
            self.outcome = "BE"
        log.info(f"HARD CLOSE Position flattened -> {self.outcome}")

    # ── Journal ───────────────────────────────────────────────────────────

    def _write_journal(self) -> None:
        """
        BUG-2 fix: called unconditionally at SESSION END.
        Previously this was only called inside the real-time fill callback,
        so trades that closed during a poll-cycle disconnect were never logged.
        """
        if not self.trade_taken:
            log.info("JOURNAL No trade today — skipping")
            return

        path = self._journal_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not path.exists()

        row = {
            "date": self._today().isoformat(),
            "symbol": self.symbol,
            "mode": self._mode,
            "direction": self.direction or "",
            "entry_time": self.entry_time.isoformat() if self.entry_time else "",
            "exit_time": self.exit_time.isoformat() if self.exit_time else "",
            "entry_price": self.entry_price or "",
            "stop_price": self.stop_price or "",
            "target_price": self.target_price or "",
            "exit_price": self.exit_price or "",
            "risk": self.risk or "",
            "qty": self.qty or "",
            "risk_usd": (
                round(self.risk * self.qty, 2) if (self.risk and self.qty) else ""
            ),
            "result_r": self.result_r if self.result_r is not None else "",
            "outcome": self.outcome or "",
            "today_or": self.today_or or "",
            "today_atr": self.today_atr or "",
        }

        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(row.keys()))
            if write_header:
                writer.writeheader()
            writer.writerow(row)

        log.info(f"JOURNAL Written -> {path}")

    # ── Scan tick ─────────────────────────────────────────────────────────

    def _scan_tick(self, ib: IB, bars: pd.DataFrame) -> None:
        """
        Called every ~30 s while in the scan window.

        BUG-1 fix: the very first check is `if self.trade_taken: reconcile only`.
        Once an entry fills, trade_taken=True for the rest of the session and
        no new signal is ever evaluated, regardless of whether trade_open is
        still True or has already become False.
        """
        if self.trade_taken:
            self._reconcile(ib)
            return

        if not self.filter_pass:
            return

        trade_start_t = datetime.time(*map(int, self.cfg.trade_start.split(":")))
        trade_end_t = datetime.time(*map(int, self.cfg.trade_end.split(":")))
        now_t = self._now_ny().time()

        if now_t < trade_start_t or now_t > trade_end_t:
            return

        signal_trades = generate_trades(bars, self.cfg)
        if not signal_trades:
            return

        sig = signal_trades[-1]  # generate_trades returns at most 1 per day
        direction = sig["direction"]
        entry = sig["entry"]
        stop = sig["stop"]
        target = sig["target"]
        risk = sig.get("risk", abs(entry - stop))

        if risk <= 0:
            return

        qty = max(1, int(self.risk_per_trade / risk))
        log.info(
            f"SIGNAL {direction} entry={entry:.2f} stop={stop:.2f} "
            f"target={target:.2f} risk={risk:.4f} qty={qty}"
        )

        entry_id, stop_id, target_id = self._place_bracket(
            ib, direction, entry, stop, target, qty
        )
        self.trade_open = True
        self.entry_order_id = entry_id
        self.stop_order_id = stop_id
        self.target_order_id = target_id
        self.direction = direction
        self.entry_price = entry
        self.stop_price = stop
        self.target_price = target
        self.risk = risk
        self.qty = qty
        self.entry_time = self._now_ny()

    # ── Main run loop ─────────────────────────────────────────────────────

    def run(self) -> None:
        today = self._today()
        mode_label = "PAPER MODE" if self.paper else "LIVE MODE"
        log_path = (
            f"logs/live_{self.symbol}_{today}_{'paper' if self.paper else 'live'}.log"
        )
        _setup_logging(log_path)

        log.info(f"LOGGING Log file {log_path}")
        log.info("")
        log.info(f"FVG Live Trader -- {mode_label}")  # BUG-3 fix: mode from self._mode
        log.info(f"Symbol {self.symbol}")
        log.info(f"Port {self.port} (paper=7497 live=7496)")
        log.info(f"Risk/trade {self.risk_per_trade}")
        log.info(f"Session {self.cfg.session_start} - {self.cfg.trade_end} NY entries")
        log.info("Monitoring until 16:00 NY open trade hard close")
        log.info(f"Trade win {self.cfg.trade_start} - {self.cfg.trade_end} NY")
        log.info(f"Script started at {self._now_ny().strftime('%H%M')} NY")
        log.info("")

        # ── Pre-market wait ────────────────────────────────────────────────
        market_open_t = datetime.time(9, 30)
        while self._now_ny().time() < market_open_t:
            rem = (
                datetime.datetime.combine(today, market_open_t, tzinfo=NY)
                - self._now_ny()
            )
            s = int(rem.total_seconds())
            log.info(
                f"PRE-MARKET Waiting for market open 0930 NY. "
                f"{s // 60}m {s % 60:02d}s remaining..."
            )
            time.sleep(30)

        # ── Warm-up ───────────────────────────────────────────────────────
        log.info("MARKET OPEN Market is open. Starting warm-up.")
        self._warmup()

        # ── Wait for opening range ─────────────────────────────────────────
        opening_end_t = datetime.time(9, 35)
        while self._now_ny().time() < opening_end_t:
            rem = (
                datetime.datetime.combine(today, opening_end_t, tzinfo=NY)
                - self._now_ny()
            )
            s = int(rem.total_seconds())
            log.info(
                f"OPENING RANGE Waiting for 0935 NY. "
                f"{s // 60}m {s % 60:02d}s remaining..."
            )
            time.sleep(15)

        # ── Fetch today filters ────────────────────────────────────────────
        self._fetch_today_filters()

        if not self.filter_pass:
            log.info("SESSION END Filter skip — no trades today")
            self._write_journal()  # BUG-2 fix
            return

        # ── Scan / monitor loop ────────────────────────────────────────────
        scan_end_t = datetime.time(15, 0)
        hard_close_t = datetime.time(16, 0)
        cid = 24

        log.info(
            f"SCANNING Starting incremental scan loop "
            f"{self.cfg.trade_start} {self.cfg.trade_end} NY..."
        )
        log.info("SCANNING First fetch full day bars from market open...")

        while self._now_ny().time() < hard_close_t:
            ib = None
            try:
                ib = self._connect(cid)
                cid += 1
                now = self._now_ny()

                # Fetch today's bars via a second connection to avoid clientId clash
                today_start = datetime.datetime.combine(
                    today, datetime.time(9, 30), tzinfo=NY
                )
                hist_cfg = IBHistoryConfig(
                    host=self.host, port=self.port, client_id=cid
                )
                cid += 1
                hist = IBKRHistoryClient(hist_cfg)
                bars = hist.fetch_1m_bars(self.symbol, today_start, now)
                hist.disconnect()

                if not bars.empty:
                    log.info(f"SCANNING Loaded {len(bars)} bars for today.")

                    # Always reconcile first if a bracket is open
                    if self.trade_open or self.entry_order_id:
                        self._reconcile(ib)

                    # Hard-close if past 16:00
                    if now.time() >= hard_close_t and self.trade_open:
                        self._hard_close(ib)
                        break

                    # Scan only inside window and only if no trade taken today
                    if not self.trade_taken and now.time() < scan_end_t:
                        self._scan_tick(ib, bars)

            except Exception as exc:
                log.error(f"SCAN ERROR {exc}")
            finally:
                if ib and ib.isConnected():
                    ib.disconnect()
                    log.info("Disconnecting")

            time.sleep(30)

        # ── Session end ────────────────────────────────────────────────────
        log.info("SESSION END Hard close time reached")
        try:
            ib = self._connect(cid)
            if self.trade_open:
                self._hard_close(ib)
            elif self.entry_order_id:
                self._reconcile(ib)
            ib.disconnect()
        except Exception as exc:
            log.error(f"SESSION END final reconcile error: {exc}")

        self._write_journal()

        or_str = f"{self.today_or:.3f}" if self.today_or is not None else "NA"
        atr_str = f"{self.today_atr:.3f}" if self.today_atr is not None else "NA"
        log.info("===== SESSION SUMMARY =====")
        log.info(f"  Mode       {self._mode}")
        log.info(f"  Symbol     {self.symbol}")
        log.info(f"  Trade      {'YES' if self.trade_taken else 'NO'}")
        if self.trade_taken:
            log.info(f"  Direction  {self.direction}")
            log.info(f"  Entry      {self.entry_price:.2f}")
            ep = f"{self.exit_price:.2f}" if self.exit_price is not None else "N/A"
            log.info(f"  Exit       {ep}")
            rr = f"{self.result_r:.2f}R" if self.result_r is not None else "N/A"
            log.info(f"  Result     {rr}  [{self.outcome}]")
        log.info(f"  Filter OR  {or_str}")
        log.info(f"  Filter ATR {atr_str}")
        log.info("===========================")


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from strategy.fvg_strategy import FVGConfig

    cfg = FVGConfig(
        tick_size=0.01,
        rr=3.0,
        session_start="09:30",
        opening_end="09:35",
        trade_start="10:00",
        trade_end="15:00",
        one_trade_per_day=True,
        retest_mode="close",
        use_profit_lock=True,
        lock1_trigger_r=1.5,
        lock1_stop_r=0.5,
        lock2_trigger_r=2.5,
        lock2_stop_r=1.0,
    )

    trader = FVGLiveTrader(
        symbol="QQQ",
        cfg=cfg,
        risk_per_trade=100.0,
        paper=True,
    )
    trader.run()
