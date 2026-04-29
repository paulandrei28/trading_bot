from __future__ import annotations

import logging
import time
from typing import Optional

from ib_insync import IB, Stock, LimitOrder, StopOrder, Order, Trade, util

log = logging.getLogger("live_trader")


class IBKRExecutionClient:
    """
    Bracket order placement AND monitoring using a SINGLE persistent connection.

    Critical design rule: placing and monitoring MUST use the same clientId /
    the same IB instance.  If you disconnect after placing and reconnect with
    a different clientId, self.ib.trades() returns an empty list and monitoring
    loops forever returning "Unknown".

    Usage pattern:
        client = IBKRExecutionClient(port=7497, client_id=20)
        client.connect()
        parent_id = client.place_bracket_order(...)
        outcome, result_r = client.monitor_until_done(
            parent_id, direction, entry, stop_px, target, risk, hard_close_dt
        )
        client.disconnect()
    """

    def __init__(self, port: int = 7497, client_id: int = 20) -> None:
        self.port = port
        self.client_id = client_id
        self.ib = IB()

    # ─────────────────────────────────────────────────────────────────────────
    # Connection
    # ─────────────────────────────────────────────────────────────────────────

    def connect(self) -> None:
        if not self.ib.isConnected():
            self.ib.connect("127.0.0.1", self.port, clientId=self.client_id)
            log.debug(f"[EXEC] Connected port={self.port} clientId={self.client_id}")

    def disconnect(self) -> None:
        if self.ib.isConnected():
            self.ib.disconnect()
            log.debug("[EXEC] Disconnected.")

    # ─────────────────────────────────────────────────────────────────────────
    # Place bracket
    # ─────────────────────────────────────────────────────────────────────────

    def place_bracket_order(
        self,
        symbol: str,
        action: str,
        quantity: int,
        entry_price: float,
        stop_price: float,
        target_price: float,
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> int:
        """
        Submit LMT entry + STP stop + LMT target as a linked bracket.
        Returns parent orderId. Raises RuntimeError on failure.
        All three orders stay on THIS connection so monitoring can see them.
        """
        self.connect()  # no-op if already connected

        contract = Stock(symbol, exchange, currency)
        self.ib.qualifyContracts(contract)

        reverse = "SELL" if action == "BUY" else "BUY"

        parent = LimitOrder(action, quantity, round(entry_price, 2))
        parent.orderId = self.ib.client.getReqId()
        parent.transmit = False

        stop = StopOrder(reverse, quantity, round(stop_price, 2))
        stop.orderId = self.ib.client.getReqId()
        stop.parentId = parent.orderId
        stop.transmit = False

        target = LimitOrder(reverse, quantity, round(target_price, 2))
        target.orderId = self.ib.client.getReqId()
        target.parentId = parent.orderId
        target.transmit = True  # fires all three

        self.ib.placeOrder(contract, parent)
        self.ib.placeOrder(contract, stop)
        self.ib.placeOrder(contract, target)

        self.ib.sleep(1)  # allow TWS acknowledgement

        log.info(
            f"[EXEC] Bracket placed | parentId={parent.orderId} "
            f"{action} {quantity}x{symbol} "
            f"entry={entry_price:.2f}  stop={stop_price:.2f}  target={target_price:.2f}"
        )
        return parent.orderId

    # ─────────────────────────────────────────────────────────────────────────
    # Monitor until done  (replaces separate get_order_status / get_fill_price)
    # ─────────────────────────────────────────────────────────────────────────

    def monitor_until_done(
        self,
        parent_id: int,
        direction: str,
        entry: float,
        stop_px: float,
        target: float,
        risk: float,
        hard_close_dt,  # datetime (NY tz-aware)
        poll_interval: int = 10,
    ) -> tuple[str, float]:
        """
        Poll every poll_interval seconds on the SAME connection used to place.
        Checks child order fills (stop or target) for the final result.
        Returns (outcome, result_r).
        """
        from datetime import datetime
        import pytz

        NY = pytz.timezone("America/New_York")

        log.info(
            f"[MONITORING] Watching parentId={parent_id} | "
            f"stop={stop_px:.2f}  target={target:.2f}"
        )

        while True:
            now = datetime.now(NY)

            # ── Hard close at 16:00 ──────────────────────────────────────────
            if now >= hard_close_dt:
                log.info("[MONITORING] Hard close 16:00 — cancelling open bracket.")
                self._cancel_bracket(parent_id)
                return "BE", 0.0

            # ── Check all trades visible on this connection ──────────────────
            self.ib.sleep(0)  # process pending events without blocking

            filled_child = self._find_filled_child(parent_id)

            if filled_child is not None:
                fill_price = filled_child.orderStatus.avgFillPrice
                if fill_price and risk > 0:
                    result_r = round(
                        (
                            (fill_price - entry) / risk
                            if direction == "LONG"
                            else (entry - fill_price) / risk
                        ),
                        2,
                    )
                else:
                    result_r = 0.0

                outcome = (
                    "WIN" if result_r >= 2.5 else ("LOSS" if result_r <= -0.9 else "BE")
                )
                log.info(
                    f"[RESULT] {outcome}  result_r={result_r:+.2f}R  "
                    f"fill={fill_price:.2f}  (orderId={filled_child.order.orderId})"
                )
                return outcome, result_r

            # ── Check if parent was cancelled externally ─────────────────────
            parent_status = self._get_trade_status(parent_id)
            if parent_status in ("Cancelled", "Inactive"):
                log.warning(
                    f"[MONITORING] Parent order {parent_id} was cancelled externally."
                )
                return "BE", 0.0

            time.sleep(poll_interval)

    # ─────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _find_filled_child(self, parent_id: int) -> Optional[Trade]:
        """Return the first child trade that has Filled status, or None."""
        for trade in self.ib.trades():
            if (
                trade.order.parentId == parent_id
                and trade.orderStatus.status == "Filled"
            ):
                return trade
        return None

    def _get_trade_status(self, order_id: int) -> str:
        for trade in self.ib.trades():
            if trade.order.orderId == order_id:
                return trade.orderStatus.status
        return "Unknown"

    def _cancel_bracket(self, parent_id: int) -> None:
        """Cancel all open children and market-close the position if filled."""
        cancelled = 0
        filled_parent: Optional[Trade] = None

        for trade in self.ib.trades():
            order = trade.order
            status = trade.orderStatus.status

            if order.parentId == parent_id and status not in (
                "Filled",
                "Cancelled",
                "Inactive",
            ):
                self.ib.cancelOrder(order)
                cancelled += 1
                log.info(f"[EXEC] Cancelled child orderId={order.orderId}")

            if order.orderId == parent_id and status == "Filled":
                filled_parent = trade

        self.ib.sleep(1)

        if filled_parent:
            close_action = "SELL" if filled_parent.order.action == "BUY" else "BUY"
            qty = int(filled_parent.orderStatus.filled)
            if qty > 0:
                mkt = Order()
                mkt.action = close_action
                mkt.orderType = "MKT"
                mkt.totalQuantity = qty
                mkt.transmit = True
                self.ib.placeOrder(filled_parent.contract, mkt)
                log.info(
                    f"[EXEC] Market close: {close_action} {qty} to flatten position."
                )

        log.info(f"[EXEC] Hard close done — {cancelled} child order(s) cancelled.")
