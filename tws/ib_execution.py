from __future__ import annotations

import threading
import logging
from typing import Optional

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order

log = logging.getLogger("ib_execution")


class IBExecution(EWrapper, EClient):
    """
    Thin execution wrapper around ibapi EClient/EWrapper.
    Handles bracket order placement and order status callbacks.

    error() uses *args to stay compatible with both the classic ibapi
    callback signature (req_id, error_code, error_string) and the newer
    protobuf-based signature (req_id, error_time, error_code, error_string,
    advanced_order_reject) without raising TypeError at runtime.
    """

    def __init__(self):
        EClient.__init__(self, self)
        self.next_order_id: Optional[int] = None
        self._ready = threading.Event()
        self._orders: dict = {}

    def connect_and_run(self, host: str, port: int, client_id: int) -> None:
        self.connect(host, port, client_id)
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        if not self._ready.wait(timeout=10):
            raise ConnectionError(
                f"Could not connect to TWS at {host}:{port} within 10 s. "
                "Make sure TWS is open and API connections are enabled."
            )
        log.info(f"Connected to IBKR ({host}:{port}  client_id={client_id})")

    def disconnect_safe(self) -> None:
        try:
            self.disconnect()
        except Exception:
            pass

    # ── EWrapper callbacks ────────────────────────────────────────────────────

    def nextValidId(self, order_id: int) -> None:
        self.next_order_id = order_id
        self._ready.set()
        log.info(f"Next valid order id: {order_id}")

    def orderStatus(
        self,
        order_id,
        status,
        filled,
        remaining,
        avg_fill,
        perm_id,
        parent_id,
        last_fill,
        client_id,
        why_held,
        mkt_cap,
    ) -> None:
        self._orders[order_id] = status
        log.info(f"Order {order_id}: {status}  filled={filled}  avgFill={avg_fill}")

    def openOrder(self, order_id, contract, order, order_state) -> None:
        px = getattr(order, "lmtPrice", None) or getattr(order, "auxPrice", None)
        log.info(
            f"OpenOrder {order_id}: {order.action} "
            f"{order.totalQuantity} {contract.symbol} @ {px}"
        )

    def execDetails(self, req_id, contract, execution) -> None:
        log.info(
            f"Execution: {contract.symbol} {execution.side} "
            f"{execution.shares} @ {execution.price}"
        )

    def error(self, *args) -> None:
        """
        Flexible error handler — accepts 3, 4, or 5 positional args.

        Classic ibapi:
            error(req_id, error_code, error_string)
            error(req_id, error_code, error_string, advanced_order_reject)
        Protobuf ibapi (v10.19+):
            error(req_id, error_time, error_code, error_string, advanced_order_reject)
        """
        req_id = -1
        error_code = -1
        error_string = ""

        if len(args) == 3:
            req_id, error_code, error_string = args
        elif len(args) == 4:
            req_id, error_code, error_string, _ = args
        elif len(args) >= 5:
            # protobuf path: second arg is error_time (int milliseconds)
            req_id, _error_time, error_code, error_string = (
                args[0],
                args[1],
                args[2],
                args[3],
            )

        # Suppress known informational messages
        if error_code in (2104, 2106, 2107, 2108, 2119, 2158, 2174):
            log.debug(f"IB info  req={req_id}  code={error_code}  {error_string}")
        else:
            log.error(f"IBKR error  req={req_id}  code={error_code}  {error_string}")

    # ── Order helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _make_contract(symbol: str, exchange: str, currency: str) -> Contract:
        c = Contract()
        c.symbol = symbol
        c.secType = "STK"
        c.exchange = exchange
        c.currency = currency
        return c

    def place_bracket(
        self,
        symbol: str,
        direction: str,
        entry: float,
        stop: float,
        target: float,
        quantity: int,
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> tuple[int, int, int]:
        """
        Submit a bracket order (entry LMT + stop STP + target LMT).
        Returns (entry_id, stop_id, target_id).
        """
        if self.next_order_id is None:
            raise RuntimeError("Not connected — call connect_and_run() first.")

        contract = self._make_contract(symbol, exchange, currency)
        entry_id = self.next_order_id
        stop_id = entry_id + 1
        target_id = entry_id + 2
        self.next_order_id += 3

        side = "BUY" if direction == "LONG" else "SELL"
        exit_side = "SELL" if direction == "LONG" else "BUY"

        entry_order = Order()
        entry_order.orderId = entry_id
        entry_order.action = side
        entry_order.orderType = "LMT"
        entry_order.lmtPrice = round(entry, 2)
        entry_order.totalQuantity = quantity
        entry_order.transmit = False
        entry_order.tif = "DAY"

        stop_order = Order()
        stop_order.orderId = stop_id
        stop_order.action = exit_side
        stop_order.orderType = "STP"
        stop_order.auxPrice = round(stop, 2)
        stop_order.totalQuantity = quantity
        stop_order.parentId = entry_id
        stop_order.transmit = False
        stop_order.tif = "DAY"

        tp_order = Order()
        tp_order.orderId = target_id
        tp_order.action = exit_side
        tp_order.orderType = "LMT"
        tp_order.lmtPrice = round(target, 2)
        tp_order.totalQuantity = quantity
        tp_order.parentId = entry_id
        tp_order.transmit = True
        tp_order.tif = "DAY"

        self.placeOrder(entry_id, contract, entry_order)
        self.placeOrder(stop_id, contract, stop_order)
        self.placeOrder(target_id, contract, tp_order)

        log.info(
            f"Bracket submitted: {direction} {quantity}x {symbol}  "
            f"entry={entry}  stop={stop}  target={target}  "
            f"ids=({entry_id},{stop_id},{target_id})"
        )
        return entry_id, stop_id, target_id

    def cancel_order(self, order_id: int) -> None:
        self.cancelOrder(order_id, "")
        log.info(f"Cancel requested for order {order_id}")
