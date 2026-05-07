from __future__ import annotations

from typing import Optional

from ib_insync import IB, Stock, Order, Trade, LimitOrder, StopOrder, util

import logging

log = logging.getLogger("live_trader")


class IBKRExecutionClient:
    """
    Handles live/paper bracket order placement and monitoring via ib_insync.

    Usage:
        client = IBKRExecutionClient(port=7497, client_id=20)
        client.connect()
        parent_id = client.place_bracket_order(
            symbol="QQQ", action="BUY", quantity=10,
            entry_price=480.50, stop_price=479.66, target_price=482.90
        )
        status = client.get_order_status(parent_id)
        client.disconnect()
    """

    def __init__(self, port: int = 7497, client_id: int = 20) -> None:
        self.port = port
        self.client_id = client_id
        self.ib = IB()
        self._trades: dict[int, Trade] = {}  # parent_id -> Trade

    # --------------------------------------------------------------
    # Connection
    # --------------------------------------------------------------

    def connect(self) -> None:
        if not self.ib.isConnected():
            self.ib.connect("127.0.0.1", self.port, clientId=self.client_id)
            log.debug(f"[EXEC] Connected on port {self.port} clientId={self.client_id}")

    def disconnect(self) -> None:
        if self.ib.isConnected():
            self.ib.disconnect()
            log.debug("[EXEC] Disconnected.")

    # --------------------------------------------------------------
    # Bracket order
    # --------------------------------------------------------------

    def place_bracket_order(
        self,
        symbol: str,
        action: str,  # "BUY" or "SELL"
        quantity: int,
        entry_price: float,
        stop_price: float,
        target_price: float,
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> int:
        """
        Submit a bracket: LMT entry + STP loss + LMT profit target.

        Returns the parent order ID (int).
        Raises RuntimeError on placement failure.
        """
        self.connect()

        contract = Stock(symbol, exchange, currency)
        self.ib.qualifyContracts(contract)

        reverse = "SELL" if action == "BUY" else "BUY"

        # -- Parent: limit entry -----------------------------------
        parent = LimitOrder(action, quantity, round(entry_price, 2))
        parent.orderId = self.ib.client.getReqId()
        parent.transmit = False  # hold until children attached

        # -- Child 1: stop loss -----------------------------------
        stop = StopOrder(reverse, quantity, round(stop_price, 2))
        stop.orderId = self.ib.client.getReqId()
        stop.parentId = parent.orderId
        stop.transmit = False

        # -- Child 2: profit target --------------------------------
        target = LimitOrder(reverse, quantity, round(target_price, 2))
        target.orderId = self.ib.client.getReqId()
        target.parentId = parent.orderId
        target.transmit = True  # transmit=True on last child fires all three

        parent_trade = self.ib.placeOrder(contract, parent)
        stop_trade = self.ib.placeOrder(contract, stop)
        target_trade = self.ib.placeOrder(contract, target)

        self.ib.sleep(1)  # give TWS a moment to acknowledge

        if parent_trade is None:
            raise RuntimeError("[EXEC] Parent order was not acknowledged by TWS.")

        self._trades[parent.orderId] = parent_trade
        log.info(
            f"[EXEC] Bracket placed | parentId={parent.orderId} "
            f"{action} {quantity}x{symbol} "
            f"entry={entry_price:.2f}  stop={stop_price:.2f}  target={target_price:.2f}"
        )
        return parent.orderId

    # --------------------------------------------------------------
    # Order status & fill price
    # --------------------------------------------------------------

    def get_order_status(self, parent_id: int) -> str:
        """
        Return the current status string of the parent order.
        Possible values: "Submitted", "PreSubmitted", "Filled",
                         "Cancelled", "Inactive", "Unknown"
        """
        self.connect()
        trades = self.ib.trades()
        for trade in trades:
            if trade.order.orderId == parent_id:
                return trade.orderStatus.status
        # Also check child orders -- if a child (stop or target) filled,
        # the bracket is done.
        for trade in trades:
            if trade.order.parentId == parent_id:
                if trade.orderStatus.status == "Filled":
                    return "Filled"
        return "Unknown"

    def get_fill_price(self, parent_id: int) -> Optional[float]:
        """
        Return the average fill price of whichever child order filled
        (stop or target), or None if neither has filled yet.
        """
        self.connect()
        for trade in self.ib.trades():
            if (
                trade.order.parentId == parent_id
                and trade.orderStatus.status == "Filled"
                and trade.orderStatus.avgFillPrice
            ):
                return float(trade.orderStatus.avgFillPrice)
        return None

    # --------------------------------------------------------------
    # Hard close
    # --------------------------------------------------------------

    def cancel_all_children(self, parent_id: int) -> None:
        """
        Cancel all open child orders (stop + target) and close any
        remaining position with a market order.
        """
        self.connect()
        cancelled = 0
        filled_action = None
        qty = 0

        for trade in self.ib.trades():
            order = trade.order
            status = trade.orderStatus.status

            # Cancel open children
            if order.parentId == parent_id and status not in (
                "Filled",
                "Cancelled",
                "Inactive",
            ):
                self.ib.cancelOrder(order)
                cancelled += 1
                log.info(f"[EXEC] Cancelled child orderId={order.orderId}")

            # Remember entry fill direction so we know how to flatten
            if order.orderId == parent_id and status == "Filled":
                filled_action = order.action
                qty = int(order.filledQuantity)

        self.ib.sleep(1)

        # Flatten residual position with a MKT order
        if filled_action and qty > 0:
            close_action = "SELL" if filled_action == "BUY" else "BUY"
            # Reconstruct contract -- pull from any trade with this parent
            contract = None
            for trade in self.ib.trades():
                if (
                    trade.order.orderId == parent_id
                    or trade.order.parentId == parent_id
                ):
                    contract = trade.contract
                    break

            if contract:
                mkt = Order()
                mkt.action = close_action
                mkt.orderType = "MKT"
                mkt.totalQuantity = qty
                mkt.transmit = True
                self.ib.placeOrder(contract, mkt)
                log.info(
                    f"[EXEC] Market close sent: {close_action} {qty} to flatten position."
                )

        log.info(f"[EXEC] Hard close complete -- {cancelled} child(ren) cancelled.")
