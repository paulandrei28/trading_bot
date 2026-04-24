from __future__ import annotations

import logging
import threading
import time
from typing import Optional

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.wrapper import EWrapper

log = logging.getLogger("ib_api")


class IBKR(EWrapper, EClient):
    """
    Lightweight IBKR client used for simple order placement (non-bracket).
    For bracket orders and live-trader use, prefer IBExecution in ib_execution.py.

    error() uses *args to stay compatible with both classic and protobuf-based
    ibapi callback signatures, suppressing informational codes silently.
    """

    def __init__(self):
        EClient.__init__(self, self)
        self.nextOrderId: Optional[int] = None

    def nextValidId(self, orderId: int) -> None:
        self.nextOrderId = orderId
        log.info(f"Next valid order id: {orderId}")

    def orderStatus(
        self,
        orderId,
        status,
        filled,
        remaining,
        avgFillPrice,
        permId,
        parentId,
        lastFillPrice,
        clientId,
        whyHeld,
        mktCapPrice,
    ) -> None:
        log.info(
            f"Order {orderId} status: {status}  filled: {filled}  avgFillPrice: {avgFillPrice}"
        )

    def openOrder(self, orderId, contract, order, orderState) -> None:
        log.info(
            f"OpenOrder id={orderId}: {order.action} "
            f"{order.totalQuantity} {contract.symbol} @ {order.lmtPrice}"
        )

    def error(self, *args) -> None:
        req_id = -1
        error_code = -1
        error_string = ""

        if len(args) == 3:
            req_id, error_code, error_string = args
        elif len(args) == 4:
            req_id, error_code, error_string, _ = args
        elif len(args) >= 5:
            req_id, _error_time, error_code, error_string = (
                args[0],
                args[1],
                args[2],
                args[3],
            )

        if error_code in (2104, 2106, 2107, 2108, 2119, 2158, 2174):
            log.debug(f"IB info  req={req_id}  code={error_code}  {error_string}")
        else:
            log.error(f"IBKR error  req={req_id}  code={error_code}  {error_string}")

    def connect_and_run(
        self, host: str = "127.0.0.1", port: int = 7497, clientId: int = 0
    ) -> None:
        self.connect(host, port, clientId)
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        deadline = time.time() + 10
        while self.nextOrderId is None:
            if time.time() > deadline:
                raise ConnectionError(
                    f"Could not connect to TWS at {host}:{port} within 10 s."
                )
            time.sleep(0.1)
        log.info(f"Connected to TWS ({host}:{port}  clientId={clientId})")

    def place_order(
        self,
        symbol: str,
        action: str,
        quantity: int,
        order_type: str = "MKT",
        lmtPrice: Optional[float] = None,
    ) -> None:
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        order = Order()
        order.action = action
        order.orderType = order_type
        order.totalQuantity = quantity
        if order_type == "LMT" and lmtPrice is not None:
            order.lmtPrice = lmtPrice

        self.placeOrder(self.nextOrderId, contract, order)
        self.nextOrderId += 1
