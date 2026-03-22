from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
import time

class IBKR(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.nextOrderId = None

    def nextValidId(self, orderId: int):
        self.nextOrderId = orderId
        print(f"Next valid order id: {orderId}")

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice,
                    permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        print(f"Order {orderId} status: {status}, filled: {filled}, avgFillPrice: {avgFillPrice}")

    def openOrder(self, orderId, contract, order, orderState):
        print(f"OpenOrder. Id: {orderId}, {order.action} {order.totalQuantity} {contract.symbol} at {order.lmtPrice}")

    def connect_and_run(self, host='127.0.0.1', port=7497, clientId=0):
        self.connect(host, port, clientId)
        thread = threading.Thread(target=self.run)
        thread.start()
        while self.nextOrderId is None:
            time.sleep(0.1)
        print("Connected to TWS")

    def place_order(self, symbol, action, quantity, order_type='MKT', lmtPrice=None):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        order = Order()
        order.action = action
        order.orderType = order_type
        order.totalQuantity = quantity
        if order_type == 'LMT' and lmtPrice:
            order.lmtPrice = lmtPrice

        self.placeOrder(self.nextOrderId, contract, order)
        self.nextOrderId += 1