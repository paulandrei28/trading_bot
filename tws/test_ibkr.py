from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
import time

class VerboseOrderTestApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        print("[INIT] App initialized")

    # -----------------------------
    # Connection callbacks
    # -----------------------------
    def nextValidId(self, orderId):
        print(f"[NEXT VALID ID] {orderId}")
        self.nextOrderId = orderId
        self.start_test_orders()

    def connectionClosed(self):
        print("[INFO] Connection closed by IBKR server")

    # -----------------------------
    # Account callbacks
    # -----------------------------
    def accountSummary(self, reqId, account, tag, value, currency):
        print(f"[ACCOUNT] ReqId:{reqId}, Account:{account}, Tag:{tag}, Value:{value}, Currency:{currency}")

    def accountSummaryEnd(self, reqId):
        print(f"[ACCOUNT] Account summary end. ReqId:{reqId}")

    # -----------------------------
    # Market data callbacks
    # -----------------------------
    def tickPrice(self, reqId, tickType, price, attrib):
        print(f"[TICK PRICE] ReqId:{reqId}, TickType:{tickType}, Price:{price}")

    def tickSize(self, reqId, tickType, size):
        print(f"[TICK SIZE] ReqId:{reqId}, TickType:{tickType}, Size:{size}")

    # -----------------------------
    # Order callbacks
    # -----------------------------
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice,
                    permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        print(f"[ORDER STATUS] OrderId:{orderId}, Status:{status}, Filled:{filled}, AvgFill:{avgFillPrice}")

    def openOrder(self, orderId, contract, order, orderState):
        print(f"[OPEN ORDER] OrderId:{orderId}, {contract.symbol} {order.action} {order.totalQuantity} at {order.lmtPrice}")

    def execDetails(self, reqId, contract, execution):
        print(f"[EXECUTION] ReqId:{reqId}, {contract.symbol} {execution.side} {execution.shares} at {execution.price}")

    def error(self, reqId, errorCode, errorString):
        print(f"[ERROR] Id:{reqId}, Code:{errorCode}, Msg:{errorString}")

    # -----------------------------
    # Test orders
    # -----------------------------
    def start_test_orders(self):
        print("[INFO] Placing simulated bracket orders for testing...")

        # Example: simulate a LONG trade with entry, SL, and TP
        contract = Contract()
        contract.symbol = "AAPL"
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        entry_order = Order()
        entry_order.action = "BUY"
        entry_order.orderType = "LMT"
        entry_order.totalQuantity = 10
        entry_order.lmtPrice = 150.00
        entry_order.transmit = False  # do not transmit yet

        sl_order = Order()
        sl_order.action = "SELL"
        sl_order.orderType = "STP"
        sl_order.auxPrice = 148.00
        sl_order.totalQuantity = 10
        sl_order.parentId = self.nextOrderId
        sl_order.transmit = False

        tp_order = Order()
        tp_order.action = "SELL"
        tp_order.orderType = "LMT"
        tp_order.lmtPrice = 155.00
        tp_order.totalQuantity = 10
        tp_order.parentId = self.nextOrderId
        tp_order.transmit = True  # last order transmits the bracket

        # Place orders
        self.placeOrder(self.nextOrderId, contract, entry_order)
        self.placeOrder(self.nextOrderId + 1, contract, sl_order)
        self.placeOrder(self.nextOrderId + 2, contract, tp_order)
        print("[INFO] Bracket orders submitted (simulated)")

def run_loop(app):
    print("[INFO] Starting API event loop")
    app.run()

# -----------------------------
# Connect to TWS / IB Gateway
# -----------------------------
app = VerboseOrderTestApp()
app_thread = threading.Thread(target=run_loop, args=(app,), daemon=True)
app_thread.start()

print("[INFO] Connecting to IBKR...")
app.connect("127.0.0.1", 7497, clientId=123)  # TWS paper port

time.sleep(10)  # let connection establish and test orders run

print("[INFO] Disconnecting...")
app.disconnect()
print("[INFO] Test complete")