import os
import time
import logging
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

API_KEY    = os.environ["ALPACA_API_KEY"]
API_SECRET = os.environ["ALPACA_API_SECRET"]

client = TradingClient(API_KEY, API_SECRET, paper=True)


def cancel_all_open_orders():
    client.cancel_orders()
    logging.info("Cancelled all open orders.")


def run():
    cancel_all_open_orders()

    shares_bought = 0
    while shares_bought < 22:
        order = LimitOrderRequest(
            symbol="AAPL",
            qty=1,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
            limit_price=22.00,
        )
        result = client.submit_order(order)
        shares_bought += 1
        logging.info(f"BUY 1 AAPL @ $22.00 limit — order id: {result.id} ({shares_bought}/22)")
        if shares_bought < 22:
            time.sleep(10)

    logging.info("Done — placed 22 AAPL limit buy orders.")


if __name__ == "__main__":
    run()
