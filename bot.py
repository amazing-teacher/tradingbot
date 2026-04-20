import os
import logging
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

API_KEY    = os.environ["ALPACA_API_KEY"]
API_SECRET = os.environ["ALPACA_API_SECRET"]

client = TradingClient(API_KEY, API_SECRET, paper=True)


def run():
    order = LimitOrderRequest(
        symbol="MSFT",
        qty=1,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        limit_price=10.00,
    )
    result = client.submit_order(order)
    logging.info(f"BUY 1 MSFT @ $10.00 limit — order id: {result.id}")


if __name__ == "__main__":
    run()
