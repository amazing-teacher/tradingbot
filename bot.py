import time
import logging
from datetime import datetime
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

API_KEY    = "YOUR_ALPACA_PAPER_API_KEY"
API_SECRET = "YOUR_ALPACA_PAPER_SECRET_KEY"
SYMBOL     = "AAPL"
SHARES_PER_BUY  = 1
TOTAL_BUYS      = 10   # buy every minute, sell after 10 buys

client = TradingClient(API_KEY, API_SECRET, paper=True)


def is_market_open() -> bool:
    clock = client.get_clock()
    return clock.is_open


def place_order(side: OrderSide, qty: int):
    order = MarketOrderRequest(
        symbol=SYMBOL,
        qty=qty,
        side=side,
        time_in_force=TimeInForce.DAY
    )
    result = client.submit_order(order)
    logging.info(f"{side.name} {qty} {SYMBOL} — order id: {result.id}")
    return result


def run():
    logging.info("AAPL trading bot started (paper mode)")

    while True:
        if not is_market_open():
            logging.info("Market closed — sleeping 60s")
            time.sleep(60)
            continue

        logging.info("Market is open — starting 10-minute buy cycle")
        shares_bought = 0

        for i in range(TOTAL_BUYS):
            # Re-check market is still open each minute
            if not is_market_open():
                logging.warning(f"Market closed mid-cycle at buy #{i+1} — aborting cycle")
                break

            place_order(OrderSide.BUY, SHARES_PER_BUY)
            shares_bought += SHARES_PER_BUY
            logging.info(f"Buy #{i+1}/{TOTAL_BUYS} complete — {shares_bought} shares held")

            if i < TOTAL_BUYS - 1:   # no sleep after the last buy
                time.sleep(60)

        if shares_bought > 0:
            logging.info(f"Selling all {shares_bought} shares")
            place_order(OrderSide.SELL, shares_bought)

        logging.info("Cycle complete — restarting loop")


if __name__ == "__main__":
    run()
