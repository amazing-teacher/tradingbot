import os
import time
import logging
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data import StockHistoricalDataClient, ScreenerClient
from alpaca.data.requests import StockSnapshotRequest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

API_KEY    = os.environ["ALPACA_API_KEY"]
API_SECRET = os.environ["ALPACA_API_SECRET"]
SHARES_PER_BUY = 1
INTERVAL_SECS  = 30
MIN_VOLUME     = 500_000
MIN_PRICE      = 10.0
MAX_PRICE      = 500.0
TOP_N          = 5
LIMIT_OFFSET   = 0.995  # place limit 0.5% below latest trade price

client          = TradingClient(API_KEY, API_SECRET, paper=True)
data_client     = StockHistoricalDataClient(API_KEY, API_SECRET)
screener_client = ScreenerClient(API_KEY, API_SECRET)


def select_tickers() -> list[str]:
    movers  = screener_client.get_market_movers(top=20).gainers
    symbols = [m.symbol for m in movers]

    snapshots = data_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=symbols))

    candidates = []
    for mover in movers:
        snap = snapshots.get(mover.symbol)
        if snap is None:
            continue
        price  = snap.latest_trade.price
        volume = snap.daily_bar.volume if snap.daily_bar else 0
        if MIN_PRICE <= price <= MAX_PRICE and volume >= MIN_VOLUME:
            candidates.append((mover.symbol, mover.percent_change))

    candidates.sort(key=lambda x: x[1], reverse=True)
    selected = [sym for sym, _ in candidates[:TOP_N]]
    logging.info(f"Selected tickers: {selected}")
    return selected


def place_limit_buy(symbol: str, qty: int):
    snap        = data_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=symbol))
    limit_price = round(snap[symbol].latest_trade.price * LIMIT_OFFSET, 2)
    order = LimitOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        limit_price=limit_price,
    )
    result = client.submit_order(order)
    logging.info(f"BUY {qty} {symbol} @ ${limit_price} limit — order id: {result.id}")
    return result


def run():
    tickers = select_tickers()
    if not tickers:
        logging.warning("No tickers passed screening — exiting.")
        return
    logging.info(f"Trading {len(tickers)} tickers in paper mode")
    for i, symbol in enumerate(tickers):
        place_limit_buy(symbol, SHARES_PER_BUY)
        if i < len(tickers) - 1:
            time.sleep(INTERVAL_SECS)
    logging.info(f"Done — placed {len(tickers)} limit buy orders.")


if __name__ == "__main__":
    run()
