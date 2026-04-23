import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import date, datetime

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderStatus, TimeInForce
from alpaca.trading.requests import LimitOrderRequest, StopLimitOrderRequest

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

API_KEY      = os.environ["ALPACA_API_KEY"]
API_SECRET   = os.environ["ALPACA_API_SECRET"]
STATE_FILE   = os.environ.get("STATE_FILE", "state.json")
POLL_INTERVAL  = int(os.environ.get("POLL_INTERVAL", "60"))
PROFIT_TARGET  = 0.20
STOP_LOSS      = 0.10

client = TradingClient(API_KEY, API_SECRET, paper=True)

TERMINAL_BAD = {
    OrderStatus.CANCELED,
    OrderStatus.EXPIRED,
    OrderStatus.REJECTED,
    OrderStatus.SUSPENDED,
}


# ── State persistence ──────────────────────────────────────────────────────────

def load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

state = load_state()


# ── Helpers ────────────────────────────────────────────────────────────────────

def build_occ_symbol(ticker: str, expiration_date: str, option_type: str, strike: float) -> str:
    dt = datetime.strptime(expiration_date, "%Y-%m-%d")
    ymd = dt.strftime("%y%m%d")
    cp = "C" if option_type.lower() == "call" else "P"
    strike_int = int(round(strike * 1000))
    return f"{ticker.upper()}{ymd}{cp}{strike_int:08d}"


def place_exit_orders(order_id: str, entry: dict) -> None:
    occ_symbol = entry["occ_symbol"]
    qty        = entry["qty"]
    fill_price = entry["fill_price"]

    sell_price = round(fill_price * (1 + PROFIT_TARGET), 2)
    stop_price = round(fill_price * (1 - STOP_LOSS), 2)

    # Mark done immediately so a failure doesn't cause endless retries
    state[order_id]["exit_placed"] = True
    save_state(state)

    # Wait for Alpaca to register the position before placing exit orders
    time.sleep(5)

    sell_req = LimitOrderRequest(
        symbol=occ_symbol,
        qty=qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.GTC,
        limit_price=sell_price,
    )
    try:
        sell_result = client.submit_order(sell_req)
        logging.info(f"[{occ_symbol}] SELL limit @ ${sell_price} — id: {sell_result.id}")
    except Exception as e:
        logging.critical(f"[{occ_symbol}] SELL ORDER FAILED — manual action required: SELL {qty}x @ ${sell_price}. Error: {e}")

    stop_req = StopLimitOrderRequest(
        symbol=occ_symbol,
        qty=qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.GTC,
        stop_price=stop_price,
        limit_price=stop_price,
    )
    try:
        stop_result = client.submit_order(stop_req)
        logging.info(f"[{occ_symbol}] STOP LIMIT @ ${stop_price} — id: {stop_result.id}")
    except Exception as e:
        logging.critical(f"[{occ_symbol}] STOP ORDER FAILED — manual action required: SELL {qty}x @ ${stop_price} stop limit. Error: {e}")


# ── Background poll loop ───────────────────────────────────────────────────────

async def poll_loop() -> None:
    logging.info(f"Poll loop started — checking every {POLL_INTERVAL}s")
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        for order_id, entry in list(state.items()):
            if entry.get("exit_placed"):
                continue
            try:
                order = client.get_order_by_id(order_id)
                if order.status == OrderStatus.FILLED:
                    raw = order.filled_avg_price
                    fill_price = float(raw) if raw and float(raw) > 0 else entry["limit_buy_price"]
                    if not raw or float(raw) <= 0:
                        logging.warning(f"[{entry['occ_symbol']}] filled_avg_price unavailable — using limit_buy_price")
                    state[order_id]["fill_price"] = fill_price
                    logging.info(f"[{entry['occ_symbol']}] Filled @ ${fill_price} — placing exit orders")
                    place_exit_orders(order_id, state[order_id])
                elif order.status in TERMINAL_BAD:
                    logging.warning(f"[{entry['occ_symbol']}] Order {order_id} ended with {order.status.value} — removing from tracking")
                    del state[order_id]
                    save_state(state)
                else:
                    logging.info(f"[{entry['occ_symbol']}] Status: {order.status.value}")
            except Exception as e:
                logging.error(f"Error polling order {order_id}: {e}")


# ── FastAPI app ────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(poll_loop())
    logging.info(f"Bot started — tracking {len(state)} existing order(s)")
    yield

app = FastAPI(lifespan=lifespan)


class OrderRequest(BaseModel):
    ticker: str
    option_type: str
    strike: float
    limit_buy_price: float
    expiration_date: str
    qty: int = 1


@app.post("/order")
def submit_order(req: OrderRequest):
    if req.option_type.lower() not in ("call", "put"):
        raise HTTPException(400, "option_type must be 'call' or 'put'")
    if req.strike <= 0 or req.limit_buy_price <= 0:
        raise HTTPException(400, "strike and limit_buy_price must be positive")
    if req.qty < 1:
        raise HTTPException(400, "qty must be >= 1")
    try:
        exp = datetime.strptime(req.expiration_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "expiration_date must be YYYY-MM-DD")
    if exp.date() <= date.today():
        raise HTTPException(400, "expiration_date must be in the future")

    occ_symbol = build_occ_symbol(req.ticker, req.expiration_date, req.option_type, req.strike)

    buy_req = LimitOrderRequest(
        symbol=occ_symbol,
        qty=req.qty,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        limit_price=req.limit_buy_price,
    )
    try:
        result = client.submit_order(buy_req)
    except Exception as e:
        raise HTTPException(500, f"Failed to submit order: {e}")

    state[str(result.id)] = {
        "occ_symbol": occ_symbol,
        "qty": req.qty,
        "limit_buy_price": req.limit_buy_price,
        "exit_placed": False,
    }
    save_state(state)
    logging.info(f"BUY submitted: {occ_symbol} qty={req.qty} @ ${req.limit_buy_price} — id: {result.id}")

    return {"order_id": str(result.id), "symbol": occ_symbol, "status": "tracking"}


@app.get("/orders")
def list_orders():
    return state


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
