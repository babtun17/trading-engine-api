# app/broker_alpaca.py
import os, time, json
from dataclasses import dataclass
import alpaca_trade_api as tradeapi

@dataclass
class AlpacaCreds:
    key: str
    secret: str
    base_url: str

def get_client() -> tradeapi.REST:
    creds = AlpacaCreds(
        key=os.environ["ALPACA_API_KEY"],
        secret=os.environ["ALPACA_API_SECRET"],
        base_url=os.environ.get("ALPACA_BASE_URL","https://paper-api.alpaca.markets")
    )
    return tradeapi.REST(key_id=creds.key, secret_key=creds.secret, base_url=creds.base_url, api_version='v2')

def market_is_open(api: tradeapi.REST) -> bool:
    try:
        clock = api.get_clock()
        return bool(getattr(clock, "is_open", False))
    except Exception:
        return True  # fail open for crypto; you may refine by asset class

def get_positions(api: tradeapi.REST) -> dict[str, dict]:
    pos = {}
    for p in api.list_positions():
        pos[p.symbol] = {"qty": float(p.qty), "avg_price": float(p.avg_entry_price)}
    return pos

def submit_market_order(api: tradeapi.REST, symbol: str, side: str, qty: float, client_order_id: str) -> dict:
    order = api.submit_order(
        symbol=symbol,
        side=side,
        type='market',
        time_in_force='day',
        qty=qty,
        client_order_id=client_order_id
    )
    return order._raw
