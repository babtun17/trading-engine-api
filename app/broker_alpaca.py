# app/broker_alpaca.py
import os, time, json
from dataclasses import dataclass
import alpaca_trade_api as tradeapi
from typing import Optional, Dict, Any

class BrokerError(Exception):
    """Base exception for broker operations"""
    pass

class MarketClosedError(BrokerError):
    """Raised when trying to trade while market is closed"""
    pass

class OrderSubmissionError(BrokerError):
    """Raised when order submission fails"""
    pass

class PositionRetrievalError(BrokerError):
    """Raised when position retrieval fails"""
    pass

@dataclass
class AlpacaCreds:
    key: str
    secret: str
    base_url: str

def get_client() -> tradeapi.REST:
    """Get Alpaca client with proper error handling"""
    try:
        creds = AlpacaCreds(
            key=os.environ["ALPACA_API_KEY"],
            secret=os.environ["ALPACA_API_SECRET"],
            base_url=os.environ.get("ALPACA_BASE_URL","https://paper-api.alpaca.markets")
        )
        return tradeapi.REST(key_id=creds.key, secret_key=creds.secret, base_url=creds.base_url, api_version='v2')
    except KeyError as e:
        raise BrokerError(f"Missing required environment variable: {e}")
    except Exception as e:
        raise BrokerError(f"Failed to create Alpaca client: {e}")

def market_is_open(api: tradeapi.REST) -> bool:
    """Check if market is open with proper error handling"""
    try:
        clock = api.get_clock()
        return bool(getattr(clock, "is_open", False))
    except Exception as e:
        print(f"Warning: Could not check market status: {e}")
        return True  # fail open for crypto; you may refine by asset class

def get_positions(api: tradeapi.REST) -> dict[str, dict]:
    """Get current positions with proper error handling"""
    try:
        pos = {}
        for p in api.list_positions():
            pos[p.symbol] = {"qty": float(p.qty), "avg_price": float(p.avg_entry_price)}
        return pos
    except Exception as e:
        raise PositionRetrievalError(f"Failed to retrieve positions: {e}")

def submit_market_order(api: tradeapi.REST, symbol: str, side: str, qty: float, client_order_id: str) -> dict:
    """Submit market order with proper error handling"""
    try:
        # Validate inputs
        if not isinstance(symbol, str) or not symbol.strip():
            raise OrderSubmissionError("Symbol must be a non-empty string")
        if side not in ['buy', 'sell']:
            raise OrderSubmissionError("Side must be 'buy' or 'sell'")
        if not isinstance(qty, (int, float)) or qty <= 0:
            raise OrderSubmissionError("Quantity must be a positive number")
        
        order = api.submit_order(
            symbol=symbol,
            side=side,
            type='market',
            time_in_force='day',
            qty=qty,
            client_order_id=client_order_id
        )
        return order._raw
    except Exception as e:
        raise OrderSubmissionError(f"Failed to submit order for {symbol}: {e}")
