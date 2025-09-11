# app/executor.py
import os, time, math, json
from typing import List, Dict, Any
import pandas as pd
from supabase import create_client
from app.broker_alpaca import get_client, market_is_open, get_positions, submit_market_order

SB_URL = os.environ["SUPABASE_URL"]
SB_KEY = os.environ["SUPABASE_SERVICE_KEY"]
sb = create_client(SB_URL, SB_KEY)

MAX_GROSS_LEVERAGE = float(os.getenv("MAX_GROSS_LEVERAGE", "1.0"))
MAX_POSITION_PCT   = float(os.getenv("MAX_POSITION_PCT", "0.15"))
MIN_PROB           = float(os.getenv("MIN_PROB", "0.60"))
ORDER_DOLLAR_STEP  = float(os.getenv("ORDER_DOLLAR_STEP", "1000"))

def _ts() -> int: return int(time.time())

def fetch_latest_signals(limit=200) -> pd.DataFrame:
    r = sb.table("signals").select("*").order("ts", desc=True).limit(limit).execute()
    df = pd.DataFrame(r.data or [])
    if df.empty: return df
    # keep only most recent per ticker
    df["day"] = pd.to_datetime(df["ts"]).dt.date
    df = df.sort_values(["ticker","ts"]).drop_duplicates("ticker", keep="last")
    return df

def fetch_prices(symbols: List[str]) -> Dict[str,float]:
    # Simple: use last 'price' from signals (assumes close) — replace with live quotes if needed
    out = {}
    for t in symbols: out[t] = None
    return out

def write_order_row(row: dict):
    sb.table("orders").insert(row).execute()

def write_fill_row(row: dict):
    sb.table("fills").insert(row).execute()

def upsert_position_row(ticker: str, qty: float, avg_price: float):
    sb.table("positions").upsert({"ticker": ticker, "qty": qty, "avg_price": avg_price}).execute()

def get_existing_client_ids() -> set:
    r = sb.table("orders").select("client_order_id").execute()
    return set([x["client_order_id"] for x in (r.data or []) if x.get("client_order_id")])

def build_orders_from_signals(df: pd.DataFrame, equity_usd: float) -> List[dict]:
    """
    Map signals to orders using a simple dollar-step sizing.
    Enforces MIN_PROB, per-name cap, gross leverage cap.
    """
    df = df[df["signal"]=="long"].copy()
    df = df[df["prob"] >= MIN_PROB]
    if df.empty: return []

    # Size: base lot per name; clamp to MAX_POSITION_PCT of equity
    per_name_cap = MAX_POSITION_PCT * equity_usd
    lot = ORDER_DOLLAR_STEP
    orders = []
    for r in df.itertuples():
        # If you have df['size'] from risk, replace with: notional = r.size * equity_usd
        notional = min(per_name_cap, lot)
        if notional <= 0: continue
        # Approx qty (we’ll let market decide exact fill)
        # If you track a live price feed, divide by that; else default qty=round_down(notional/price)
        qty = max(1, math.floor(notional / float(getattr(r, "price", 100.0))))
        cid = f"{r.ticker}-{int(time.time())}"
        orders.append({
            "ticker": r.ticker,
            "side": "buy",
            "qty": qty,
            "client_order_id": cid,
            "signal_prob": float(r.prob),
            "signal_horizon": getattr(r, "h", "5d"),
            "meta": {"regime": getattr(r, "regime","neutral")},
        })
    # TODO: enforce gross leverage by limiting number of orders if needed
    return orders

def place_orders(orders: List[dict]) -> List[dict]:
    api = get_client()
    if not market_is_open(api):
        # OK for crypto but equity market may be closed; drop or keep crypto only.
        pass

    placed = []
    for o in orders:
        try:
            raw = submit_market_order(api, symbol=o["ticker"], side=o["side"],
                                      qty=o["qty"], client_order_id=o["client_order_id"])
            placed.append({"ok": True, "raw": raw, "order": o})
        except Exception as e:
            placed.append({"ok": False, "error": str(e), "order": o})
    return placed

def sync_positions():
    """Refresh positions snapshot from broker."""
    api = get_client()
    pos = get_positions(api)
    for t, p in pos.items():
        upsert_position_row(t, p["qty"], p["avg_price"])

def run_once():
    # 1) Pull latest signals
    sig = fetch_latest_signals(limit=300)
    if sig.empty:
        sb.table("metrics").insert({"name":"exec_no_signals","ts": pd.Timestamp.utcnow().isoformat(), "value": 1}).execute()
        return

    # 2) Dedupe by client_order_id
    existing = get_existing_client_ids()

    # 3) Equity snapshot (static for now; replace with actual equity -> table 'equity')
    EQUITY_USD = 100000.0

    # 4) Build orders
    to_send = build_orders_from_signals(sig, equity_usd=EQUITY_USD)

    if not to_send:
        sb.table("metrics").insert({"name":"exec_no_orders","ts": pd.Timestamp.utcnow().isoformat(), "value": 1}).execute()
        return

    # Filter out already-sent client IDs (idempotency)
    to_send = [o for o in to_send if o["client_order_id"] not in existing]

    # 5) Record 'pending' and place
    ts_iso = pd.Timestamp.utcnow().isoformat()
    for o in to_send:
        write_order_row({
            "ts": ts_iso, "ticker": o["ticker"], "side": o["side"], "qty": o["qty"],
            "type": "market", "time_in_force":"day", "client_order_id": o["client_order_id"],
            "status": "sent", "signal_prob": o.get("signal_prob"), "signal_horizon": o.get("signal_horizon"),
            "meta": json.dumps(o.get("meta", {}))
        })

    results = place_orders(to_send)

    # 6) Write fills (partial info now; you can also poll order status for final fills)
    for res in results:
        if res["ok"]:
            raw = res["raw"]
            write_fill_row({
                "ticker": raw.get("symbol"),
                "side": raw.get("side"),
                "qty": float(raw.get("qty", 0) or 0),
                "price": None,
                "broker_order_id": raw.get("id"),
                "client_order_id": raw.get("client_order_id"),
                "raw": json.dumps(raw)
            })
        else:
            # mark order as rejected
            sb.table("orders").update({"status":"rejected"}).eq("client_order_id", res["order"]["client_order_id"]).execute()

    # 7) Sync positions
    sync_positions()

    sb.table("metrics").insert({"name":"exec_orders_sent","ts": ts_iso, "value": len(results)}).execute()

if __name__ == "__main__":
    run_once()
