# app/storage.py — Supabase writer (backend)
import os, time
from typing import Iterable, Dict, List, Tuple, Any

# Option 1: Supabase Python client (recommended)
from supabase import create_client, Client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
_sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def upsert_universe(rows: Iterable[Tuple[str,str,int,float,int]]) -> None:
    """
    rows: (ticker, country, is_crypto, adv, last_update_ts)
    """
    payload = []
    for t,c,is_c,adv,ts in rows:
        payload.append({
            "ticker": t,
            "country": c,
            "is_crypto": bool(is_c),
            "adv": float(adv),
            "last_update": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))
        })
    if payload:
        _sb.table("universe").upsert(payload).execute()

def write_signals(rows: Iterable[Tuple[int,str,float,str,float,float,str,str]]) -> None:
    """
    rows: (ts, ticker, prob, signal, size, price, horizon, regime)
    """
    payload = []
    for ts,t,prob,sig,size,price,h,reg in rows:
        payload.append({
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts)),
            "ticker": t, "prob": float(prob), "signal": sig,
            "size": float(size), "price": float(price),
            "horizon": h, "regime": reg
        })
    if payload:
        _sb.table("signals").insert(payload).execute()


def write_equity(rows: Iterable[Tuple[int, float]]) -> None:
    # rows: (ts, eq) where ts converts to date primary key d
    # Ensure one row per date to avoid ON CONFLICT twice in same statement
    by_date = {}
    for ts, eq in rows:
        d = time.strftime("%Y-%m-%d", time.gmtime(ts))
        by_date[d] = float(eq)  # last wins

    payload = [{"d": d, "eq": v} for d, v in by_date.items()]
    if not payload:
        return
    try:
        if _mode == "client":
            _sb.table("equity").upsert(payload).execute()
            _log("sb_upsert_equity_ok", count=len(payload))
        elif _mode == "rest":
            _rest_upsert("equity", payload)
        else:
            _log("sb_no_client")
    except Exception as e:
        _log("sb_upsert_equity_fail", error=str(e))

def write_metrics(rows: Iterable[Tuple[str,int,float]]) -> None:
    """
    rows: (name, ts, value)
    """
    payload = []
    for name, ts, val in rows:
        payload.append({
            "name": name,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts)),
            "value": float(val)
        })
    if payload:
        _sb.table("metrics").insert(payload).execute()

# Frontend reads Supabase directly, so these are not used by the API anymore.
def fetch_universe(): return []
def fetch_latest_signals(limit=100): return []
def fetch_equity(window=250): return []
def fetch_metrics(): return []
