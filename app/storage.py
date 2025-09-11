# app/storage.py — Supabase writer (always exports the 4 functions)
import os, time, json, sys
from typing import Iterable, Tuple

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

def _ts_iso(ts: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))

def _log(ev: str, **kw):
    print(json.dumps({"ev": ev, **kw}), file=sys.stdout, flush=True)

# Try client first
_sb = None
_mode = None
try:
    from supabase import create_client
    if SUPABASE_URL and SUPABASE_SERVICE_KEY:
        _sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        _mode = "client"
        _log("sb_init_ok", mode=_mode)
    else:
        _log("sb_env_missing", url=bool(SUPABASE_URL), key=bool(SUPABASE_SERVICE_KEY))
except Exception as e:
    _log("sb_client_import_fail", error=str(e))

# REST fallback
import requests
HEADERS = None
REST = None
if SUPABASE_URL and SUPABASE_SERVICE_KEY:
    HEADERS = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    REST = f"{SUPABASE_URL}/rest/v1"
    if _mode is None:
        _mode = "rest"
        _log("sb_init_ok", mode=_mode)

def _rest_upsert(table: str, rows):
    r = requests.post(f"{REST}/{table}", headers=HEADERS, json=rows)
    if not r.ok:
        _log("sb_rest_error", table=table, status=r.status_code, text=r.text[:300])
    else:
        _log("sb_rest_ok", table=table, status=r.status_code)
    r.raise_for_status()

def _rest_insert(table: str, rows):
    # same as upsert but without Prefer header
    headers = {k:v for k,v in HEADERS.items() if k != "Prefer"}
    r = requests.post(f"{REST}/{table}", headers=headers, json=rows)
    if not r.ok:
        _log("sb_rest_error", table=table, status=r.status_code, text=r.text[:300])
    else:
        _log("sb_rest_ok", table=table, status=r.status_code)
    r.raise_for_status()

# ---- EXPORTED FUNCTIONS (pipeline imports these) ----

def upsert_universe(rows: Iterable[Tuple[str,str,int,float,int]]) -> None:
    """rows: (ticker, country, is_crypto, adv, last_update_ts)"""
    payload = [{
        "ticker": t,
        "country": c,
        "is_crypto": bool(is_c),
        "adv": float(adv),
        "last_update": _ts_iso(ts)
    } for t,c,is_c,adv,ts in rows]
    if not payload: return
    try:
        if _mode == "client":
            _sb.table("universe").upsert(payload).execute()
            _log("sb_upsert_universe_ok", count=len(payload))
        elif _mode == "rest":
            _rest_upsert("universe", payload)
        else:
            _log("sb_no_client")
    except Exception as e:
        _log("sb_upsert_universe_fail", error=str(e))

def write_signals(rows: Iterable[Tuple[int,str,float,str,float,float,str,str]]) -> None:
    """rows: (ts, ticker, prob, signal, size, price, horizon, regime)"""
    payload = [{
        "ts": _ts_iso(ts),
        "ticker": t,
        "prob": float(prob),
        "signal": sig,
        "size": float(size),
        "price": float(price),
        "horizon": h,
        "regime": reg
    } for ts,t,prob,sig,size,price,h,reg in rows]
    if not payload: return
    try:
        if _mode == "client":
            _sb.table("signals").insert(payload).execute()
            _log("sb_insert_signals_ok", count=len(payload))
        elif _mode == "rest":
            _rest_insert("signals", payload)
        else:
            _log("sb_no_client")
    except Exception as e:
        _log("sb_insert_signals_fail", error=str(e))

def write_equity(rows: Iterable[Tuple[int,float]]) -> None:
    """rows: (ts_midday_utc, eq) -> stored as date d"""
    payload = []
    for ts, eq in rows:
        d = time.strftime("%Y-%m-%d", time.gmtime(ts))
        payload.append({"d": d, "eq": float(eq)})
    if not payload: return
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
    """rows: (name, ts, value)"""
    payload = [{"name": name, "ts": _ts_iso(ts), "value": float(val)} for name,ts,val in rows]
    if not payload: return
    try:
        if _mode == "client":
            _sb.table("metrics").insert(payload).execute()
            _log("sb_insert_metrics_ok", count=len(payload))
        elif _mode == "rest":
            _rest_insert("metrics", payload)
        else:
            _log("sb_no_client")
    except Exception as e:
        _log("sb_insert_metrics_fail", error=str(e))

# Optional: API uses these in Option B; for Option A they can remain no-ops.
def fetch_universe(): return []
def fetch_latest_signals(limit=100): return []
def fetch_equity(window=250): return []
def fetch_metrics(): return []
