# ---- header & init ----
import os, time, sys, json
from typing import Iterable, Tuple

_mode = None
def _log(ev: str, **kw):
    print(json.dumps({"ev": ev, **kw}), file=sys.stdout, flush=True)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

_sb = None
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

import requests
HEADERS = REST = None
if SUPABASE_URL and SUPABASE_SERVICE_KEY:
    HEADERS = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }
    REST = f"{SUPABASE_URL}/rest/v1"
    if _mode is None:
        _mode = "rest"
        _log("sb_init_ok", mode=_mode)

def _rest_upsert(table: str, rows):
    r = requests.post(f"{REST}/{table}", headers={**HEADERS, "Prefer":"resolution=merge-duplicates"}, json=rows)
    if not r.ok:
        _log("sb_rest_error", table=table, status=r.status_code, text=r.text[:300])
    else:
        _log("sb_rest_ok", table=table, status=r.status_code)
    r.raise_for_status()

def _rest_insert(table: str, rows):
    r = requests.post(f"{REST}/{table}", headers=HEADERS, json=rows)
    if not r.ok:
        _log("sb_rest_error", table=table, status=r.status_code, text=r.text[:300])
    else:
        _log("sb_rest_ok", table=table, status=r.status_code)
    r.raise_for_status()

# ---- equity writer (de-dupes dates) ----
def write_equity(rows: Iterable[Tuple[int, float]]) -> None:
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
