import time, pandas as pd
from datetime import datetime, timezone
from app.storage import upsert_universe, write_signals, write_equity, write_metrics
from app.data import build_universe, load_prices_and_data
from app.features import make_panel_with_augments
from app.model import train_ensemble_daily, infer_intraday
from app.risk import size_positions_and_apply_costs

def _now_ts(): return int(time.time())

def _heartbeat(tag: str):
    write_metrics([("heartbeat_" + tag, _now_ts(), 1.0)])

def run_daily():
    _heartbeat("daily_start")
    # 1) Universe
    uni_df = build_universe(max_us=60, max_uk=20, max_crypto=5)
    upsert_universe([(r.ticker, r.country, int(r.is_crypto), float(r.adv), _now_ts())
                     for r in uni_df.itertuples()])

    # 2) Data assembly (prices/funda/sentiment/macro can be expanded)
    panel = load_prices_and_data(uni_df["ticker"].tolist())
    panel = make_panel_with_augments(panel, use_finbert=False)

    # 3) Model training & daily signals
    signals_df, equity_curve = train_ensemble_daily(panel)

    # 4) Risk sizing & costs
    signals_df = size_positions_and_apply_costs(signals_df, crypto_cap=0.05)

    # 5) Persist
    write_signals([
        (_now_ts(), r.ticker, r.prob, r.signal, r.size, r.price, r.h, r.regime)
        for r in signals_df.itertuples()
    ])
    # Convert equity (date, value) to (ts, value)
    eq_rows = []
    for d, val in equity_curve:
        ts = int(pd.Timestamp(d + " 12:00:00+00:00").timestamp())
        eq_rows.append((ts, float(val)))
    write_equity(eq_rows)

    _heartbeat("daily_done")

def run_intraday():
    _heartbeat("intraday_start")
    signals_df, equity_curve = infer_intraday()
    write_signals([
        (_now_ts(), r.ticker, r.prob, r.signal, r.size, r.price, r.h, r.regime)
        for r in signals_df.itertuples()
    ])
    eq_rows = []
    for d, val in equity_curve:
        ts = int(pd.Timestamp(d + " 12:00:00+00:00").timestamp())
        eq_rows.append((ts, float(val)))
    write_equity(eq_rows)
    _heartbeat("intraday_done")

if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv)>1 else "help"
    if cmd == "daily": run_daily()
    elif cmd == "intraday": run_intraday()
    else: print("Usage: python -m app.pipeline [daily|intraday]")
