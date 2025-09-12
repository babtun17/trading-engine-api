import time, pandas as pd
import numpy as np
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

from app.data import load_prices_and_data, build_universe
from app.features import make_panel_with_augments
from app.model import _train_ensemble, _predict_proba  # import internal helpers

def run_intraday():
    try:
        _heartbeat("intraday_start")
        # small, fast intraday refresh using latest data
        uni_df = build_universe(max_us=40, max_uk=15, max_crypto=3)
        
        # Try to load data with fallback
        try:
            panel = load_prices_and_data(uni_df["ticker"].tolist())
        except Exception as e:
            print(f"Failed to load prices and data: {e}")
            # Fallback: try with a smaller subset
            print("Trying with smaller subset...")
            uni_df = build_universe(max_us=20, max_uk=10, max_crypto=2)
            panel = load_prices_and_data(uni_df["ticker"].tolist())
        
        if panel.empty:
            print("No data available for intraday processing")
            write_metrics([("intraday_error", _now_ts(), 1.0)])
            return
            
        feats = make_panel_with_augments(panel, use_finbert=False)

        if feats.empty:
            print("No features available for intraday processing")
            write_metrics([("intraday_error", _now_ts(), 1.0)])
            return

        model = _train_ensemble(feats)  # fast; can reduce n_estimators to 120
        latest = feats[feats["date"] == feats["date"].max()].copy()
        
        if latest.empty:
            print("No latest data available for intraday processing")
            write_metrics([("intraday_error", _now_ts(), 1.0)])
            return
            
        latest["prob"] = _predict_proba(model, latest)
        from app.constants import SIGNAL_THRESHOLD
        latest["signal"] = np.where(latest["prob"] >= SIGNAL_THRESHOLD, "long", "flat")
        latest["size"] = 0.0
        latest["price"] = latest["adj close"]
        latest["h"] = "5d"
        latest["regime"] = "neutral"

        from app.risk import size_positions_and_apply_costs
        latest = size_positions_and_apply_costs(latest, crypto_cap=0.05)

        now = int(time.time())
        sig_rows = [(now, r.ticker, r.prob, r.signal, r.size, r.price, r.h, r.regime) for r in latest.itertuples()]
        write_signals(sig_rows)

        # equity refresh (optional): you can recompute a mark-to-market or just no-op
        write_metrics([("intraday_runtime_s", now, 1.0)])
        _heartbeat("intraday_done")
        
    except Exception as e:
        print(f"Intraday pipeline failed: {e}")
        write_metrics([("intraday_error", _now_ts(), 1.0)])
        raise

if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv)>1 else "help"
    if cmd == "daily": run_daily()
    elif cmd == "intraday": run_intraday()
    else: print("Usage: python -m app.pipeline [daily|intraday]")
