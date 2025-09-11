import time, random, pandas as pd
from datetime import datetime, timezone

def model_version() -> str:
    return "prod-1.0"

def train_ensemble_daily(panel: pd.DataFrame):
    # Replace with LR/XGB/LSTM ensemble; this returns deterministic-like rows for demo
    now = int(time.time())
    rows = []
    for t in panel["Ticker"].tolist():
        prob = 0.58 + random.random()*0.2
        sig  = "long" if prob > 0.6 else "flat"
        size = 0.01 if sig == "long" else 0.0
        price= 100.0
        rows.append({"ts": now, "ticker": t, "prob": prob, "signal": sig, "size": size, "price": price, "h": "5d", "regime":"neutral"})
    d = datetime.now(timezone.utc).date().isoformat()
    equity = [(d, 1.0), (d, 1.01)]
    return pd.DataFrame(rows), equity

def infer_intraday():
    now = int(time.time())
    rows = [{
        "ts": now, "ticker": "AAPL", "prob": 0.63, "signal": "long", "size": 0.012,
        "price": 210.0, "h": "5d", "regime": "neutral"
    }]
    d = datetime.now(timezone.utc).date().isoformat()
    equity = [(d, 1.01)]
    return pd.DataFrame(rows), equity
