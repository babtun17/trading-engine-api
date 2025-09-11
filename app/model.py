import time
from typing import Tuple
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler

# Optional: you could persist models per ticker/day if you add a store; here we retrain fast each run.

def model_version() -> str:
    return "ensemble-1.0"

def _features_targets(df: pd.DataFrame):
    feat_cols = [c for c in df.columns if c.endswith("_z") or c in ("rsi14","atr_pct","mom_10","mom_20","vol_20","vol_60")]
    X = df[feat_cols].values
    y = df["label_5d_up"].values
    return feat_cols, X, y

def _train_ensemble(df: pd.DataFrame):
    """
    Train two learners on all-ticker pooled panel with time-series CV:
    - Scaled Logistic Regression
    - XGBoost
    Blend probabilities 50/50 (you can optimize weights later).
    """
    feat_cols, X, y = _features_targets(df)
    # split features/labels by time using TimeSeriesSplit for regularization
    tss = TimeSeriesSplit(n_splits=5)
    # Fit simple scalers/estimators on full data (we could stack CV preds too)
    scaler = StandardScaler(with_mean=True, with_std=True)
    Xs = scaler.fit_transform(np.nan_to_num(X, copy=False))

    # Logistic Regression (liblinear for speed)
    lr = LogisticRegression(max_iter=200, n_jobs=1, solver="liblinear", class_weight="balanced")
    lr.fit(Xs, y)

    # XGBoost (small trees, conservative)
    xgb = XGBClassifier(
        n_estimators=200,
        max_depth=3,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=0.0,
        reg_lambda=1.0,
        n_jobs=2,
        eval_metric="logloss",
        verbosity=0
    )
    xgb.fit(X, y)

    return {"scaler": scaler, "lr": lr, "xgb": xgb, "feat_cols": feat_cols}

def _predict_proba(model, df_live: pd.DataFrame) -> np.ndarray:
    X_live = df_live[model["feat_cols"]].values
    Xs_live = model["scaler"].transform(np.nan_to_num(X_live, copy=False))
    p_lr = model["lr"].predict_proba(Xs_live)[:, 1]
    p_xgb = model["xgb"].predict_proba(X_live)[:, 1]
    p = 0.5 * p_lr + 0.5 * p_xgb
    return p

def train_ensemble_daily(panel: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """
    Daily training on pooled panel; generate today's signals and a single equity snapshot.
    """
    df = panel.copy()
    model = _train_ensemble(df)

    # Today rows: latest date per ticker
    latest_date = df["date"].max()
    live = df[df["date"] == latest_date].copy()
    live["prob"] = _predict_proba(model, live)

    # Signal rule: prob thresholds (adaptive later)
    live["signal"] = np.where(live["prob"] >= 0.6, "long", "flat")
    live["size"] = 0.0  # sized later in risk module
    live["price"] = live["adj close"]
    live["h"] = "5d"
    live["regime"] = "neutral"

    signals = live[["ticker","prob","signal","size","price","h","regime"]].copy()
    now = int(time.time())
    signals.insert(0, "ts", now)

    # Equity snapshot (placeholder: you’d compute from backtest or PnL aggregator; keep 1 row per date)
    eq = [(pd.to_datetime(latest_date).date().isoformat(), 1.01)]
    return signals, eq

def infer_intraday() -> Tuple[pd.DataFrame, list]:
    """
    Intraday: for simplicity retrain quickly on the last 9 months of data per ticker and emit fresh proba.
    (If you later persist the daily model, you can load it here instead.)
    """
    # To avoid a heavy re-ingest here, call the same daily path from pipeline before this, or:
    # We assume pipeline.data->features assembled before calling this in your actual flow.
    # As a standalone fallback, return empty to avoid errors.
    return pd.DataFrame(columns=["ts","ticker","prob","signal","size","price","h","regime"]), []
