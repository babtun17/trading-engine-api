import numpy as np
import pandas as pd

# ---- Technical indicators (pure pandas; no TA-Lib)

def rsi(series: pd.Series, n: int = 14) -> pd.Series:
    delta = series.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(up, index=series.index).ewm(alpha=1/n, adjust=False).mean()
    roll_down = pd.Series(down, index=series.index).ewm(alpha=1/n, adjust=False).mean()
    rs = roll_up / (roll_down.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50.0).clip(0, 100)

def atr(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
    hl = (high - low).abs()
    hc = (high - close.shift(1)).abs()
    lc = (low - close.shift(1)).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=1).mean()

def make_panel_with_augments(panel: pd.DataFrame, use_finbert: bool = False) -> pd.DataFrame:
    """
    Input: tidy panel from data.load_prices_and_data()
    Output: features with proper lags and normalization per ticker.
    """
    df = panel.copy()
    df.rename(columns=str.lower, inplace=True)  # lower-case standardization
    # Ensure required columns
    req = {"date","ticker","open","high","low","close","adj close","volume"}
    missing = req - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns from data panel: {missing}")

    df["ret_1d"] = df.groupby("ticker")["adj close"].pct_change()
    df["ret_5d"] = df.groupby("ticker")["adj close"].pct_change(5)

    # Indicators
    df["rsi14"] = df.groupby("ticker", group_keys=False).apply(lambda g: rsi(g["adj close"], 14))
    df["atr"] = df.groupby("ticker", group_keys=False).apply(lambda g: atr(g["high"], g["low"], g["adj close"], 14))
    df["atr_pct"] = df["atr"] / df["adj close"].replace(0, np.nan)

    # Momentum & vol
    df["mom_10"] = df.groupby("ticker")["adj close"].pct_change(10)
    df["mom_20"] = df.groupby("ticker")["adj close"].pct_change(20)
    df["vol_20"] = df.groupby("ticker")["ret_1d"].rolling(20).std().reset_index(level=0, drop=True)
    df["vol_60"] = df.groupby("ticker")["ret_1d"].rolling(60).std().reset_index(level=0, drop=True)

    # Macro normalization (z-scores) – fill forward already in data layer
    macro_cols = [c for c in df.columns if c.endswith("_close") and c.startswith("^")]
    for c in ["^VIX_Close","^TNX_Close","UUP_Close","GLD_Close","USO_Close"]:
        if c in df.columns:
            z = (df[c] - df[c].rolling(60).mean()) / (df[c].rolling(60).std().replace(0, np.nan))
            df[c + "_z"] = z

    # Fundamentals: winsorize & zscore per date cross-section
    funda_cols = ["pe","profit_margin","roe","rev_growth","eps_growth","sentiment_vader"]
    for col in funda_cols:
        if col in df.columns:
            # forward-fill within ticker
            df[col] = df.groupby("ticker")[col].ffill().bfill()
            # cross-sectional zscore each date
            df[col + "_z"] = df.groupby("date")[col].transform(
                lambda s: (s - s.median()) / (s.mad() if s.mad() else 1.0)
            )

    # Lags to avoid lookahead
    feat_cols = [
        "rsi14","atr_pct","mom_10","mom_20","vol_20","vol_60",
        "^VIX_Close_z","^TNX_Close_z","UUP_Close_z","GLD_Close_z","USO_Close_z",
        "pe_z","profit_margin_z","roe_z","rev_growth_z","eps_growth_z","sentiment_vader_z"
    ]
    feat_cols = [c for c in feat_cols if c in df.columns]
    df[feat_cols] = df.groupby("ticker")[feat_cols].shift(1)

    # Training label: forward 5d return > 0
    df["fwd_5d"] = df.groupby("ticker")["adj close"].pct_change(5).shift(-5)
    df["label_5d_up"] = (df["fwd_5d"] > 0).astype(float)

    df = df.dropna(subset=feat_cols + ["label_5d_up"]).reset_index(drop=True)
    return df
