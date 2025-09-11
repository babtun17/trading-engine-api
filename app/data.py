import time
from typing import List, Dict
import pandas as pd
import yfinance as yf

# ---------- Helpers

def _dl_prices(tickers: List[str], period="2y", interval="1d") -> pd.DataFrame:
    """
    Returns OHLCV panel with columns: ('Open','High','Low','Close','Adj Close','Volume')
    Multi-index columns from yfinance -> we stack to tidy.
    """
    data = yf.download(tickers, period=period, interval=interval, auto_adjust=False, progress=False, group_by='ticker')
    if isinstance(data.columns, pd.MultiIndex):
        frames = []
        for t in tickers:
            if t not in data.columns.levels[0]:
                continue
            df = data[t].copy()
            df["Ticker"] = t
            frames.append(df)
        out = pd.concat(frames)
    else:
        # single ticker
        out = data.copy()
        out["Ticker"] = tickers[0]
    out = out.reset_index().rename(columns={"Date":"Date"})
    out = out.dropna(subset=["Close"])
    return out  # long format by stacking later

def _macro_frame() -> pd.DataFrame:
    """
    Lightweight macro set via yfinance:
    ^VIX (vol), ^TNX (US 10y yield), UUP (DXY proxy ETF), GLD (gold), USO (oil ETF)
    """
    macro_tk = ["^VIX", "^TNX", "UUP", "GLD", "USO"]
    m = yf.download(macro_tk, period="2y", interval="1d", auto_adjust=False, progress=False, group_by='ticker')  # MultiIndex
    out = []
    for t in macro_tk:
        if t not in m.columns.levels[0]:
            continue
        df = m[t][["Close"]].reset_index().rename(columns={"Close": f"{t}_Close", "Date": "Date"})
        out.append(df)
    macro = out[0]
    for df in out[1:]:
        macro = macro.merge(df, on="Date", how="outer")
    macro = macro.sort_values("Date").ffill()
    return macro

def _light_fundamentals(tickers: List[str]) -> pd.DataFrame:
    """
    Lightweight funda using yfinance .info (may be sparse). We guard with try/except.
    Metrics: trailingPE, profitMargins, returnOnEquity, revenueGrowth, earningsGrowth.
    """
    rows: List[Dict] = []
    for t in tickers:
        try:
            info = yf.Ticker(t).get_info()  # yfinance 0.2.43
        except Exception:
            info = {}
        rows.append({
            "Ticker": t,
            "pe": info.get("trailingPE"),
            "profit_margin": info.get("profitMargins"),
            "roe": info.get("returnOnEquity"),
            "rev_growth": info.get("revenueGrowth"),
            "eps_growth": info.get("earningsGrowth"),
        })
    return pd.DataFrame(rows)

def _vader_sentiment(tickers: List[str]) -> pd.DataFrame:
    """
    Pulls recent headlines from yfinance .news (limited but free)
    and scores them with NLTK VADER; aggregates per ticker over last ~14 days.
    """
    try:
        from nltk.sentiment import SentimentIntensityAnalyzer
        import nltk
        try:
            nltk.data.find('sentiment/vader_lexicon.zip')
        except LookupError:
            nltk.download('vader_lexicon')
        sia = SentimentIntensityAnalyzer()
    except Exception:
        # if nltk missing for some reason, return zeros
        return pd.DataFrame({"Ticker": tickers, "sentiment_vader": [0.0]*len(tickers)})

    scores = []
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=14)
    for t in tickers:
        try:
            news = yf.Ticker(t).news or []
        except Exception:
            news = []
        vals = []
        for n in news:
            title = (n.get("title") or "")[:280]
            pub = n.get("providerPublishTime")
            if pub:
                ts = pd.to_datetime(pub, unit="s", utc=True)
                if ts < cutoff:
                    continue
            if title:
                s = sia.polarity_scores(title)["compound"]
                vals.append(s)
        mean = float(pd.Series(vals).mean()) if vals else 0.0
        scores.append({"Ticker": t, "sentiment_vader": mean})
    return pd.DataFrame(scores)

# ---------- Public API

def build_universe(max_us: int, max_uk: int, max_crypto: int) -> pd.DataFrame:
    """
    Simple seed universe; replace with premium screens when available.
    """
    us = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","AMD","ADBE","COST","QCOM","MU","NFLX","AVGO","NOW",
          "ORCL","CRM","TXN","INTC","LIN","ABBV","UNH","PEP","KO","JPM","BAC","WFC","GS","XOM","CVX"]
    uk = ["AZN.L","BP.L","HSBA.L","ULVR.L","RIO.L","GLEN.L","LSEG.L","VOD.L","BATS.L","DGE.L","SHEL.L"]
    crypto = ["BTC-USD","ETH-USD","SOL-USD","ADA-USD"]
    tickers = us[:max_us] + uk[:max_uk] + crypto[:max_crypto]
    now = int(time.time())
    rows = []
    for t in tickers:
        rows.append({
            "ticker": t,
            "country": "UK" if t.endswith(".L") else ("CRYPTO" if "-USD" in t else "US"),
            "is_crypto": int("-USD" in t),
            "adv": 1_000_000,
            "last_update": now
        })
    return pd.DataFrame(rows)

def load_prices_and_data(tickers: List[str]) -> pd.DataFrame:
    """
    Returns a tidy panel with per-ticker OHLCV, funda, sentiment, and macro columns merged by date.
    """
    # Prices
    px = _dl_prices(tickers, period="2y", interval="1d")
    # Macro (joined by Date)
    macro = _macro_frame()
    frame = px.merge(macro, on="Date", how="left")

    # Funda + sentiment (cross-sectional; later merged back on Ticker)
    funda = _light_fundamentals(tickers)
    senti = _vader_sentiment(tickers)
    cs = funda.merge(senti, on="Ticker", how="left")

    # Merge cross-sectional attributes onto each row
    out = frame.merge(cs, on="Ticker", how="left")
    out = out.sort_values(["Ticker","Date"]).reset_index(drop=True)
    return out
