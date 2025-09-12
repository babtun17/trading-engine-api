import os, time, random
import logging
from typing import List, Dict
import pandas as pd
import yfinance as yf

class DataUnavailableError(Exception):
    """Raised when market data cannot be retrieved"""
    pass

# ---------- Networking hardening for yfinance
import requests
from requests.adapters import HTTPAdapter, Retry
from pandas_datareader import data as pdr
import io

def _is_crypto(t: str) -> bool:
    return isinstance(t, str) and ("-USD" in t)

def _dl_prices_stooq(tickers: List[str], period_days: int = 730) -> List[pd.DataFrame]:
    """
    Fetch EOD OHLCV from Stooq for non-crypto tickers. Returns list of tidy DataFrames.
    """
    frames: List[pd.DataFrame] = []
    cutoff = pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=period_days)

    def stooq_symbol(t: str) -> str | None:
        if _is_crypto(t):
            return None
        if t.endswith('.L'):
            base = t.split('.')[0].lower()
            return f"{base}.uk"
        return f"{t.lower()}.us"

    for t in tickers:
        sym = stooq_symbol(t)
        if not sym:
            continue
        url = f"https://stooq.com/q/d/l/?s={sym}&i=d"
        try:
            r = _yf_session.get(url, timeout=15)
            if r.status_code != 200 or not r.text or r.text.strip().lower().startswith('<'):
                # fallback to pandas-datareader which may have its own mapping
                try:
                    df = pdr.DataReader(t, 'stooq')
                except Exception:
                    df = None
                if df is None or df.empty:
                    continue
                df = df.sort_index().reset_index().rename(columns={"Date":"Date"})
            else:
                df = pd.read_csv(io.StringIO(r.text))
            if df is None or df.empty:
                continue
            # Normalize columns
            rename_map = {"Open":"Open","High":"High","Low":"Low","Close":"Close","Volume":"Volume",
                          "open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"}
            df = df.rename(columns=rename_map)
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
                df = df[df['Date'] >= cutoff]
            if "Adj Close" not in df.columns and "Close" in df.columns:
                df["Adj Close"] = df["Close"]
            df["Ticker"] = t
            keep_cols = [c for c in ["Date","Open","High","Low","Close","Adj Close","Volume","Ticker"] if c in df.columns]
            if keep_cols:
                frames.append(df[keep_cols])
        except Exception:
            continue
        finally:
            time.sleep(0.2)
    return frames

def _make_session(pool_maxsize: int = 50, retries: int = 3, backoff: float = 0.5) -> requests.Session:
    sess = requests.Session()
    # Set a browser-like User-Agent to reduce chance of upstream blocking
    sess.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        )
    })
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_maxsize, pool_maxsize=pool_maxsize)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

# Use a larger pooled session across yfinance
_yf_session = _make_session(pool_maxsize=40, retries=4, backoff=0.8)
yf.shared._requests = _yf_session  # yfinance uses this session under the hood

# Tame noisy loggers from yfinance/urllib3
logging.getLogger("yfinance").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Avoid tz cache noise in Render
try:
    cache_dir = "/opt/render/project/.cache/py-yfinance"
    os.makedirs(cache_dir, exist_ok=True)
    from yfinance.utils import set_tz_cache_location
    set_tz_cache_location(cache_dir)
except Exception:
    pass


# ---------- Helpers

# Feature flags via env (use strings '1' to enable)
DISABLE_FUNDA = os.getenv("DISABLE_FUNDA", "0") == "1"
DISABLE_NEWS = os.getenv("DISABLE_NEWS", "0") == "1"
DISABLE_MACRO = os.getenv("DISABLE_MACRO", "0") == "1"

def _dl_prices(tickers: List[str], period="2y", interval="1d", chunk=1, pause=6.0) -> pd.DataFrame:
    """
    Download in small chunks to avoid Yahoo throttling. Retries on empty batches.
    Returns tidy stacked OHLCV for all tickers that succeeded.
    """
    frames = []
    T = [t for t in tickers if isinstance(t, str) and t.strip()]

    # 0) Try Stooq first for non-crypto tickers (EOD)
    eq_tickers = [t for t in T if not _is_crypto(t)]
    stooq_frames = _dl_prices_stooq(eq_tickers)
    frames.extend(stooq_frames)
    got_eq = set([df["Ticker"].iloc[0] for df in stooq_frames if not df.empty])
    # Remaining tickers
    remaining = [t for t in T if (t not in got_eq)]
    # Split into crypto vs equities that still missing
    remaining_crypto = [t for t in remaining if _is_crypto(t)]
    remaining_eq = [t for t in remaining if not _is_crypto(t)]
    
    # Try individual ticker downloads as fallback
    # 1) Only use yfinance batch for crypto symbols (equities handled by Stooq only)
    if len(remaining_crypto) > 1:
        for i in range(0, len(remaining_crypto), chunk):
            batch = remaining_crypto[i:i+chunk]
            data = None
            for tries in range(3):
                try:
                    data = yf.download(
                        batch, period=period, interval=interval,
                        auto_adjust=False, progress=False, group_by='ticker',
                        threads=False,  # Disable threading to reduce rate limiting
                        session=_yf_session
                    )
                    if data is not None and not data.empty:
                        break
                except Exception as e:
                    print(f"Download attempt {tries + 1} failed for batch {batch}: {e}")
                    if tries < 2:  # Not the last attempt
                        time.sleep(5 * (tries + 1))  # Exponential backoff
                    data = None
            
            if data is None or data.empty:
                print(f"Skipping batch {batch} - no data retrieved")
                time.sleep(pause + random.uniform(0, 2))
                continue

            if isinstance(data.columns, pd.MultiIndex):
                for t in batch:
                    if t in data.columns.levels[0]:
                        df = data[t].copy()
                        if df.empty or "Close" not in df.columns:
                            continue
                        df["Ticker"] = t
                        df = df.reset_index()
                        # Ensure Date column is datetime
                        if 'Date' in df.columns:
                            df['Date'] = pd.to_datetime(df['Date'])
                        frames.append(df)
            else:
                # single ticker case
                if "Close" in data.columns:
                    df = data.copy()
                    df["Ticker"] = batch[0]
                    df = df.reset_index()
                    # Ensure Date column is datetime
                    if 'Date' in df.columns:
                        df['Date'] = pd.to_datetime(df['Date'])
                    frames.append(df)

            time.sleep(pause + random.uniform(0, 2))
    
    # If batch download failed, try individual tickers
    if not frames:
        print("Batch download failed, trying individual tickers...")
        # Try crypto via yfinance one-by-one; skip equities to avoid Yahoo for them
        for t in remaining_crypto:
            try:
                data = yf.download(
                    t, period=period, interval=interval,
                    auto_adjust=False, progress=False,
                    session=_yf_session
                )
                if data is not None and not data.empty and "Close" in data.columns:
                    df = data.copy().reset_index()
                else:
                    df = None
                if df is not None and not df.empty:
                    # Normalize to our schema
                    # Stooq returns columns upper-case already, with index as Date
                    if "Date" not in df.columns:
                        df = df.reset_index()
                    rename_map = {"Open":"Open","High":"High","Low":"Low","Close":"Close","Volume":"Volume",
                                   "open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"}
                    df = df.rename(columns=rename_map)
                    if "Adj Close" not in df.columns and "Close" in df.columns:
                        df["Adj Close"] = df["Close"]
                    if 'Date' in df.columns:
                        df['Date'] = pd.to_datetime(df['Date'])
                    df["Ticker"] = t
                    frames.append(df[[c for c in ["Date","Open","High","Low","Close","Adj Close","Volume","Ticker"] if c in df.columns]])
                    print(f"Successfully downloaded {t}")
                else:
                    print(f"Failed to download {t}")
            except Exception as e:
                print(f"Failed to download {t}: {e}")
            time.sleep(pause + random.uniform(0, 2))

    if not frames:
        print("All download attempts failed - no market data available")
        raise DataUnavailableError("No market data available - cannot proceed safely")
    
    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["Close"]).sort_values(["Ticker","Date"]).reset_index(drop=True)
    return out

def _macro_frame() -> pd.DataFrame:
    """
    Lightweight macro set via yfinance:
    ^VIX (vol), ^TNX (US 10y yield), UUP (DXY proxy ETF), GLD (gold), USO (oil ETF)
    """
    macro_tk = ["^VIX", "^TNX", "UUP", "GLD", "USO"]
    m = None
    for attempt in range(3):
        try:
            m = yf.download(macro_tk, period="2y", interval="1d", auto_adjust=False, progress=False, group_by='ticker', session=_yf_session)
            if m is not None and not m.empty:
                break
        except Exception as e:
            print(f"Macro download attempt {attempt + 1} failed: {e}")
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
    
    if m is None or m.empty:
        print("Failed to download macro data, returning empty DataFrame")
        return pd.DataFrame(columns=["Date"])
    
    out = []
    for t in macro_tk:
        if t not in m.columns.levels[0]:
            continue
        df = m[t][["Close"]].reset_index().rename(columns={"Close": f"{t}_Close", "Date": "Date"})
        # Ensure Date column is datetime
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
        out.append(df)
    if not out:
        return pd.DataFrame(columns=["Date"])
    macro = out[0]
    for df in out[1:]:
        macro = macro.merge(df, on="Date", how="outer")
    macro = macro.sort_values("Date").ffill()
    return macro

def _light_fundamentals(tickers: List[str]) -> pd.DataFrame:
    if DISABLE_FUNDA:
        return pd.DataFrame({
            "Ticker": tickers,
            "pe": [None]*len(tickers),
            "profit_margin": [None]*len(tickers),
            "roe": [None]*len(tickers),
            "rev_growth": [None]*len(tickers),
            "eps_growth": [None]*len(tickers),
        })
    rows: List[Dict] = []
    for t in tickers:
        info = {}
        try:
            tk = yf.Ticker(t, session=_yf_session)
            # yfinance .get_info can throw / return None when rate-limited
            info = tk.get_info() or {}
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
        time.sleep(0.05)  # tiny pacing to be gentle
    return pd.DataFrame(rows)

def _vader_sentiment(tickers: List[str]) -> pd.DataFrame:
    if DISABLE_NEWS:
        return pd.DataFrame({"Ticker": tickers, "sentiment_vader": [0.0]*len(tickers)})
    try:
        from nltk.sentiment import SentimentIntensityAnalyzer
        import nltk
        try:
            nltk.data.find('sentiment/vader_lexicon.zip')
        except LookupError:
            nltk.download('vader_lexicon')
        sia = SentimentIntensityAnalyzer()
    except Exception:
        return pd.DataFrame({"Ticker": tickers, "sentiment_vader": [0.0]*len(tickers)})

    scores = []
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=14)
    for t in tickers:
        vals = []
        try:
            ticker_obj = yf.Ticker(t, session=_yf_session)
            # Add retry logic for news retrieval
            news = []
            for attempt in range(3):
                try:
                    news = ticker_obj.news or []
                    if news:  # If we got news, break out of retry loop
                        break
                except Exception as e:
                    if attempt == 2:  # Last attempt
                        print(f"Failed to retrieve news for {t} after 3 attempts: {e}")
                    time.sleep(0.5 * (attempt + 1))  # Exponential backoff
            
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
        except Exception as e:
            print(f"Error processing news for {t}: {e}")
            pass
        mean = float(pd.Series(vals).mean()) if vals else 0.0
        scores.append({"Ticker": t, "sentiment_vader": mean})
        time.sleep(0.1)  # Increased delay to be more respectful to Yahoo
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
    
    if px.empty:
        print("Warning: No price data retrieved")
        return pd.DataFrame()
    
    # Macro (joined by Date)
    macro = _macro_frame()
    if not macro.empty:
        frame = px.merge(macro, on="Date", how="left")
    else:
        print("Warning: No macro data retrieved, proceeding without macro features")
        frame = px.copy()

    # Funda + sentiment (cross-sectional; later merged back on Ticker)
    try:
        funda = _light_fundamentals(tickers)
    except Exception as e:
        print(f"Warning: Failed to load fundamentals: {e}")
        funda = pd.DataFrame({"Ticker": tickers})
    
    try:
        senti = _vader_sentiment(tickers)
    except Exception as e:
        print(f"Warning: Failed to load sentiment: {e}")
        senti = pd.DataFrame({"Ticker": tickers, "sentiment_vader": [0.0] * len(tickers)})
    
    cs = funda.merge(senti, on="Ticker", how="left")

    # Merge cross-sectional attributes onto each row
    out = frame.merge(cs, on="Ticker", how="left")
    out = out.sort_values(["Ticker","Date"]).reset_index(drop=True)
    return out
