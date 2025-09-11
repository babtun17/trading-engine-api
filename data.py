import pandas as pd, time

def build_universe(max_us: int, max_uk: int, max_crypto: int) -> pd.DataFrame:
    # Production tip: replace with Polygon/Finnhub top actives + liquidity screens
    us = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","AMD","ADBE","COST","TXN","QCOM","MU","NOW"]
    uk = ["AZN.L","BP.L","HSBA.L","ULVR.L","RIO.L","GLEN.L","LSEG.L","VOD.L"]
    crypto = ["BTC-USD","ETH-USD","SOL-USD"]
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

def load_prices_and_data(tickers: list[str]) -> pd.DataFrame:
    # Placeholder panel; swap in yfinance + funda/news/macro joins as you iterate
    return pd.DataFrame({"Ticker": tickers, "Close": [100.0]*len(tickers)})
