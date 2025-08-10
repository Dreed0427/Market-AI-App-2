# ... keep your existing code above ...

import time, requests
from datetime import datetime, timedelta
FINNHUB_KEY = os.getenv("FINNHUB_KEY")
WATCH = os.getenv("WATCH_TICKERS", "SPY,AAPL,QQQ").split(",")

def finnhub_candles(symbol, days=1):
    if not FINNHUB_KEY: return []
    now = int(datetime.utcnow().timestamp())
    frm = int((datetime.utcnow() - timedelta(days=days)).timestamp())
    r = requests.get("https://finnhub.io/api/v1/stock/candle",
                     params={"symbol":symbol,"resolution":"5","from":frm,"to":now,"token":FINNHUB_KEY},
                     timeout=20)
    j = r.json()
    if j.get("s") != "ok": return []
    return [(datetime.utcfromtimestamp(t), float(c)) for t, c in zip(j["t"], j["c"])]

if __name__ == "__main__":
    init()
    # crypto
    pull_coingecko("bitcoin","BTC","1")
    pull_coingecko("ethereum","ETH","1")
    # stocks
    for sym in WATCH:
        for ts, price in finnhub_candles(sym.strip().upper(), days=1):
            upsert(sym.strip().upper(), ts, price)
