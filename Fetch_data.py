import os, requests, psycopg2
from datetime import datetime

DBURL = os.getenv("DATABASE_URL")
if not DBURL:
    raise SystemExit("DATABASE_URL not set")

def conn(): return psycopg2.connect(DBURL, connect_timeout=10)

def init():
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS market_bars(
          id SERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          ts TIMESTAMP NOT NULL,
          open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume NUMERIC
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bars_symbol_ts ON market_bars(symbol, ts);
        """); cn.commit()

def upsert(symbol, ts, price):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO market_bars(symbol, ts, open, high, low, close, volume)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol, ts) DO UPDATE SET close=EXCLUDED.close;
        """, (symbol, ts, price, price, price, price, 0))
        cn.commit()

def pull_coingecko(asset_id, label, days="1"):
    url = f"https://api.coingecko.com/api/v3/coins/{asset_id}/market_chart"
    r = requests.get(url, params={"vs_currency":"usd","days":days}, timeout=20)
    r.raise_for_status()
    for t, price in r.json().get("prices", []):
        upsert(label, datetime.utcfromtimestamp(t/1000.0), float(price))

if __name__ == "__main__":
    init()
    pull_coingecko("bitcoin", "BTC", "1")
    pull_coingecko("ethereum", "ETH", "1")
