import os, json, httpx, psycopg2, pandas as pd

DBURL = os.getenv("DATABASE_URL")
WEBHOOK = os.getenv("WEBHOOK_URL")
THRESH = float(os.getenv("FLOW_ALERT_MUSD", "500"))

def conn(): return psycopg2.connect(DBURL, connect_timeout=10)

def ping(msg):
    if WEBHOOK:
        try: httpx.post(WEBHOOK, json={"text": msg}, timeout=10)
        except Exception: pass

if __name__ == "__main__":
    if not DBURL: raise SystemExit("DATABASE_URL not set")
    with conn() as cn:
        df = pd.read_sql_query("""
            SELECT date, SUM(flow_musd) AS net
            FROM etf_flows
            GROUP BY date
            ORDER BY date DESC
            LIMIT 1
        """, cn)
    if len(df):
        d = df.iloc[0]["date"]; net = float(df.iloc[0]["net"] or 0.0)
        if abs(net) >= THRESH:
            ping(f"ALERT: BTC ETF net flow {d}: ${net:.1f}M")
