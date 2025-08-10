import os
import json
from datetime import datetime, timedelta

import requests
import httpx
import pandas as pd
import psycopg2
from bs4 import BeautifulSoup

# ================== ENV ==================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise SystemExit("DATABASE_URL not set")

# Optional knobs (safe defaults)
UA_EMAIL = "priestndgo@gmail.com"
FLOW_ALERT_MUSD = float(os.getenv("FLOW_ALERT_MUSD", "500"))        # alert threshold for daily net BTC ETF flow
SEC_KEYWORDS    = os.getenv("SEC_KEYWORDS", "bitcoin OR digital asset")
SEC_FORMS       = [x.strip() for x in os.getenv("SEC_FORMS", "8-K,13F-HR").split(",") if x.strip()]
WEBHOOK_URL     = os.getenv("WEBHOOK_URL")                          # Slack/Discord incoming webhook (optional)

# ================== DB ==================
def conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)

def init_schema():
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS market_bars(
          id SERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          ts TIMESTAMP NOT NULL,
          open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume NUMERIC
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bars_symbol_ts ON market_bars(symbol, ts);

        CREATE TABLE IF NOT EXISTS etf_flows(
          id SERIAL PRIMARY KEY,
          date TEXT NOT NULL,
          fund TEXT NOT NULL,
          flow_musd NUMERIC NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_etf_date_fund ON etf_flows(date, fund);

        CREATE TABLE IF NOT EXISTS sec_filings(
          id SERIAL PRIMARY KEY,
          filed_at TIMESTAMP,
          form TEXT,
          company TEXT,
          title TEXT,
          link TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sec_unique ON sec_filings(filed_at, form, company, title);

        CREATE TABLE IF NOT EXISTS alerts(
          id SERIAL PRIMARY KEY,
          ts TIMESTAMP DEFAULT NOW(),
          kind TEXT,
          payload JSONB
        );
        """)
        cn.commit()

# ================== UTIL ==================
UA = {"User-Agent": f"MarketAI/1.0 ({UA_EMAIL})"}

def ping_webhook(text: str):
    if not WEBHOOK_URL:
        return
    try:
        httpx.post(WEBHOOK_URL, json={"text": text}, timeout=10)
    except Exception:
        pass

# ================== TASK 1: CRYPTO (CoinGecko) ==================
def upsert_bar(symbol: str, ts: datetime, price: float):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO market_bars(symbol, ts, open, high, low, close, volume)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol, ts) DO UPDATE SET close=EXCLUDED.close;
        """, (symbol, ts, price, price, price, price, 0))
        cn.commit()

def pull_coingecko(asset_id: str, label: str, days: str = "1"):
    url = f"https://api.coingecko.com/api/v3/coins/{asset_id}/market_chart"
    r = requests.get(url, params={"vs_currency": "usd", "days": days}, headers=UA, timeout=20)
    r.raise_for_status()
    for t_ms, price in r.json().get("prices", []):
        ts = datetime.utcfromtimestamp(t_ms / 1000.0)
        upsert_bar(label, ts, float(price))

def task_crypto():
    try:
        pull_coingecko("bitcoin", "BTC", "1")
        pull_coingecko("ethereum", "ETH", "1")
        return True, "crypto ok"
    except Exception as e:
        return False, f"crypto error: {e}"

# ================== TASK 2: BTC ETF FLOWS (Farside) ==================
def upsert_flow(date_s: str, fund: str, flow_musd: float):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO etf_flows(date, fund, flow_musd)
        VALUES (%s,%s,%s)
        ON CONFLICT (date, fund) DO UPDATE SET flow_musd=EXCLUDED.flow_musd;
        """, (date_s, fund, float(flow_musd)))
        cn.commit()

def fetch_farside_rows():
    url = "https://www.farside.co.uk/bitcoin-etf-flows"
    r = httpx.get(url, headers=UA, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    rows = []
    for tr in table.find_all("tr"):
        cols = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
        if len(cols) < 3 or "fund" in cols[0].lower():
            continue
        try:
            fund, date_s, flow_s = cols[0], cols[1], cols[2]
            flow = float(flow_s.replace("$", "").replace("m", "").replace(",", ""))
            rows.append({"fund": fund, "date": date_s, "flow_musd": flow})
        except Exception:
            continue
    return rows

def task_etf_flows():
    try:
        rows = fetch_farside_rows()
        if not rows:
            return False, "no farside rows"
        dates = set()
        for r in rows:
            upsert_flow(r["date"], r["fund"], r["flow_musd"])
            dates.add(r["date"])
        # Alert on latest net
        latest = sorted(dates)[-1]
        with conn() as cn:
            df = pd.read_sql_query(
                "SELECT SUM(flow_musd) AS total FROM etf_flows WHERE date=%s",
                cn, params=(latest,)
            )
        net = float(df["total"].iloc[0] or 0.0)
        if abs(net) >= FLOW_ALERT_MUSD:
            payload = {"date": latest, "net_musd": net}
            with conn() as cn, cn.cursor() as cur:
                cur.execute("INSERT INTO alerts(kind, payload) VALUES (%s, %s)", ("etf_net_flow", json.dumps(payload)))
                cn.commit()
            ping_webhook(f"BTC ETF net flow on {latest}: ${net:.1f}M")
        return True, f"etf ok (latest {latest} net {net:.1f}M)"
    except Exception as e:
        return False, f"etf error: {e}"

# ================== TASK 3: SEC FILINGS ==================
def insert_filing(filed_at_iso: str, form: str, company: str, title: str, link: str | None):
    # Convert ISO to timestamp; tolerate missing or malformed strings
    ts = None
    if filed_at_iso:
        try:
            ts = datetime.fromisoformat(filed_at_iso.replace("Z", "+00:00"))
        except Exception:
            ts = None
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO sec_filings(filed_at, form, company, title, link)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (filed_at, form, company, title) DO NOTHING;
        """, (ts, form, company, title, link))
        cn.commit()

def fetch_sec_hits():
    url = "https://efts.sec.gov/LATEST/search-index"
    headers = {**UA, "Accept": "application/json", "Content-Type": "application/json"}
    form_filters = " OR ".join([f'formType:"{f}"' for f in SEC_FORMS]) if SEC_FORMS else ""
    if form_filters:
        q = f'({SEC_KEYWORDS}) AND ({form_filters})'
    else:
        q = f'({SEC_KEYWORDS})'
    payload = {
        "keys": q, "category": "custom", "from": 0, "size": 40,
        "sort": [{"filedAt": {"order": "desc"}}]
    }
    resp = httpx.post(url, headers=headers, json=payload, timeout=20)
    resp.raise_for_status()
    hits = resp.json().get("hits", {}).get("hits", [])
    out = []
    for h in hits:
        s = h.get("_source", {})
        link = s.get("link")
        out.append({
            "filed_at": s.get("filedAt"),
            "form": s.get("formType"),
            "company": (s.get("displayNames") or [s.get("companyName")])[0],
            "title": s.get("displayTitle") or s.get("documentDescription"),
            "link": (f'https://www.sec.gov/ixviewer/doc?action=display&source=content&doc={link}' if link else None)
        })
    return out

def task_sec():
    try:
        items = fetch_sec_hits()
        if not items:
            return False, "no sec items"
        for it in items:
            insert_filing(it["filed_at"], it["form"], it["company"], it["title"], it["link"])
        return True, f"sec ok ({len(items)} items)"
    except Exception as e:
        return False, f"sec error: {e}"

# ================== MAIN ==================
if __name__ == "__main__":
    init_schema()

    results = []
    for fn in (task_crypto, task_etf_flows, task_sec):
        ok, msg = fn()
        results.append((ok, msg))

    # Log a single summary alert row (for quick health checks)
    payload = {"results": results, "ran_at": datetime.utcnow().isoformat() + "Z"}
    try:
        with conn() as cn, cn.cursor() as cur:
            cur.execute("INSERT INTO alerts(kind, payload) VALUES (%s, %s)", ("worker_summary", json.dumps(payload)))
            cn.commit()
    except Exception:
        pass

    # Print to logs (visible in Railway)
    print("worker run summary:", results)
