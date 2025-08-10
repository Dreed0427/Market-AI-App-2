import os
import json
from datetime import datetime
import requests, httpx, pandas as pd, psycopg2
from bs4 import BeautifulSoup

DATABASE_URL   = os.getenv("DATABASE_URL")
if not DATABASE_URL: raise SystemExit("DATABASE_URL not set")
UA_EMAIL       = "priestndgo@gmail.com"
FLOW_ALERT_MUSD = float(os.getenv("FLOW_ALERT_MUSD", "500"))
SEC_KEYWORDS    = os.getenv("SEC_KEYWORDS", "bitcoin OR digital asset")
SEC_FORMS       = [x.strip() for x in os.getenv("SEC_FORMS", "8-K,13F-HR").split(",") if x.strip()]
WEBHOOK_URL     = os.getenv("WEBHOOK_URL")

UA = {"User-Agent": f"MarketAI/1.0 ({UA_EMAIL})"}

def conn(): return psycopg2.connect(DATABASE_URL, connect_timeout=10)

def init_schema():
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS market_bars(
          id SERIAL PRIMARY KEY, symbol TEXT NOT NULL, ts TIMESTAMP NOT NULL,
          open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume NUMERIC
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bars_symbol_ts ON market_bars(symbol, ts);
        CREATE TABLE IF NOT EXISTS etf_flows(
          id SERIAL PRIMARY KEY, date TEXT NOT NULL, fund TEXT NOT NULL, flow_musd NUMERIC NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_etf_date_fund ON etf_flows(date, fund);
        CREATE TABLE IF NOT EXISTS sec_filings(
          id SERIAL PRIMARY KEY, filed_at TIMESTAMP, form TEXT, company TEXT, title TEXT, link TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sec_unique ON sec_filings(filed_at, form, company, title);
        CREATE TABLE IF NOT EXISTS alerts(
          id SERIAL PRIMARY KEY, ts TIMESTAMP DEFAULT NOW(), kind TEXT, payload JSONB
        );
        """); cn.commit()

def ping_webhook(text):
    if not WEBHOOK_URL: return
    try: httpx.post(WEBHOOK_URL, json={"text": text}, timeout=10)
    except Exception: pass

# --- crypto
def upsert_bar(symbol, ts, px):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO market_bars(symbol, ts, open, high, low, close, volume)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol, ts) DO UPDATE SET close=EXCLUDED.close;
        """, (symbol, ts, px, px, px, px, 0)); cn.commit()

def pull_coingecko(asset_id, label, days="1"):
    url = f"https://api.coingecko.com/api/v3/coins/{asset_id}/market_chart"
    r = requests.get(url, params={"vs_currency":"usd","days":days}, headers=UA, timeout=20); r.raise_for_status()
    for t_ms, px in r.json().get("prices", []):
        upsert_bar(label, datetime.utcfromtimestamp(t_ms/1000.0), float(px))

# --- etf flows
def upsert_flow(date_s, fund, flow_musd):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO etf_flows(date, fund, flow_musd)
        VALUES (%s,%s,%s)
        ON CONFLICT (date, fund) DO UPDATE SET flow_musd=EXCLUDED.flow_musd;
        """, (date_s, fund, float(flow_musd))); cn.commit()

def fetch_farside_rows():
    url = "https://www.farside.co.uk/bitcoin-etf-flows"
    r = httpx.get(url, headers=UA, timeout=20, follow_redirects=True)
    if r.status_code != 200 or not r.text: return []
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table: return []
    out = []
    for tr in table.find_all("tr"):
        cols = [c.get_text(strip=True) for c in tr.find_all(["td","th"])]
        if len(cols) < 3 or "fund" in cols[0].lower(): continue
        try:
            fund, date_s, flow_s = cols[0], cols[1], cols[2]
            flow = float(flow_s.replace("$","").replace("m","").replace(",",""))
            out.append({"fund":fund, "date":date_s, "flow_musd":flow})
        except Exception: continue
    return out

# --- sec filings
def insert_filing(iso, form, company, title, link):
    ts = None
    if iso:
        try: ts = datetime.fromisoformat(iso.replace("Z","+00:00"))
        except Exception: ts = None
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO sec_filings(filed_at, form, company, title, link)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (filed_at, form, company, title) DO NOTHING;
        """, (ts, form, company, title, link)); cn.commit()

def fetch_sec_hits():
    url = "https://efts.sec.gov/LATEST/search-index"
    ff = " OR ".join([f'formType:"{f}"' for f in SEC_FORMS]) if SEC_FORMS else ""
    q = f'({SEC_KEYWORDS}) AND ({ff})' if ff else f'({SEC_KEYWORDS})'
    payload = {"keys": q, "category": "custom", "from": 0, "size": 40, "sort": [{"filedAt":{"order":"desc"}}]}
    r = httpx.post(url, headers={**UA,"Accept":"application/json","Content-Type":"application/json"},
                   json=payload, timeout=20)
    if r.status_code != 200: return []
    hits = r.json().get("hits", {}).get("hits", [])
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

# --- run all
if __name__ == "__main__":
    init_schema()

    results = []

    # crypto
    try:
        pull_coingecko("bitcoin", "BTC", "1")
        pull_coingecko("ethereum", "ETH", "1")
        results.append((True, "crypto ok"))
    except Exception as e:
        results.append((False, f"crypto {e}"))

    # etf
    try:
        rows = fetch_farside_rows()
        dates = set()
        for r in rows:
            upsert_flow(r["date"], r["fund"], r["flow_musd"]); dates.add(r["date"])
        if dates:
            latest = sorted(dates)[-1]
            with conn() as cn:
                df = pd.read_sql_query("SELECT SUM(flow_musd) AS total FROM etf_flows WHERE date=%s", cn, params=(latest,))
            net = float(df["total"].iloc[0] or 0.0)
            if abs(net) >= FLOW_ALERT_MUSD:
                payload = {"date": latest, "net_musd": net}
                with conn() as cn, cn.cursor() as cur:
                    cur.execute("INSERT INTO alerts(kind, payload) VALUES (%s,%s)", ("etf_net_flow", json.dumps(payload))); cn.commit()
                ping_webhook(f"BTC ETF net flow on {latest}: ${net:.1f}M")
        results.append((True, "etf ok"))
    except Exception as e:
        results.append((False, f"etf {e}"))

    # sec
    try:
        for it in fetch_sec_hits():
            insert_filing(it["filed_at"], it["form"], it["company"], it["title"], it["link"])
        results.append((True, "sec ok"))
    except Exception as e:
        results.append((False, f"sec {e}"))

    # summary row for Health tab
    try:
        with conn() as cn, cn.cursor() as cur:
            cur.execute("INSERT INTO alerts(kind, payload) VALUES (%s,%s)",
                        ("worker_summary", json.dumps({"ran_at": datetime.utcnow().isoformat()+"Z", "results": results})))
            cn.commit()
    except Exception:
        pass

    print("worker run summary:", results)
