import os, json, httpx, pandas as pd, psycopg2
from bs4 import BeautifulSoup
from datetime import datetime

DBURL = os.getenv("DATABASE_URL")
if not DBURL:
    raise SystemExit("DATABASE_URL not set")

UA = {"User-Agent": "MarketAI/1.0 (contact: priestndgo@gmail.com)"}
FLOW_ALERT = float(os.getenv("FLOW_ALERT_MUSD", "500"))  # alert if |net flow| >= this
SEC_QUERY = os.getenv("SEC_KEYWORDS", "bitcoin OR digital asset")
SEC_FORMS = os.getenv("SEC_FORMS", "8-K,13F-HR").split(",")
WEBHOOK = os.getenv("WEBHOOK_URL")  # Slack/Discord webhook (optional)

def conn(): return psycopg2.connect(DBURL, connect_timeout=10)

def init():
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
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
        """); cn.commit()

def upsert_flow(date_s, fund, flow_musd):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO etf_flows(date, fund, flow_musd)
        VALUES (%s,%s,%s)
        ON CONFLICT (date, fund) DO UPDATE SET flow_musd=EXCLUDED.flow_musd;
        """, (date_s, fund, float(flow_musd)))
        cn.commit()

def insert_filing(filed_at, form, company, title, link):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO sec_filings(filed_at, form, company, title, link)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (filed_at, form, company, title) DO NOTHING;
        """, (filed_at, form, company, title, link))
        cn.commit()

def log_alert(kind, payload):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("INSERT INTO alerts(kind, payload) VALUES (%s, %s)", (kind, json.dumps(payload)))
        cn.commit()

def ping_webhook(text):
    if not WEBHOOK: return
    try:
        httpx.post(WEBHOOK, json={"text": text}, timeout=10)
    except Exception:
        pass

def fetch_farside():
    url = "https://www.farside.co.uk/bitcoin-etf-flows"
    r = httpx.get(url, headers=UA, timeout=20); r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table: return []
    rows = []
    for tr in table.find_all("tr"):
        cols = [c.get_text(strip=True) for c in tr.find_all(["td","th"])]
        if len(cols) < 3 or "fund" in cols[0].lower(): continue
        rows.append(cols)
    out = []
    for r0 in rows:
        try:
            fund, date_s, flow_s = r0[0], r0[1], r0[2]
            flow = float(flow_s.replace("$","").replace("m","").replace(",",""))
            out.append({"fund":fund, "date":date_s, "flow_musd":flow})
        except Exception: continue
    return out

def fetch_sec():
    url = "https://efts.sec.gov/LATEST/search-index"
    form_filters = " OR ".join([f'formType:"{f}"' for f in SEC_FORMS])
    q = f'({SEC_QUERY}) AND ({form_filters})'
    payload = {"keys": q, "category": "custom", "from": 0, "size": 40, "sort": [{"filedAt":{"order":"desc"}}]}
    r = httpx.post(url, headers={**UA, "Accept":"application/json","Content-Type":"application/json"}, json=payload, timeout=20)
    r.raise_for_status()
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

if __name__ == "__main__":
    init()

    # ETF flows
    flows = fetch_farside()
    dates = set()
    for row in flows:
        upsert_flow(row["date"], row["fund"], row["flow_musd"])
        dates.add(row["date"])
    if dates:
        latest = sorted(dates)[-1]
        with conn() as cn:
            df = pd.read_sql_query("SELECT SUM(flow_musd) AS total FROM etf_flows WHERE date=%s", cn, params=(latest,))
        net = float(df["total"].iloc[0] or 0.0)
        if abs(net) >= FLOW_ALERT:
            payload = {"date": latest, "net_musd": net}
            log_alert("etf_net_flow", payload)
            ping_webhook(f"BTC ETF net flow on {latest}: ${net:.1f}M")

    # SEC filings
    try:
        items = fetch_sec()
        for it in items:
            ts = None
            try:
                ts = datetime.fromisoformat(it["filed_at"].replace("Z","+00:00")) if it["filed_at"] else None
            except Exception:
                pass
            insert_filing(ts, it["form"], it["company"], it["title"], it["link"])
    except Exception:
        pass
