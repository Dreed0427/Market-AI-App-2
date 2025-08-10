import os, json, time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Iterable

import requests, httpx, pandas as pd, psycopg2
from psycopg2.extras import Json
from bs4 import BeautifulSoup

# ==============================
# ENV / KNOBS
# ==============================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise SystemExit("DATABASE_URL is not set")

UA_EMAIL       = "priestndgo@gmail.com"
UA_HDRS        = {"User-Agent": f"MarketAI/1.0 ({UA_EMAIL})"}

# Alerts (optional)
WEBHOOK_URL     = os.getenv("WEBHOOK_URL")
FLOW_ALERT_MUSD = float(os.getenv("FLOW_ALERT_MUSD", "500"))

# SEC scan
SEC_KEYWORDS = os.getenv("SEC_KEYWORDS", "bitcoin OR digital asset")
SEC_FORMS    = [x.strip() for x in os.getenv("SEC_FORMS", "8-K,13F-HR,13D,13G").split(",") if x.strip()]

# Finnhub / FRED
FINNHUB_KEY = os.getenv("FINNHUB_KEY")  # optional but used if present
FRED_KEY    = os.getenv("FRED_KEY")     # optional but used if present

# Choose what to pull
FINNHUB_TICKERS: Iterable[str] = os.getenv("FINNHUB_TICKERS", "SPY,AAPL,TSLA,QQQ,MSFT").split(",")
FRED_SERIES:    Iterable[str] = os.getenv("FRED_SERIES", "CPIAUCSL,DGS2,DGS10,UNRATE").split(",")

# HTTP
HTTP_TIMEOUT = 20  # seconds


# ==============================
# DB HELPERS
# ==============================
def conn(): return psycopg2.connect(DATABASE_URL, connect_timeout=10)

def ensure_schema():
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

        CREATE TABLE IF NOT EXISTS fred_series(
          id SERIAL PRIMARY KEY,
          series_id TEXT NOT NULL,
          date DATE NOT NULL,
          value NUMERIC
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_fred_series_date ON fred_series(series_id, date);

        CREATE TABLE IF NOT EXISTS news(
          id SERIAL PRIMARY KEY,
          source TEXT,
          symbol TEXT,
          dt TIMESTAMP,
          headline TEXT,
          url TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_news_symbol_dt ON news(symbol, dt DESC);

        CREATE TABLE IF NOT EXISTS alerts(
          id SERIAL PRIMARY KEY,
          ts TIMESTAMP DEFAULT NOW(),
          kind TEXT,
          payload JSONB
        );
        """); cn.commit()

def upsert_bar(symbol: str, ts: datetime, price: float):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO market_bars(symbol, ts, open, high, low, close, volume)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol, ts) DO UPDATE SET close = EXCLUDED.close;
        """, (symbol, ts, price, price, price, price, 0)); cn.commit()

def upsert_flow(date_s: str, fund: str, flow_musd: float):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO etf_flows(date, fund, flow_musd)
        VALUES (%s,%s,%s)
        ON CONFLICT (date, fund) DO UPDATE SET flow_musd = EXCLUDED.flow_musd;
        """, (date_s, fund, float(flow_musd))); cn.commit()

def insert_filing(filed_at_iso: str|None, form: str|None, company: str|None, title: str|None, link: str|None):
    ts = None
    if filed_at_iso:
        try: ts = datetime.fromisoformat(filed_at_iso.replace("Z","+00:00"))
        except Exception: ts = None
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO sec_filings(filed_at, form, company, title, link)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (filed_at, form, company, title) DO NOTHING;
        """, (ts, form, company, title, link)); cn.commit()

def upsert_fred(series_id: str, date_str: str, value: float|None):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO fred_series(series_id, date, value)
        VALUES (%s,%s,%s)
        ON CONFLICT (series_id, date) DO UPDATE SET value = EXCLUDED.value;
        """, (series_id, date_str, value)); cn.commit()

def insert_news(source: str, symbol: str, dt: datetime|None, headline: str|None, url: str|None):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO news(source, symbol, dt, headline, url)
        VALUES (%s,%s,%s,%s,%s)
        """, (source, symbol, dt, headline, url)); cn.commit()

def log_alert(kind: str, payload: Dict[str, Any]):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("INSERT INTO alerts(kind, payload) VALUES (%s,%s)", (kind, Json(payload))); cn.commit()

def webhook(text: str):
    if not WEBHOOK_URL: return
    try: httpx.post(WEBHOOK_URL, json={"text": text}, timeout=10)
    except Exception: pass


# ==============================
# DATA SOURCES
# ==============================
def coingecko_prices(coin: str, days: int = 1):
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart"
        r = requests.get(url, params={"vs_currency":"usd","days":str(days)}, headers=UA_HDRS, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        for t_ms, px in r.json().get("prices", []):
            ts = datetime.utcfromtimestamp(t_ms/1000.0)
            label = "BTC" if coin == "bitcoin" else "ETH"
            upsert_bar(label, ts, float(px))
        return True, f"coingecko {coin} ok"
    except Exception as e:
        return False, f"coingecko {coin} error: {e}"

def farside_btc_flows() -> List[Dict[str, Any]]:
    url = "https://www.farside.co.uk/bitcoin-etf-flows"
    try:
        r = httpx.get(url, headers=UA_HDRS, timeout=HTTP_TIMEOUT, follow_redirects=True)
        if r.status_code != 200 or not r.text: return []
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if table:
            rows = []
            for tr in table.find_all("tr"):
                cols = [c.get_text(strip=True) for c in tr.find_all(["td","th"])]
                if len(cols) < 3 or "fund" in cols[0].lower(): continue
                try:
                    fund, date_s, flow_s = cols[0], cols[1], cols[2]
                    flow = float(flow_s.replace("$","").replace("m","").replace(",",""))
                    rows.append({"fund":fund, "date":date_s, "flow_musd":flow})
                except Exception: continue
            return rows
        # fallback: pandas
        try:
            tables = pd.read_html(r.text)
            df = next((t for t in tables if t.shape[1] >= 3), None)
            if df is None: return []
            rows = []
            for _, rr in df.iloc[:, :3].iterrows():
                vals = [str(x) for x in rr.tolist()]
                fund, date_s, flow_s = vals[0], vals[1], vals[2]
                if "fund" in fund.lower(): continue
                try:
                    flow = float(flow_s.replace("$","").replace("m","").replace(",",""))
                    rows.append({"fund":fund, "date":date_s, "flow_musd":flow})
                except Exception: continue
            return rows
        except Exception:
            return []
    except Exception:
        return []

def sec_search():
    url = "https://efts.sec.gov/LATEST/search-index"
    forms_q = " OR ".join([f'formType:\"{f}\"' for f in SEC_FORMS]) if SEC_FORMS else ""
    keys = f'({SEC_KEYWORDS}) AND ({forms_q})' if forms_q else f'({SEC_KEYWORDS})'
    payload = {"keys": keys, "category": "custom", "from": 0, "size": 40, "sort": [{"filedAt":{"order":"desc"}}]}
    try:
        resp = httpx.post(url, headers={**UA_HDRS, "Accept":"application/json","Content-Type":"application/json"},
                          json=payload, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200: return []
        hits = resp.json().get("hits", {}).get("hits", [])
        out = []
        for h in hits:
            s = h.get("_source", {})
            link = s.get("link")
            out.append({
                "filedAt": s.get("filedAt"),
                "form": s.get("formType"),
                "company": (s.get("displayNames") or [s.get("companyName")])[0],
                "title": s.get("displayTitle") or s.get("documentDescription"),
                "link": f'https://www.sec.gov/ixviewer/doc?action=display&source=content&doc={link}' if link else None
            })
        return out
    except Exception:
        return []

# ---------- FINNHUB ----------
def finnhub_quote(symbol: str) -> Dict[str, Any] | None:
    if not FINNHUB_KEY: return None
    try:
        r = requests.get("https://finnhub.io/api/v1/quote",
                         params={"symbol": symbol, "token": FINNHUB_KEY},
                         headers=UA_HDRS, timeout=HTTP_TIMEOUT)
        if not r.ok: return None
        j = r.json()
        return {"c": j.get("c"), "pc": j.get("pc"), "t": j.get("t")}
    except Exception:
        return None

def finnhub_candles(symbol: str, frm: int, to: int, res: str = "5") -> pd.DataFrame | None:
    if not FINNHUB_KEY: return None
    try:
        r = requests.get("https://finnhub.io/api/v1/stock/candle",
                         params={"symbol": symbol, "resolution": res, "from": frm, "to": to, "token": FINNHUB_KEY},
                         headers=UA_HDRS, timeout=HTTP_TIMEOUT)
        j = r.json()
        if j.get("s") != "ok": return None
        return pd.DataFrame({
            "ts": [datetime.utcfromtimestamp(t) for t in j["t"]],
            "close": j["c"]
        })
    except Exception:
        return None

def finnhub_news(symbol: str) -> List[Dict[str, Any]]:
    if not FINNHUB_KEY: return []
    try:
        to = datetime.utcnow().date()
        frm = to - timedelta(days=3)
        r = requests.get("https://finnhub.io/api/v1/company-news",
                         params={"symbol": symbol, "from": frm.isoformat(), "to": to.isoformat(), "token": FINNHUB_KEY},
                         headers=UA_HDRS, timeout=HTTP_TIMEOUT)
        if not r.ok: return []
        out = []
        for i in r.json()[:10]:
            dt = datetime.utcfromtimestamp(i["datetime"]) if i.get("datetime") else None
            out.append({"symbol": symbol, "dt": dt, "headline": i.get("headline"), "url": i.get("url")})
        return out
    except Exception:
        return []

# ---------- FRED ----------
def fred_fetch(series_id: str) -> List[Dict[str, Any]]:
    if not FRED_KEY: return []
    try:
        r = requests.get("https://api.stlouisfed.org/fred/series/observations",
                         params={"series_id": series_id, "file_type":"json", "api_key": FRED_KEY},
                         headers=UA_HDRS, timeout=HTTP_TIMEOUT)
        if not r.ok: return []
        obs = r.json().get("observations", [])
        out = []
        for o in obs[-240:]:  # last ~20 years monthly-ish
            val = o.get("value")
            try: val_num = float(val)
            except Exception: val_num = None
            out.append({"series_id": series_id, "date": o.get("date"), "value": val_num})
        return out
    except Exception:
        return []


# ==============================
# TASKS
# ==============================
def task_crypto():
    ok1, m1 = coingecko_prices("bitcoin", 1)
    ok2, m2 = coingecko_prices("ethereum", 1)
    return ok1 and ok2, f"{m1}; {m2}"

def task_etf_flows():
    rows = farside_btc_flows()
    dates = set()
    for r in rows:
        upsert_flow(r["date"], r["fund"], r["flow_musd"]); dates.add(r["date"])
    if dates:
        latest = sorted(dates)[-1]
        with conn() as cn:
            df = pd.read_sql_query("SELECT SUM(flow_musd) total FROM etf_flows WHERE date=%s", cn, params=(latest,))
        net = float(df["total"].iloc[0] or 0.0)
        if abs(net) >= FLOW_ALERT_MUSD:
            webhook(f"BTC ETF net flow on {latest}: ${net:.1f}M")
            log_alert("etf_net_flow", {"date": latest, "net_musd": net})
    return True, f"etf rows upserted: {len(rows)}"

def task_sec():
    items = sec_search()
    for it in items:
        insert_filing(it.get("filedAt"), it.get("form"), it.get("company"), it.get("title"), it.get("link"))
    return True, f"sec filings ingested: {len(items)}"

def task_finnhub():
    """Upsert candles/latest price and news for a small ticker set."""
    if not FINNHUB_KEY:
        return True, "finnhub skipped (no key)"
    to = int(datetime.utcnow().timestamp())
    frm = int((datetime.utcnow() - timedelta(days=2)).timestamp())
    total_rows, total_news = 0, 0
    for sym in [s.strip().upper() for s in FINNHUB_TICKERS if s.strip()]:
        df = finnhub_candles(sym, frm, to, res="5")
        if df is None or df.empty:
            q = finnhub_quote(sym)
            if q and q.get("c") and q.get("t"):
                upsert_bar(sym, datetime.utcfromtimestamp(q["t"]), float(q["c"]))
                total_rows += 1
        else:
            for _, r in df.iterrows():
                upsert_bar(sym, r["ts"], float(r["close"]))
            total_rows += len(df)

        # News
        news_items = finnhub_news(sym)
        for n in news_items:
            insert_news("finnhub", sym, n["dt"], n["headline"], n["url"])
        total_news += len(news_items)
    return True, f"finnhub bars: {total_rows}, news: {total_news}"

def task_fred():
    if not FRED_KEY:
        return True, "fred skipped (no key)"
    rows = 0
    for sid in [s.strip() for s in FRED_SERIES if s.strip()]:
        obs = fred_fetch(sid)
        for o in obs:
            upsert_fred(sid, o["date"], o["value"]); rows += 1
    return True, f"fred rows upserted: {rows}"


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    started = time.time()
    ensure_schema()

    results = []
    for fn in (task_crypto, task_etf_flows, task_sec, task_finnhub, task_fred):
        try:
            ok, msg = fn()
            results.append((ok, msg))
        except Exception as e:
            results.append((False, f"{fn.__name__} error: {e}"))

    summary = {
        "ran_at": datetime.utcnow().isoformat()+"Z",
        "duration_s": round(time.time()-started, 2),
        "results": results,
    }
    try: log_alert("worker_summary", summary)
    except Exception: pass
    print("Worker summary:", json.dumps(summary, indent=2))
