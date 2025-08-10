import os
import json
import time
from datetime import datetime
from typing import List, Dict, Any

import requests
import httpx
import pandas as pd
import psycopg2
from psycopg2.extras import Json
from bs4 import BeautifulSoup

# ==============================
# Environment / knobs
# ==============================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise SystemExit("DATABASE_URL is not set")

UA_EMAIL = "priestndgo@gmail.com"
USER_AGENT = {"User-Agent": f"MarketAI/1.0 ({UA_EMAIL})"}

# Alerts
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # optional Slack/Discord webhook
FLOW_ALERT_MUSD = float(os.getenv("FLOW_ALERT_MUSD", "500"))

# SEC scan
SEC_KEYWORDS = os.getenv("SEC_KEYWORDS", "bitcoin OR digital asset")
SEC_FORMS = [x.strip() for x in os.getenv("SEC_FORMS", "8-K,13F-HR,13D,13G").split(",") if x.strip()]

# Timing
HTTP_TIMEOUT = 20  # seconds


# ==============================
# DB helpers
# ==============================
def conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


def ensure_schema():
    """Create all tables and indexes if they don't exist."""
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


def upsert_bar(symbol: str, ts: datetime, price: float):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO market_bars(symbol, ts, open, high, low, close, volume)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol, ts) DO UPDATE
          SET close = EXCLUDED.close;
        """, (symbol, ts, price, price, price, price, 0))
        cn.commit()


def upsert_flow(date_s: str, fund: str, flow_musd: float):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("""
        INSERT INTO etf_flows(date, fund, flow_musd)
        VALUES (%s,%s,%s)
        ON CONFLICT (date, fund) DO UPDATE
          SET flow_musd = EXCLUDED.flow_musd;
        """, (date_s, fund, float(flow_musd)))
        cn.commit()


def insert_filing(filed_at_iso: str, form: str, company: str, title: str, link: str | None):
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


def log_alert(kind: str, payload: Dict[str, Any]):
    with conn() as cn, cn.cursor() as cur:
        cur.execute("INSERT INTO alerts(kind, payload) VALUES (%s,%s)", (kind, Json(payload)))
        cn.commit()


def webhook(text: str):
    if not WEBHOOK_URL:
        return
    try:
        httpx.post(WEBHOOK_URL, json={"text": text}, timeout=10)
    except Exception:
        pass


# ==============================
# Data sources
# ==============================
def coingecko_prices(coin: str, days: int = 1):
    """BTC/ETH minute-ish prices → market_bars (symbol: BTC/ETH)."""
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart"
        r = requests.get(
            url,
            params={"vs_currency": "usd", "days": str(days)},
            headers=USER_AGENT, timeout=HTTP_TIMEOUT
        )
        r.raise_for_status()
        for t_ms, px in r.json().get("prices", []):
            ts = datetime.utcfromtimestamp(t_ms / 1000.0)
            label = "BTC" if coin == "bitcoin" else "ETH"
            upsert_bar(label, ts, float(px))
        return True, f"coingecko {coin} ok"
    except Exception as e:
        return False, f"coingecko {coin} error: {e}"


def farside_btc_flows() -> List[Dict[str, Any]]:
    """Robust scraper for Farside BTC ETF flows."""
    url = "https://www.farside.co.uk/bitcoin-etf-flows"
    try:
        r = httpx.get(url, headers=USER_AGENT, timeout=HTTP_TIMEOUT, follow_redirects=True)
        if r.status_code != 200 or not r.text:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if not table:
            # Little fallback: try pandas.read_html
            try:
                tables = pd.read_html(r.text)
                table_df = next((t for t in tables if t.shape[1] >= 3), None)
                if table_df is None:
                    return []
                rows = []
                for _, rr in table_df.iloc[:, :3].iterrows():
                    vals = [str(x) for x in rr.tolist()]
                    fund, date_s, flow_s = vals[0], vals[1], vals[2]
                    if "fund" in fund.lower():
                        continue
                    try:
                        flow = float(flow_s.replace("$", "").replace("m", "").replace(",", ""))
                        rows.append({"fund": fund, "date": date_s, "flow_musd": flow})
                    except Exception:
                        continue
                return rows
            except Exception:
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
    except Exception:
        return []


def sec_search():
    """Latest SEC filings for crypto keywords & selected forms."""
    url = "https://efts.sec.gov/LATEST/search-index"
    forms_q = " OR ".join([f'formType:"{f}"' for f in SEC_FORMS]) if SEC_FORMS else ""
    keys = f'({SEC_KEYWORDS}) AND ({forms_q})' if forms_q else f'({SEC_KEYWORDS})'
    payload = {
        "keys": keys, "category": "custom", "from": 0, "size": 40,
        "sort": [{"filedAt": {"order": "desc"}}]
    }
    try:
        resp = httpx.post(
            url,
            headers={**USER_AGENT, "Accept": "application/json", "Content-Type": "application/json"},
            json=payload, timeout=HTTP_TIMEOUT
        )
        if resp.status_code != 200:
            return []
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


# ==============================
# Tasks
# ==============================
def task_crypto():
    ok1, m1 = coingecko_prices("bitcoin", 1)
    ok2, m2 = coingecko_prices("ethereum", 1)
    return ok1 and ok2, f"{m1}; {m2}"


def task_etf_flows():
    rows = farside_btc_flows()
    dates = set()
    for r in rows:
        upsert_flow(r["date"], r["fund"], r["flow_musd"])
        dates.add(r["date"])
    # Alert on latest net
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


# ==============================
# Main
# ==============================
if __name__ == "__main__":
    started = time.time()
    ensure_schema()

    results = []
    for fn in (task_crypto, task_etf_flows, task_sec):
        try:
            ok, msg = fn()
            results.append((ok, msg))
        except Exception as e:
            results.append((False, f"{fn.__name__} error: {e}"))

    summary = {
        "ran_at": datetime.utcnow().isoformat() + "Z",
        "duration_s": round(time.time() - started, 2),
        "results": results,
    }
    try:
        log_alert("worker_summary", summary)
    except Exception:
        pass

    print("Worker summary:", json.dumps(summary, indent=2))
