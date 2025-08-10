import os
from datetime import datetime, timedelta

import pandas as pd
import requests
import streamlit as st
import httpx
from bs4 import BeautifulSoup

# ---------------- Config / Env ----------------
REFRESH_MS = int(os.getenv("REFRESH_MS", "60000"))  # auto-refresh every 60s
USE_DB = bool(os.getenv("DATABASE_URL"))
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
FINNHUB_KEY = os.getenv("FINNHUB_KEY")  # optional (stocks)
FRED_KEY = os.getenv("FRED_KEY")        # optional (macro)
UA_EMAIL = "priestndgo@gmail.com"       # your email for polite User-Agent

if USE_DB:
    import psycopg2  # Postgres driver

st.set_page_config(page_title="Market AI", page_icon="📈", layout="centered")
st.markdown(
    "<h1 style='margin-bottom:0'>📈 Market AI</h1>"
    "<p style='color:#9aa0a6;margin-top:2px'>Phone-friendly market dashboard</p>",
    unsafe_allow_html=True,
)
# Auto-refresh (simple JS)
st.markdown(
    f"<script>setTimeout(()=>window.location.reload(), {REFRESH_MS});</script>",
    unsafe_allow_html=True,
)

# ---------------- DB helpers ----------------
def db_conn():
    if not USE_DB: return None
    return psycopg2.connect(os.getenv("DATABASE_URL"), connect_timeout=10)

def ensure_table():
    if not USE_DB: return
    with db_conn() as cn, cn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS market_bars(
          id SERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          ts TIMESTAMP NOT NULL,
          open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume NUMERIC
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bars_symbol_ts
          ON market_bars(symbol, ts);
        """)
        cn.commit()

def save_rows(symbol, rows):  # rows list[(ts, price)]
    if not USE_DB or not rows: return
    with db_conn() as cn, cn.cursor() as cur:
        cur.executemany("""
            INSERT INTO market_bars(symbol, ts, open, high, low, close, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (symbol, ts) DO UPDATE SET close=EXCLUDED.close;
        """, [(symbol, ts, float(p), float(p), float(p), float(p), 0) for ts, p in rows])
        cn.commit()

# ---------------- Crypto (CoinGecko, no key) ----------------
@st.cache_data(ttl=60)
def coingecko_series(asset_id: str, days: int):
    url = f"https://api.coingecko.com/api/v3/coins/{asset_id}/market_chart"
    r = requests.get(
        url,
        params={"vs_currency": "usd", "days": str(days)},
        headers={"User-Agent": f"MarketAI/1.0 ({UA_EMAIL})"},
        timeout=20,
    )
    r.raise_for_status()
    data = r.json().get("prices", [])
    df = pd.DataFrame(data, columns=["ts_ms", "price"])
    df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms")
    return df[["ts", "price"]]

# ---------------- Stocks (Finnhub) ----------------
@st.cache_data(ttl=30)
def finnhub_quote(symbol: str):
    if not FINNHUB_KEY: return None
    r = requests.get(
        "https://finnhub.io/api/v1/quote",
        params={"symbol": symbol.upper(), "token": FINNHUB_KEY},
        headers={"User-Agent": f"MarketAI/1.0 ({UA_EMAIL})"},
        timeout=10,
    )
    if not r.ok: return None
    j = r.json()
    return {
        "price": float(j.get("c", 0.0)),
        "prev_close": float(j.get("pc", 0.0)),
        "time": datetime.utcfromtimestamp(j.get("t", 0)) if j.get("t") else None,
    }

@st.cache_data(ttl=60)
def finnhub_candles(symbol: str, days: int):
    if not FINNHUB_KEY: return None
    now = int(datetime.utcnow().timestamp())
    frm = int((datetime.utcnow() - timedelta(days=days)).timestamp())
    res = "5" if days <= 7 else "60"  # 5-min for <=7d, hourly otherwise
    r = requests.get(
        "https://finnhub.io/api/v1/stock/candle",
        params={"symbol": symbol.upper(), "resolution": res, "from": frm, "to": now, "token": FINNHUB_KEY},
        headers={"User-Agent": f"MarketAI/1.0 ({UA_EMAIL})"},
        timeout=20,
    )
    j = r.json()
    if j.get("s") != "ok": return None
    df = pd.DataFrame({"ts": [datetime.utcfromtimestamp(t) for t in j["t"]], "price": j["c"]})
    return df

@st.cache_data(ttl=3600)
def finnhub_company_news(symbol: str):
    if not FINNHUB_KEY: return None
    to = datetime.utcnow().date()
    frm = to - timedelta(days=7)
    r = requests.get(
        "https://finnhub.io/api/v1/company-news",
        params={"symbol": symbol.upper(), "from": frm.isoformat(), "to": to.isoformat(), "token": FINNHUB_KEY},
        headers={"User-Agent": f"MarketAI/1.0 ({UA_EMAIL})"},
        timeout=20,
    )
    if not r.ok: return None
    items = r.json()[:5]
    out = []
    for i in items:
        out.append({
            "dt": datetime.utcfromtimestamp(i["datetime"]).strftime("%Y-%m-%d") if i.get("datetime") else "",
            "headline": i.get("headline"),
            "url": i.get("url"),
        })
    return out

# ---------------- Macro (FRED) ----------------
@st.cache_data(ttl=3600)
def fred_series(series_id: str):
    if not FRED_KEY: return None
    r = requests.get(
        "https://api.stlouisfed.org/fred/series/observations",
        params={"series_id": series_id, "file_type": "json", "api_key": FRED_KEY},
        headers={"User-Agent": f"MarketAI/1.0 ({UA_EMAIL})"},
        timeout=20,
    )
    if not r.ok: return None
    df = pd.DataFrame(r.json()["observations"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"])
    return df[["date", "value"]].dropna()

@st.cache_data(ttl=3600)
def fred_cpi_yoy():
    df = fred_series("CPIAUCSL")
    if df is None: return None
    df["yoy"] = df["value"].pct_change(12) * 100
    return df[["date", "yoy"]].dropna()

# ---------------- Institutional: BTC ETF flows (Farside) ----------------
@st.cache_data(ttl=60 * 30)  # 30 min
def btc_etf_flows():
    url = "https://www.farside.co.uk/bitcoin-etf-flows"
    headers = {"User-Agent": f"MarketAI/1.0 ({UA_EMAIL})"}
    r = httpx.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table: return None

    rows = []
    for tr in table.find_all("tr"):
        cols = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
        if len(cols) < 3 or "fund" in cols[0].lower():  # skip header
            continue
        rows.append(cols)

    parsed = []
    for cols in rows:
        try:
            fund, date_s, flow_s = cols[0], cols[1], cols[2]
            flow = float(flow_s.replace("$", "").replace("m", "").replace(",", ""))
            parsed.append({"fund": fund, "date": date_s, "flow_musd": flow})
        except Exception:
            continue
    return parsed[:60]

# ---------------- Institutional: SEC search ----------------
@st.cache_data(ttl=60 * 10)  # 10 min
def sec_search(query="bitcoin OR digital asset", forms=("8-K", "13F-HR", "13D", "13G"), size=20):
    url = "https://efts.sec.gov/LATEST/search-index"
    headers = {
        "User-Agent": f"MarketAI/1.0 ({UA_EMAIL})",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    form_filters = " OR ".join([f'formType:"{f}"' for f in forms])
    q = f'({query}) AND ({form_filters})'
    payload = {"keys": q, "category": "custom", "from": 0, "size": size, "sort": [{"filedAt": {"order": "desc"}}]}
    try:
        resp = httpx.post(url, headers=headers, json=payload, timeout=20)
        resp.raise_for_status()
        hits = resp.json().get("hits", {}).get("hits", [])
        out = []
        for h in hits:
            s = h.get("_source", {})
            link = s.get("link")
            out.append({
                "form": s.get("formType"),
                "company": s.get("displayNames", [""])[0] if s.get("displayNames") else s.get("companyName"),
                "filedAt": s.get("filedAt"),
                "title": s.get("displayTitle") or s.get("documentDescription"),
                "link": f'https://www.sec.gov/ixviewer/doc?action=display&source=content&doc={link}' if link else None,
            })
        return out
    except Exception:
        return None

# ---------------- UI ----------------
tab1, tab2, tab3, tab4 = st.tabs(["Crypto", "Stocks & Macro", "AI Summary", "Institutional"])

with tab1:
    left, right = st.columns([2, 1])
    with left:
        asset = st.selectbox("Crypto asset", ["bitcoin", "ethereum"], index=0)
    with right:
        days_c = st.select_slider("Days", options=[1, 3, 7, 14, 30], value=7)

    dfc = coingecko_series(asset, days_c)
    if not dfc.empty:
        latest = float(dfc["price"].iloc[-1]); prev = float(dfc["price"].iloc[0])
        chg = (latest - prev) / prev * 100 if prev else 0.0
        k1, k2 = st.columns(2)
        k1.metric(asset.capitalize(), f"${latest:,.2f}", f"{chg:+.2f}% in {days_c}d")
        k2.metric("Points", f"{len(dfc)}")
        st.line_chart(dfc.set_index("ts")["price"])
        st.caption("Source: CoinGecko (cached 60s)")

        # Save last points to DB quietly
        try:
            ensure_table()
            recent = [(row.ts, float(row.price)) for row in dfc.tail(30).itertuples(index=False)]
            save_rows(asset.upper()[:3], recent)
            if USE_DB: st.caption("DB: recent crypto rows saved ✅")
        except Exception as e:
            st.caption(f"DB note: {e}")

with tab2:
    st.subheader("Stocks")
    tick = st.text_input("Ticker (e.g., SPY, AAPL, TSLA)", value="SPY").upper().strip()
    days_s = st.select_slider("Window", options=[1, 3, 7, 14, 30, 60, 90], value=7)

    q = finnhub_quote(tick)
    c = finnhub_candles(tick, days_s)

    colA, colB = st.columns(2)
    if q:
        pct = ((q["price"] - q["prev_close"]) / q["prev_close"] * 100) if q["prev_close"] else 0.0
        colA.metric(f"{tick} price", f"${q['price']:.2f}", f"{pct:+.2f}% vs prev close")
        if q["time"]:
            colB.write(f"Time (UTC): {q['time']}")
    else:
        st.caption("Add FINNHUB_KEY to enable stock quotes/candles.")

    if c is not None and not c.empty:
        st.line_chart(c.set_index("ts")["price"])
    else:
        st.caption("No candles (rate limit or bad ticker).")

    news = finnhub_company_news(tick)
    if news:
        st.write("Latest headlines:")
        for n in news:
            st.write(f"- {n['dt']} — [{n['headline']}]({n['url']})")

    st.subheader("Macro")
    cpi = fred_cpi_yoy()
    dgs10 = fred_series("DGS10")
    dgs2  = fred_series("DGS2")
    unemp = fred_series("UNRATE")

    met1, met2, met3 = st.columns(3)
    if cpi is not None and len(cpi):
        met1.metric("US CPI YoY", f"{cpi['yoy'].iloc[-1]:.2f}%")
    else:
        met1.caption("Add FRED_KEY for CPI.")
    if dgs10 is not None and len(dgs10) and dgs2 is not None and len(dgs2):
        spread = float(dgs10['value'].iloc[-1] - dgs2['value'].iloc[-1])
        met2.metric("2s10s Spread", f"{spread:.2f} pp")
    if unemp is not None and len(unemp):
        met3.metric("Unemployment", f"{unemp['value'].iloc[-1]:.2f}%")
    if dgs10 is not None and len(dgs10):
        st.line_chart(dgs10.tail(180).set_index("date")["value"])
        st.caption("10Y Treasury yield (last ~6 months)")

with tab3:
    if OPENAI_KEY:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_KEY)
            brief = coingecko_series("bitcoin", 7).tail(40).to_dict(orient="records")
            prompt = (
                "Give 5 concise bullets on crypto & equities today, using these BTC time/price points: "
                f"{brief}. Mention CPI trend, yield curve slope, and any effect on risk assets. "
                "Be simple and neutral."
            )
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
            )
            st.write(resp.choices[0].message.content)
        except Exception as e:
            st.caption(f"AI summary unavailable: {e}")
    else:
        st.caption("Tip: add OPENAI_API_KEY in Railway → Variables to enable AI summaries.")

with tab4:
    st.subheader("BTC Spot ETF flows (daily)")
    flows = btc_etf_flows()
    if flows:
        try:
            last_date = flows[0]["date"]
            day_total = sum(x["flow_musd"] for x in flows if x["date"] == last_date)
            st.metric(f"Net flow on {last_date}", f"${day_total:.1f}M")
        except Exception:
            pass
        df_flows = pd.DataFrame(flows)
        st.dataframe(df_flows.head(20))
        st.caption("Source: Farside (scraped; cached 30 min)")
    else:
        st.caption("Couldn’t read the Farside table (layout may have changed).")

    st.markdown("---")
    st.subheader("Recent SEC filings mentioning crypto")
    qtext = st.text_input("Keyword(s)", value="bitcoin OR digital asset")
    forms = st.multiselect("Forms", options=["8-K", "13F-HR", "13D", "13G"], default=["8-K", "13F-HR"])
    if st.button("Scan SEC", use_container_width=True):
        results = sec_search(qtext, tuple(forms))
        if results:
            for r in results[:15]:
                filed = r["filedAt"][:10] if r.get("filedAt") else ""
                company = r.get("company") or ""
                title = r.get("title") or ""
                link = r.get("link")
                if link:
                    st.write(f"- **{filed} · {r['form']}** — {company}: [{title}]({link})")
                else:
                    st.write(f"- **{filed} · {r['form']}** — {company}: {title}")
            st.caption("Source: SEC full-text search (cached 10 min)")
        else:
            st.caption("No results or rate-limited. Try simpler keywords.")
