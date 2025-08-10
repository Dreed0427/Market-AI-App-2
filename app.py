import os
from datetime import datetime, timedelta

import pandas as pd
import requests
import streamlit as st
import httpx
from bs4 import BeautifulSoup

# ========= config =========
REFRESH_MS = int(os.getenv("REFRESH_MS", "60000"))
DATABASE_URL = os.getenv("DATABASE_URL")
OPENAI_KEY   = os.getenv("OPENAI_API_KEY")    # optional
FINNHUB_KEY  = os.getenv("FINNHUB_KEY")       # optional
FRED_KEY     = os.getenv("FRED_KEY")          # optional
WEBHOOK_URL  = os.getenv("WEBHOOK_URL")       # optional (Slack/Discord)
UA_EMAIL     = "priestndgo@gmail.com"

USE_DB = bool(DATABASE_URL)
if USE_DB:
    import psycopg2

st.set_page_config(page_title="Market AI", page_icon="📈", layout="centered")
st.markdown(
    "<h1 style='margin-bottom:0'>📈 Market AI — Phone Dashboard</h1>"
    "<p style='color:#444;margin-top:2px'>Crypto · Stocks · Macro · Institutional · Health</p>",
    unsafe_allow_html=True,
)
st.markdown(f"<script>setTimeout(()=>window.location.reload(), {REFRESH_MS});</script>", unsafe_allow_html=True)

# ========= db helpers =========
def db_conn():
    if not USE_DB: return None
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)

# ========= crypto (coingecko) =========
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

# ========= stocks (finnhub) =========
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
    res = "5" if days <= 7 else "60"
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

# ========= macro (fred) =========
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

# ========= institutional (robust ETF + SEC) =========
@st.cache_data(ttl=60 * 30)
def btc_etf_flows_safe():
    """Never crash the UI. Try soup → fallback read_html → else empty list."""
    url = "https://www.farside.co.uk/bitcoin-etf-flows"
    headers = {"User-Agent": f"MarketAI/1.0 ({UA_EMAIL})"}
    try:
        r = httpx.get(url, headers=headers, timeout=20, follow_redirects=True)
        if r.status_code != 200 or not r.text:
            return []
        # First pass: soup
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if table:
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
            if rows:
                return rows[:60]
        # Fallback: pandas
        try:
            tables = pd.read_html(r.text)
            if tables:
                df = next((t for t in tables if t.shape[1] >= 3), None)
                if df is not None:
                    rows = []
                    for _, rr in df.iloc[:, :3].iterrows():
                        vals = [str(x) for x in rr.tolist()]
                        fund, date_s, flow_s = vals[0], vals[1], vals[2]
                        if "fund" in fund.lower():  # skip headers
                            continue
                        try:
                            flow = float(flow_s.replace("$", "").replace("m", "").replace(",", ""))
                            rows.append({"fund": fund, "date": date_s, "flow_musd": flow})
                        except Exception:
                            continue
                    return rows[:60]
        except Exception:
            pass
    except Exception:
        pass
    return []

@st.cache_data(ttl=60 * 10)
def sec_search_live(query="bitcoin OR digital asset", forms=("8-K", "13F-HR", "13D", "13G"), size=20):
    url = "https://efts.sec.gov/LATEST/search-index"
    headers = {
        "User-Agent": f"MarketAI/1.0 ({UA_EMAIL})",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    ff = " OR ".join([f'formType:"{f}"' for f in forms]) if forms else ""
    q = f'({query}) AND ({ff})' if ff else f'({query})'
    payload = {"keys": q, "category": "custom", "from": 0, "size": size, "sort": [{"filedAt": {"order": "desc"}}]}
    try:
        resp = httpx.post(url, headers=headers, json=payload, timeout=20)
        if resp.status_code != 200:
            return []
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
        return []

# ========= ui =========
tab1, tab2, tab3, tab4, tab5 = st.tabs(["Crypto", "Stocks & Macro", "AI", "Institutional", "Health"])

# -- Crypto
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
    else:
        st.warning("No data from CoinGecko.")

# -- Stocks & Macro
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
        st.info("Add FINNHUB_KEY in Variables to enable stock quotes/candles.")

    if c is not None and not c.empty:
        st.line_chart(c.set_index("ts")["price"])
    else:
        st.caption("No candles (rate limit or bad ticker).")

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

# -- AI
with tab3:
    if OPENAI_KEY:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_KEY)
            brief = coingecko_series("bitcoin", 7).tail(40).to_dict(orient="records")
            prompt = (
                "Give 5 concise bullets on crypto & equities today, using these BTC time/price points: "
                f"{brief}. Mention CPI trend, yield curve slope, and any effect on risk assets. "
                "Keep it neutral, punchy, and clear."
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
        st.caption("Tip: add OPENAI_API_KEY in Variables to enable AI summaries.")

# -- Institutional
with tab4:
    st.subheader("BTC Spot ETF flows (daily)")
    # DB view (from worker)
    if USE_DB:
        try:
            with db_conn() as cn:
                df_db = pd.read_sql_query(
                    "SELECT date, fund, flow_musd FROM etf_flows ORDER BY date DESC, fund ASC LIMIT 60", cn
                )
            if len(df_db):
                last_date = df_db["date"].iloc[0]
                day_total = float(df_db[df_db["date"] == last_date]["flow_musd"].sum())
                st.metric(f"Net flow on {last_date}", f"${day_total:.1f}M")
                st.dataframe(df_db.head(30))
                st.caption("From DB (persisted).")
            else:
                st.caption("No ETF flows in DB yet.")
        except Exception as e:
            st.caption(f"DB read issue: {e}")

    # Live snapshot (safe)
    live = btc_etf_flows_safe()
    if live:
        st.write("Latest (live):")
        st.dataframe(pd.DataFrame(live).head(15))
        st.caption("Live source: Farside (cached 30 min).")
    else:
        st.caption("Could not fetch the live flows right now (rate limit or site layout change).")

    st.markdown("---")
    st.subheader("Recent SEC filings mentioning crypto")
    if USE_DB:
        try:
            with db_conn() as cn:
                df_fil = pd.read_sql_query(
                    "SELECT filed_at, form, company, title, link FROM sec_filings "
                    "ORDER BY filed_at DESC NULLS LAST LIMIT 25", cn
                )
            if len(df_fil):
                for _, r in df_fil.iterrows():
                    filed = str(r["filed_at"])[:10] if r["filed_at"] else ""
                    if r["link"]:
                        st.write(f"- **{filed} · {r['form']}** — {r['company']}: [{r['title']}]({r['link']})")
                    else:
                        st.write(f"- **{filed} · {r['form']}** — {r['company']}: {r['title']}")
                st.caption("From DB (persisted).")
            else:
                st.caption("No filings in DB yet.")
        except Exception as e:
            st.caption(f"DB read issue: {e}")

    qtext = st.text_input("Live scan keywords", value="bitcoin OR digital asset")
    forms = st.multiselect("Forms", options=["8-K", "13F-HR", "13D", "13G"], default=["8-K", "13F-HR"])
    if st.button("Scan SEC now", use_container_width=True):
        results = sec_search_live(qtext, tuple(forms))
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
            st.caption("Live from SEC (cached ~10 min).")
        else:
            st.caption("No results or rate-limited. Try simpler keywords.")

# -- Health
with tab5:
    st.subheader("Service Health")
    col1, col2 = st.columns(2)

    # env presence
    with col1:
        st.write("**Environment keys**")
        st.write(f"- DATABASE_URL: {'✅' if USE_DB else '❌'}")
        st.write(f"- FINNHUB_KEY: {'✅' if FINNHUB_KEY else '❌'}")
        st.write(f"- FRED_KEY: {'✅' if FRED_KEY else '❌'}")
        st.write(f"- OPENAI_API_KEY: {'✅' if OPENAI_KEY else '❌'}")
        st.write(f"- WEBHOOK_URL: {'✅' if WEBHOOK_URL else '❌'}")

    # db stats
    with col2:
        if USE_DB:
            try:
                with db_conn() as cn:
                    bars = pd.read_sql_query("SELECT COUNT(*) n FROM market_bars", cn)["n"].iloc[0]
                    etf  = pd.read_sql_query("SELECT COUNT(*) n FROM etf_flows", cn)["n"].iloc[0]
                    sec  = pd.read_sql_query("SELECT COUNT(*) n FROM sec_filings", cn)["n"].iloc[0]
                    last_alert = pd.read_sql_query(
                        "SELECT ts, kind FROM alerts ORDER BY ts DESC LIMIT 1", cn
                    )
                st.write("**Database rows**")
                st.write(f"- market_bars: {int(bars)}")
                st.write(f"- etf_flows: {int(etf)}")
                st.write(f"- sec_filings: {int(sec)}")
                if len(last_alert):
                    st.write(f"- last alert: {last_alert['ts'].iloc[0]} ({last_alert['kind'].iloc[0]})")
            except Exception as e:
                st.warning(f"DB check failed: {e}")
        else:
            st.caption("No database connected.")

    st.markdown("---")
    st.subheader("Send test webhook (optional)")
    if WEBHOOK_URL:
        if st.button("Send test ping"):
            try:
                httpx.post(WEBHOOK_URL, json={"text": "Market AI: test webhook from dashboard"}, timeout=10)
                st.success("Sent!")
            except Exception as e:
                st.error(f"Failed: {e}")
    else:
        st.caption("Add WEBHOOK_URL in Variables to enable test pings.")
