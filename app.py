import os, requests, pandas as pd, streamlit as st
from datetime import datetime, timedelta

# ---------- Config ----------
REFRESH_MS = int(os.getenv("REFRESH_MS", "60000"))  # page auto-refresh
USE_DB = bool(os.getenv("DATABASE_URL"))
USE_OPENAI = bool(os.getenv("OPENAI_API_KEY"))
FINNHUB_KEY = os.getenv("FINNHUB_KEY")  # optional (stocks)
FRED_KEY = os.getenv("FRED_KEY")        # optional (macro)

if USE_DB:
    import psycopg2

st.set_page_config(page_title="Market AI", page_icon="📈", layout="centered")
st.markdown(
    "<h1 style='margin-bottom:0'>📈 Market AI</h1>"
    "<p style='color:#9aa0a6;margin-top:2px'>Phone-friendly market dashboard</p>",
    unsafe_allow_html=True,
)
st.markdown(
    f"<script>setTimeout(()=>window.location.reload(), {REFRESH_MS});</script>",
    unsafe_allow_html=True,
)

# ---------- DB helpers ----------
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
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bars_symbol_ts ON market_bars(symbol, ts);
        """); cn.commit()

def save_rows(symbol, rows):  # rows [(ts, price)]
    if not USE_DB or not rows: return
    with db_conn() as cn, cn.cursor() as cur:
        cur.executemany("""
            INSERT INTO market_bars(symbol, ts, open, high, low, close, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (symbol, ts) DO UPDATE SET close=EXCLUDED.close;
        """, [(symbol, ts, p, p, p, p, 0) for ts, p in rows])
        cn.commit()

# ---------- Crypto (no key) ----------
@st.cache_data(ttl=60)
def coingecko_series(asset_id: str, days: int):
    url = f"https://api.coingecko.com/api/v3/coins/{asset_id}/market_chart"
    r = requests.get(url, params={"vs_currency":"usd","days":str(days)}, timeout=20)
    r.raise_for_status()
    data = r.json().get("prices", [])
    df = pd.DataFrame(data, columns=["ts_ms","price"])
    df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms")
    return df[["ts","price"]]

# ---------- Stocks (Finnhub) ----------
@st.cache_data(ttl=30)
def finnhub_quote(symbol: str, key: str):
    if not key: return None
    r = requests.get("https://finnhub.io/api/v1/quote",
                     params={"symbol":symbol.upper(),"token":key}, timeout=10)
    if not r.ok: return None
    j = r.json()
    return {
        "price": float(j.get("c", 0.0)),
        "prev_close": float(j.get("pc", 0.0)),
        "time": datetime.utcfromtimestamp(j.get("t", 0))
    }

@st.cache_data(ttl=60)
def finnhub_candles(symbol: str, days: int, key: str):
    """Minute/hour candles depending on days; returns df with ts, close."""
    if not key: return None
    now = int(datetime.utcnow().timestamp())
    frm = int((datetime.utcnow() - timedelta(days=days)).timestamp())
    # resolution: 5 = 5-min for <= 7d; 60 = hourly for >7d
    res = "5" if days <= 7 else "60"
    r = requests.get("https://finnhub.io/api/v1/stock/candle",
                     params={"symbol":symbol.upper(),"resolution":res,"from":frm,"to":now,"token":key},
                     timeout=20)
    j = r.json()
    if j.get("s") != "ok": return None
    df = pd.DataFrame({"ts":[datetime.utcfromtimestamp(t) for t in j["t"]], "price": j["c"]})
    return df

@st.cache_data(ttl=3600)
def finnhub_company_news(symbol: str, key: str):
    if not key: return None
    to = datetime.utcnow().date()
    frm = to - timedelta(days=7)
    r = requests.get("https://finnhub.io/api/v1/company-news",
                     params={"symbol":symbol.upper(),"from":frm.isoformat(),"to":to.isoformat(),"token":key},
                     timeout=20)
    if not r.ok: return None
    items = r.json()[:5]
    return [{"dt": i.get("datetime"), "headline": i.get("headline"), "url": i.get("url")} for i in items]

# ---------- Macro (FRED) ----------
@st.cache_data(ttl=3600)
def fred_series(series_id: str, key: str):
    if not key: return None
    r = requests.get("https://api.stlouisfed.org/fred/series/observations",
                     params={"series_id":series_id,"file_type":"json","api_key":key}, timeout=20)
    if not r.ok: return None
    df = pd.DataFrame(r.json()["observations"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"])
    return df[["date","value"]].dropna()

@st.cache_data(ttl=3600)
def fred_cpi_yoy(key: str):
    df = fred_series("CPIAUCSL", key)
    if df is None: return None
    df["yoy"] = df["value"].pct_change(12) * 100
    return df[["date","yoy"]].dropna()

# ---------- UI ----------
tab1, tab2, tab3 = st.tabs(["Crypto", "Stocks & Macro", "AI Summary"])

with tab1:
    left, right = st.columns([2,1])
    with left:
        asset = st.selectbox("Crypto asset", ["bitcoin","ethereum"], index=0)
    with right:
        days_c = st.select_slider("Days", options=[1,3,7,14,30], value=7)

    dfc = coingecko_series(asset, days_c)
    if not dfc.empty:
        latest = float(dfc["price"].iloc[-1]); prev = float(dfc["price"].iloc[0])
        chg = (latest - prev) / prev * 100 if prev else 0.0
        k1, k2 = st.columns(2)
        k1.metric(asset.capitalize(), f"${latest:,.2f}", f"{chg:+.2f}% in {days_c}d")
        k2.metric("Points", f"{len(dfc)}")
        st.line_chart(dfc.set_index("ts")["price"], height=220)
        st.caption("Source: CoinGecko (cached 60s)")

        # save last points to DB (quiet)
        try:
            ensure_table()
            recent = [(row.ts, float(row.price)) for row in dfc.tail(30).itertuples(index=False)]
            save_rows(asset.upper()[:3], recent)
            if USE_DB: st.caption("DB: recent crypto rows saved ✅")
        except Exception as e:
            st.caption(f"DB note: {e}")

with tab2:
    # ---- Stocks
    st.subheader("Stocks")
    tick = st.text_input("Ticker (e.g., SPY, AAPL, TSLA)", value="SPY").upper().strip()
    days_s = st.select_slider("Window", options=[1,3,7,14,30,60,90], value=7)

    q = finnhub_quote(tick, FINNHUB_KEY)
    c = finnhub_candles(tick, days_s, FINNHUB_KEY)

    colA, colB, colC = st.columns(3)
    if q:
        pct = ((q["price"] - q["prev_close"]) / q["prev_close"] * 100) if q["prev_close"] else 0.0
        colA.metric(f"{tick} price", f"${q['price']:.2f}", f"{pct:+.2f}% vs prev close")
        colB.write(f"Time (UTC): {q['time']}")
    else:
        colA.caption("Add FINNHUB_KEY to enable stock quotes.")

    if c is not None and not c.empty:
        st.line_chart(c.set_index("ts")["price"], height=220)
    else:
        st.caption("No candles (rate limit or bad ticker).")

    news = finnhub_company_news(tick, FINNHUB_KEY)
    if news:
        st.write("Latest headlines:")
        for n in news:
            ts = datetime.utcfromtimestamp(n["dt"]).strftime("%Y-%m-%d") if n["dt"] else ""
            st.write(f"- {ts} – [{n['headline']}]({n['url']})")

    # ---- Macro
    st.subheader("Macro")
    cpi = fred_cpi_yoy(FRED_KEY)
    dgs10 = fred_series("DGS10", FRED_KEY)   # 10Y Treasury yield
    dgs2  = fred_series("DGS2", FRED_KEY)    # 2Y Treasury yield
    unemp = fred_series("UNRATE", FRED_KEY)  # Unemployment rate

    met1, met2, met3 = st.columns(3)
    if cpi is not None and len(cpi):
        met1.metric("US CPI YoY", f"{cpi['yoy'].iloc[-1]:.2f}%")
    else:
        met1.caption("Add FRED_KEY for CPI.")

    if dgs10 is not None and len(dgs10) and dgs2 is not None and len(dgs2):
        s = float(dgs10["value"].iloc[-1] - dgs2["value"].iloc[-1])
        met2.metric("2s10s Spread", f"{s:.2f} pp")
    if unemp is not None and len(unemp):
        met3.metric("Unemployment", f"{unemp['value'].iloc[-1]:.2f}%")

    # small chart for yields if available
    if dgs10 is not None and len(dgs10):
        d = dgs10.tail(180).set_index("date")["value"]
        st.line_chart(d, height=160)
        st.caption("10Y Treasury yield (last ~6 months)")

with tab3:
    if USE_OPENAI:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            brief = coingecko_series("bitcoin", 7).tail(40).to_dict(orient="records")
            prompt = (
                "Give 5 concise bullets on crypto & equities today, using these BTC data points: "
                f"{brief}. Mention CPI trend, yield curve slope, and any effect on risk assets. "
                "Be simple and neutral."
            )
            resp = client.chat.completions.create(
                model="gpt-4o-mini", messages=[{"role":"user","content":prompt}], temperature=0.3)
            st.write(resp.choices[0].message.content)
        except Exception as e:
            st.caption(f"AI summary unavailable: {e}")
    else:
        st.caption("Tip: add OPENAI_API_KEY in Railway → Variables to enable AI summaries.")

st.caption("Auto-refreshes every 60s. Use tabs above.")
