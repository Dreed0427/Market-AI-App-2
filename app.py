import os, requests, pandas as pd, streamlit as st
from datetime import datetime

# ---------- Config ----------
REFRESH_MS = int(os.getenv("REFRESH_MS", "60000"))
USE_DB = bool(os.getenv("DATABASE_URL"))
USE_OPENAI = bool(os.getenv("OPENAI_API_KEY"))
FINNHUB_KEY = os.getenv("FINNHUB_KEY")  # optional (SPY)
FRED_KEY = os.getenv("FRED_KEY")        # optional (CPI)

if USE_DB:
    import psycopg2

st.set_page_config(page_title="Market AI", page_icon="📈", layout="centered")

# Header
st.markdown(
    "<h1 style='margin-bottom:0'>📈 Market AI</h1>"
    "<p style='color:#9aa0a6;margin-top:2px'>Phone-friendly market dashboard</p>",
    unsafe_allow_html=True,
)

# Auto-refresh (every REFRESH_MS)
st.markdown(
    f"<script>setTimeout(()=>window.location.reload(), {REFRESH_MS});</script>",
    unsafe_allow_html=True,
)

# ---------- DB helpers ----------
def db_conn():
    if not USE_DB:
        return None
    return psycopg2.connect(os.getenv("DATABASE_URL"), connect_timeout=10)

def ensure_table():
    if not USE_DB:
        return
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

def save_rows(symbol, rows):  # rows list[(ts, price)]
    if not USE_DB or not rows:
        return
    with db_conn() as cn, cn.cursor() as cur:
        cur.executemany("""
            INSERT INTO market_bars(symbol, ts, open, high, low, close, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (symbol, ts) DO UPDATE SET close=EXCLUDED.close;
        """, [(symbol, ts, p, p, p, p, 0) for ts, p in rows])
        cn.commit()

# ---------- Fetchers ----------
@st.cache_data(ttl=60)
def coingecko_series(asset_id: str, days: int):
    url = f"https://api.coingecko.com/api/v3/coins/{asset_id}/market_chart"
    r = requests.get(url, params={"vs_currency": "usd", "days": str(days)}, timeout=20)
    r.raise_for_status()
    data = r.json().get("prices", [])
    df = pd.DataFrame(data, columns=["ts_ms", "price"])
    df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms")
    return df[["ts", "price"]]

@st.cache_data(ttl=60)
def finnhub_spy_quote(key: str):
    if not key: return None
    r = requests.get("https://finnhub.io/api/v1/quote",
                     params={"symbol": "SPY", "token": key}, timeout=10)
    if not r.ok: return None
    j = r.json()
    return {"price": float(j.get("c", 0.0)), "time": datetime.utcfromtimestamp(j.get("t", 0))}

@st.cache_data(ttl=3600)
def fred_cpi_yoy(key: str):
    if not key: return None
    r = requests.get("https://api.stlouisfed.org/fred/series/observations",
                     params={"series_id": "CPIAUCSL", "file_type": "json", "api_key": key}, timeout=20)
    if not r.ok: return None
    obs = pd.DataFrame(r.json()["observations"])
    obs["value"] = pd.to_numeric(obs["value"], errors="coerce")
    obs["date"] = pd.to_datetime(obs["date"])
    obs["yoy"] = obs["value"].pct_change(12) * 100
    obs = obs.dropna()
    return {"latest": float(obs["yoy"].iloc[-1]), "date": obs["date"].iloc[-1]}

# ---------- UI ----------
tab1, tab2, tab3 = st.tabs(["Crypto", "Stocks & Macro", "AI Summary"])

with tab1:
    left, right = st.columns([2,1])
    with left:
        asset = st.selectbox("Crypto asset", ["bitcoin", "ethereum"], index=0)
    with right:
        days = st.select_slider("Days", options=[1,3,7,14,30], value=7)

    df = coingecko_series(asset, days)
    latest = float(df["price"].iloc[-1]) if not df.empty else None
    prev = float(df["price"].iloc[0]) if len(df) else None
    chg = (latest - prev) / prev * 100 if latest and prev else 0.0

    k1, k2 = st.columns(2)
    k1.metric(f"{asset.capitalize()} price", f"${latest:,.2f}" if latest else "—",
              f"{chg:+.2f}% in {days}d" if latest else None)
    k2.metric("Points", f"{len(df)}")

    st.line_chart(df.set_index("ts")["price"], height=220)
    st.caption("Source: CoinGecko (cached 60s)")

    # Save to DB (quietly)
    try:
        ensure_table()
        recent = [(row.ts, float(row.price)) for row in df.tail(30).itertuples(index=False)]
        save_rows(asset.upper()[:3], recent)
        if USE_DB: st.caption("DB: recent rows saved ✅")
    except Exception as e:
        st.caption(f"DB note: {e}")

with tab2:
    c1, c2 = st.columns(2)
    spy = finnhub_spy_quote(FINNHUB_KEY)
    if spy:
        c1.metric("SPY (approx. real-time)", f"${spy['price']:.2f}", help=f"Time: {spy['time']} UTC")
    else:
        c1.caption("Add FINNHUB_KEY to show SPY.")

    cpi = fred_cpi_yoy(FRED_KEY)
    if cpi:
        c2.metric("US CPI YoY (latest)", f"{cpi['latest']:.2f}%", help=f"Date: {cpi['date'].date()}")
    else:
        c2.caption("Add FRED_KEY for CPI YoY.")

with tab3:
    if USE_OPENAI:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            brief = coingecko_series(asset, days).tail(40).to_dict(orient="records")
            prompt = (
                f"Explain in 5 short bullets what happened to {asset} prices "
                f"in the last {days} days using ONLY this time/price data: {brief}. "
                f"Then add one line connecting it to SPY and CPI (if provided)."
            )
            resp = client.chat.completions.create(
                model="gpt-4o-mini", messages=[{"role":"user","content":prompt}], temperature=0.3)
            st.write(resp.choices[0].message.content)
        except Exception as e:
            st.caption(f"AI summary unavailable: {e}")
    else:
        st.caption("Tip: add OPENAI_API_KEY in Railway → Variables to enable AI summaries.")

st.caption("Auto-refreshes every 60s. Use tabs above. ")
