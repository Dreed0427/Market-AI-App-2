import os, requests, pandas as pd, streamlit as st
from datetime import datetime

# ---------- Settings ----------
REFRESH_MS = int(os.getenv("REFRESH_MS", "60000"))  # auto refresh every 60s

# ---------- Optional services ----------
USE_DB = bool(os.getenv("DATABASE_URL"))
USE_OPENAI = bool(os.getenv("OPENAI_API_KEY"))
FINNHUB_KEY = os.getenv("FINNHUB_KEY")        # optional (SPY)
FRED_KEY = os.getenv("FRED_KEY")              # optional (CPI)

if USE_DB:
    import psycopg2

st.set_page_config(page_title="Market AI", page_icon="📈", layout="centered")
st.title("📈 Market AI – Phone Dashboard")

# Auto-refresh (simple + mobile friendly)
st.markdown(
    f"""
    <script>
      setTimeout(function(){{ window.location.reload(); }}, {REFRESH_MS});
    </script>
    """,
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
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bars_symbol_ts
          ON market_bars(symbol, ts);
        """)
        cn.commit()

def save_rows(symbol, rows):  # rows = list[(ts, price)]
    if not USE_DB or not rows:
        return
    with db_conn() as cn, cn.cursor() as cur:
        for ts, price in rows:
            cur.execute("""
            INSERT INTO market_bars(symbol, ts, open, high, low, close, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (symbol, ts) DO UPDATE SET close=EXCLUDED.close;
            """, (symbol, ts, price, price, price, price, 0))
        cn.commit()

# ---------- Data fetchers ----------
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
def finnhub_spy_quote():
    if not FINNHUB_KEY:
        return None
    r = requests.get("https://finnhub.io/api/v1/quote",
                     params={"symbol": "SPY", "token": FINNHUB_KEY}, timeout=10)
    if not r.ok:
        return None
    j = r.json()
    return {"price": float(j.get("c", 0.0)), "time": datetime.utcfromtimestamp(j.get("t", 0))}

@st.cache_data(ttl=3600)
def fred_cpi_yoy():
    if not FRED_KEY:
        return None
    r = requests.get("https://api.stlouisfed.org/fred/series/observations",
                     params={"series_id": "CPIAUCSL", "file_type": "json", "api_key": FRED_KEY},
                     timeout=20)
    if not r.ok:
        return None
    obs = pd.DataFrame(r.json()["observations"])
    obs["value"] = pd.to_numeric(obs["value"], errors="coerce")
    obs["date"] = pd.to_datetime(obs["date"])
    obs["yoy"] = obs["value"].pct_change(12) * 100
    return obs[["date", "yoy"]].dropna()

# ---------- UI ----------
tab1, tab2, tab3 = st.tabs(["Crypto", "Stocks & Macro", "AI Summary"])

with tab1:
    asset = st.selectbox("Crypto asset", ["bitcoin", "ethereum"])
    days = st.slider("Days", 1, 30, 7)
    df = coingecko_series(asset, days)
    st.line_chart(df.set_index("ts")["price"])
    st.dataframe(df.tail(10))

    # Persist recent points to DB if configured
    try:
        ensure_table()
        recent = [(row.ts, float(row.price)) for row in df.tail(30).itertuples(index=False)]
        save_rows(asset.upper()[:3], recent)
        if USE_DB:
            st.caption("DB: recent crypto rows saved ✅")
    except Exception as e:
        st.caption(f"DB note: {e}")

with tab2:
    col1, col2 = st.columns(2)
    with col1:
        spy = finnhub_spy_quote()
        if spy:
            st.metric("SPY (approx. real-time)", f"${spy['price']:.2f}",
                      help=f"Time: {spy['time']} UTC")
        else:
            st.caption("Add FINNHUB_KEY to show SPY quote.")
    with col2:
        cpi = fred_cpi_yoy()
        if cpi is not None and len(cpi) > 0:
            st.metric("US CPI YoY (latest)", f"{cpi.iloc[-1]['yoy']:.2f}%")
        else:
            st.caption("Add FRED_KEY to show CPI YoY.")

with tab3:
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=api_key)
            brief = df.tail(40).to_dict(orient="records")
            prompt = (
                f"Explain in 5 short bullets what happened to {asset} prices "
                f"in the last {days} days using ONLY this time/price data: {brief}. "
                f"Then add one line on how current CPI (if provided) and SPY might relate."
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

st.caption("Auto-refreshes every 60s. Tap the tabs to switch views.")
