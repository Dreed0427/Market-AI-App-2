import os
import requests
import pandas as pd
import streamlit as st

# --- Optional DB (safe if missing) ---
USE_DB = bool(os.getenv("DATABASE_URL"))
if USE_DB:
    import psycopg2

st.set_page_config(page_title="Market AI", page_icon="📈", layout="centered")
st.autorefresh(interval=60_000, key="refresh")  # refresh UI every 60s
st.title("📈 Market AI – Phone Dashboard")

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

def save_rows(symbol, rows):
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

# ---------- Data (no key needed) ----------
@st.cache_data(ttl=60)
def coingecko_series(asset_id: str, days: int):
    url = f"https://api.coingecko.com/api/v3/coins/{asset_id}/market_chart"
    r = requests.get(url, params={"vs_currency": "usd", "days": str(days)}, timeout=20)
    r.raise_for_status()
    data = r.json().get("prices", [])
    df = pd.DataFrame(data, columns=["ts_ms", "price"])
    df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms")
    return df[["ts", "price"]]

# ---------- UI ----------
asset = st.selectbox("Asset", ["bitcoin", "ethereum"])
days = st.slider("Days", 1, 30, 7)

df = coingecko_series(asset, days)
st.line_chart(df.set_index("ts")["price"])
st.dataframe(df.tail(10))

# Persist last points to DB (optional)
try:
    ensure_table()
    # keep last ~30 points
    recent = [(row.ts, float(row.price)) for row in df.tail(30).itertuples(index=False)]
    save_rows(asset.upper()[:3], recent)
    if USE_DB:
        st.caption("DB: recent rows saved ✅")
except Exception as e:
    st.caption(f"DB note: {e}")

# ---------- Optional AI summary ----------
api_key = os.getenv("OPENAI_API_KEY")
if api_key:
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        brief = df.tail(40).to_dict(orient="records")
        prompt = (
            f"Explain in 5 short bullets what happened to {asset} prices "
            f"in the last {days} days using ONLY this time/price data: {brief}"
        )
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        st.subheader("AI summary")
        st.write(resp.choices[0].message.content)
    except Exception as e:
        st.caption(f"AI summary unavailable: {e}")
else:
    st.caption("Tip: add OPENAI_API_KEY in Railway → Variables to enable AI summaries.")
