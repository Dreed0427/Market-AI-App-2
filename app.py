import os, time, requests, pandas as pd
import streamlit as st

# --- Optional: DB (won't crash if not set) ---
USE_DB = all(os.getenv(k) for k in ["PGHOST","PGUSER","PGPASSWORD","PGDATABASE"])
if USE_DB:
    import os, psycopg2
    def db_conn():
        return psycopg2.connect(os.getenv("DATABASE_URL"))
            host=os.getenv("PGHOST"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
            dbname=os.getenv("PGDATABASE"),
            port=os.getenv("PGPORT", "5432"),
            connect_timeout=10,
        )
    def create_table():
        with db_conn() as cn, cn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS market_bars(
                id SERIAL PRIMARY KEY,
                symbol TEXT, ts TIMESTAMP,
                open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume NUMERIC
            );
            """); cn.commit()
    create_table()

st.set_page_config(page_title="Market AI", page_icon="📈", layout="centered")
st.title("📈 Market AI (Railway)")

asset = st.selectbox("Asset", ["bitcoin","ethereum"])
days = st.slider("Days", 1, 30, 7)

# --- No key required: CoinGecko snapshot ---
@st.cache_data(ttl=60)
def pull_coingecko(asset_id, days):
    url = f"https://api.coingecko.com/api/v3/coins/{asset_id}/market_chart"
    r = requests.get(url, params={"vs_currency":"usd","days":str(days)}, timeout=20)
    r.raise_for_status()
    prices = r.json().get("prices", [])
    df = pd.DataFrame(prices, columns=["ts_ms","price"])
    df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms")
    return df[["ts","price"]]

df = pull_coingecko(asset, days)
st.line_chart(df.set_index("ts")["price"])
st.dataframe(df.tail(10))

# --- Save a few rows to DB if configured ---
if USE_DB and not df.empty:
    try:
        with db_conn() as cn, cn.cursor() as cur:
            rows = list(df.tail(20).itertuples(index=False, name=None))
            for ts, price in rows:
                cur.execute(
                    "INSERT INTO market_bars(symbol, ts, open, high, low, close, volume) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;",
                    (asset.upper()[:3], ts, price, price, price, price, 0),
                )
            cn.commit()
        st.caption("Saved recent rows to Postgres ✅")
    except Exception as e:
        st.caption(f"DB write skipped: {e}")

# --- Optional AI summary (requires OPENAI_API_KEY) ---
if os.getenv("OPENAI_API_KEY"):
    try:
        from openai import OpenAI
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        brief = df.tail(30).to_dict(orient="records")
        prompt = f"Explain in 5 short bullets what happened to {asset} prices in the last {days} days using ONLY this data: {brief}"
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":prompt}],
            temperature=0.3,
        )
        st.subheader("AI summary")
        st.write(resp.choices[0].message.content)
    except Exception as e:
        st.caption(f"AI summary unavailable: {e}")
else:
    st.caption("Add OPENAI_API_KEY in Railway Variables for AI summaries.")
