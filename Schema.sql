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
