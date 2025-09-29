# --- original imports ---
import requests
import os
from dotenv import load_dotenv
load_dotenv()
import csv
from datetime import datetime
import time



# --- ADDED import for Snowflake ---
from snowflake import connector

POLYGON_API_KEY=os.getenv('POLYGON_API_KEY')
LIMIT=1000
DS='2025-09-29'  # just an example date; change as needed

# --- minimal retry helper for Polygon rate limits ---
MAX_RETRIES = int(os.getenv("POLYGON_MAX_RETRIES", "6"))

def get_with_retry(url: str):
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, timeout=60)
        if resp.status_code == 429:
            # Respect server hint if present; fallback to ~12s (≈5 req/min)
            ra = resp.headers.get("Retry-After")
            try:
                sleep_s = max(12, int(ra)) if ra is not None else 12
            except ValueError:
                sleep_s = 12
            print(f"[{attempt}/{MAX_RETRIES}] 429 rate-limited. Sleeping {sleep_s}s…")
            time.sleep(sleep_s)
            continue
        resp.raise_for_status()
        return resp
    raise requests.HTTPError(f"Failed after {MAX_RETRIES} retries: {url}")

# --- your original function (unchanged except the final return) ---
def run_stock_job():
    DS = datetime.now().strftime('%Y-%m-%d')
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    
    # use helper instead of plain requests.get
    response = get_with_retry(url)
    tickers = []

    data = response.json()
    for ticker in data.get('results', []):
        ticker['ds'] = DS
        tickers.append(ticker)

    while 'next_url' in data:
        print('requesting next page', data['next_url'])
        response = get_with_retry(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        for ticker in data.get('results', []):
            ticker['ds'] = DS
            tickers.append(ticker)

    return tickers


# --- your original example (left as-is) ---
example_ticker={'ticker': 'HUM', 
'name': 'Humana Inc.', 
'market': 'stocks', 
'locale': 'us', 
'primary_exchange': 'XNYS', 
'type': 'CS', 
'active': True, 
'currency_name': 'usd', 
'cik': '0000049071', 
'composite_figi': 'BBG000BLKK03', 
'share_class_figi': 'BBG001S5S1X6', 
'last_updated_utc': '2025-09-16T06:05:51.697381223Z',
"ds":'2025-09-29'}

# --- your original CSV writer (left as-is; optional) ---
def run_stock_lob():
    fieldnames = list(example_ticker.keys())
    output_csv='ticker.csv'

    with open("tickers.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        # write everything you already collected
        for t in tickers:
            writer.writerow({k: t.get(k) for k in fieldnames})

# =========================
# ADDED: Snowflake uploader
# =========================

# table name (created unquoted → safest to use UPPERCASE)
SF_TABLE = os.getenv("SNOWFLAKE_TABLE") or "STOCK_TICKERS"

# column order must match your Snowflake table
SNOWFLAKE_COLS = [
    "ticker","name","market","locale","primary_exchange","type","active",
    "currency_name","cik","composite_figi","share_class_figi","last_updated_utc", "ds"
]

def load_to_snowflake(
    rows,
    table: str = None,            # defaults to env or STOCK_TICKERS
    fieldnames: list[str] = None, # order of columns to insert
    truncate: bool = False,       # True = TRUNCATE table first
    batch_size: int = 1000,
):
    if not rows:
        print("load_to_snowflake: no rows to insert.")
        return 0

    connect_kwargs = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),           # e.g. SZYDTVY-HG42632
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    }
    role = os.getenv("SNOWFLAKE_ROLE")
    if role:
        connect_kwargs["role"] = role

    table = table or os.getenv("SNOWFLAKE_TABLE") or "STOCK_TICKERS"

    # Use your provided order and map to UPPERCASE identifiers (matches unquoted table)
    fieldnames = fieldnames or list(rows[0].keys())
    snow_cols   = [c.upper() for c in fieldnames]
    col_list    = ", ".join(snow_cols)
    placeholders = ", ".join(["%s"] * len(fieldnames))
    insert_sql   = f'INSERT INTO {table} ({col_list}) VALUES ({placeholders})'

    def norm(r):
        out = {k: r.get(k) for k in fieldnames}
        if "active" in out and out["active"] is not None:
            out["active"] = bool(out["active"])
        # last_updated_utc stays ISO; Snowflake parses TIMESTAMP
        return tuple(out[k] for k in fieldnames)

    con = connector.connect(**connect_kwargs)
    try:
        cur = con.cursor()
        if truncate:
            cur.execute(f"TRUNCATE TABLE {table}")

        total = 0
        batch = []
        for r in rows:
            batch.append(norm(r))
            if len(batch) >= batch_size:
                cur.executemany(insert_sql, batch)
                total += len(batch)
                batch.clear()
        if batch:
            cur.executemany(insert_sql, batch)
            total += len(batch)

        con.commit()
        print(f"Snowflake: inserted {total} rows into {table}.")
        return total
    finally:
        con.close()

# --- original main, with one new call to upload ---
if __name__=='__main__':
    tickers = run_stock_job()
    print(f"Fetched {len(tickers)} tickers.")
    # Overwrite table each run? set truncate=True
    load_to_snowflake(tickers, table=SF_TABLE, fieldnames=SNOWFLAKE_COLS, truncate=False)
