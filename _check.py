import duckdb
conn = duckdb.connect('data/market.duckdb', read_only=True)
for t in ['stock_info','us_daily','etf_daily','hk_daily','a_daily','sync_status']:
    cnt = conn.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0]
    print(f"{t}: {cnt} rows")
conn.close()
