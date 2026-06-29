#!/usr/bin/env python
"""小批量 A 股增量拉取 — 每次 30 只，限流自动跳过"""
import sys, os
import pandas as pd
import duckdb
from datetime import datetime, timedelta

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, '.')
BATCH = 30

# checkpoint
c = duckdb.connect('data/market.duckdb')
c.execute('CHECKPOINT')
synced_before = c.execute("SELECT COUNT(*) FROM sync_status WHERE market='A' AND last_sync IS NOT NULL").fetchone()[0]
# 取需要更新的股票（包括从未同步的），按 last_sync 排序优先拉未同步的
stocks = c.execute("""
    SELECT ts_code, last_sync FROM sync_status 
    WHERE market='A' AND error_count < 10
    ORDER BY CASE WHEN last_sync IS NULL THEN 0 ELSE 1 END, last_sync
    LIMIT ?
""", [BATCH]).fetchall()
c.close()

if not stocks:
    c = duckdb.connect('data/market.duckdb')
    rows = c.execute('SELECT COUNT(*) FROM a_daily').fetchone()[0]
    c.close()
    print(f'[done] All A-stocks synced! {synced_before}/5528 stocks, {rows:,} rows')
    sys.exit(0)

from src.pipeline import Pipeline
p = Pipeline()
today = datetime.now().strftime('%Y-%m-%d')
ok = fail = 0

for ts_code, last_sync in stocks:
    if last_sync is None or pd.isna(last_sync):
        start = '2015-01-01'
    else:
        if isinstance(last_sync, str):
            last_sync = pd.to_datetime(last_sync).date()
        start = (last_sync + timedelta(days=1)).strftime('%Y-%m-%d')
    try:
        p._fetch_daily_with_retry('A', ts_code, start, today)
        ok += 1
    except Exception as e:
        fail += 1
        print(f'  fail {ts_code}: {str(e)[:80]}')

p.close()

c = duckdb.connect('data/market.duckdb')
c.execute('CHECKPOINT')
synced = c.execute("SELECT COUNT(*) FROM sync_status WHERE market='A' AND last_sync IS NOT NULL").fetchone()[0]
rows = c.execute('SELECT COUNT(*) FROM a_daily').fetchone()[0]
c.close()

print(f'[batch] ok={ok}/{len(stocks)} (+{synced-synced_before}), {rows:,} rows, {synced}/5528')
