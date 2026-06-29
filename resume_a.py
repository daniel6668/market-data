#!/usr/bin/env python
"""A股数据补齐 — 持续运行直到全部完成"""
import sys, os, time
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, '.')
from src.pipeline import Pipeline
import duckdb

# 从数据库动态获取目标股票数，不再硬编码
c = duckdb.connect('data/market.duckdb', read_only=True)
TARGET = c.execute("SELECT COUNT(*) FROM stock_info WHERE market='A'").fetchone()[0]
c.close()
ROUND = 0

print(f'[resume] 目标: {TARGET} 只 A 股全量日线')

while True:
    ROUND += 1
    try:
        p = Pipeline()
        r = p.update_market('A')
        p.close()
        print(f'[round {ROUND}] total={r["total"]} success={r["success"]} failed={r["failed"]}')
        
        # 检查进度
        import duckdb
        c = duckdb.connect('data/market.duckdb')
        c.execute('CHECKPOINT')
        synced = c.execute("SELECT COUNT(*) FROM sync_status WHERE market='A' AND last_sync IS NOT NULL").fetchone()[0]
        total_rows = c.execute('SELECT COUNT(*) FROM a_daily').fetchone()[0]
        c.close()
        print(f'[progress] {synced}/{TARGET} stocks, {total_rows:,} rows')
        
        if synced >= TARGET and r['total'] == 0:
            print(f'[DONE] All {TARGET} stocks synced! Total rows: {total_rows:,}')
            break
    except Exception as e:
        msg = str(e)
        if '频率' in msg or 'limit' in msg.lower():
            wait = 120
            print(f'[rate-limited] waiting {wait}s...')
        elif 'token不对' in msg:
            wait = 60
            print(f'[token error] waiting {wait}s...')
        elif '权限' in msg:
            print(f'[PERMISSION] {msg}')
            break
        else:
            wait = 30
            print(f'[error] {msg[:100]}')
        time.sleep(wait)
    
    time.sleep(10)  # 每轮间隔

print('[EXIT]')
