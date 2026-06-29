# tests/test_db.py
import duckdb
import pandas as pd
from src.db import get_connection, create_tables, upsert_daily, get_sync_status, update_sync_status

def test_create_tables():
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = {t[0] for t in tables}
    expected = {"stock_info", "a_daily", "a_daily_basic", "a_adj_factor",
                "etf_daily", "hk_daily", "us_daily", "sync_status"}
    assert expected.issubset(names)

def test_upsert_daily_insert():
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    df = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "trade_date": ["2024-01-01"],
        "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
        "pre_close": [10.1], "change": [0.4], "pct_chg": [3.96],
        "vol": [100000.0], "amount": [1050000.0]
    })
    count = upsert_daily(conn, "a_daily", df)
    assert count == 1
    row = conn.execute("SELECT * FROM a_daily WHERE ts_code='000001.SZ'").fetchone()
    assert row[2] == 10.0  # open

def test_upsert_daily_update():
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    df1 = pd.DataFrame({
        "ts_code": ["000001.SZ"], "trade_date": ["2024-01-01"],
        "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
        "pre_close": [10.1], "change": [0.4], "pct_chg": [3.96],
        "vol": [100000.0], "amount": [1050000.0]
    })
    upsert_daily(conn, "a_daily", df1)
    df2 = pd.DataFrame({
        "ts_code": ["000001.SZ"], "trade_date": ["2024-01-01"],
        "open": [10.2], "high": [11.2], "low": [9.7], "close": [10.7],
        "pre_close": [10.1], "change": [0.6], "pct_chg": [5.94],
        "vol": [110000.0], "amount": [1150000.0]
    })
    count = upsert_daily(conn, "a_daily", df2)
    assert count == 1
    row = conn.execute("SELECT open FROM a_daily WHERE ts_code='000001.SZ'").fetchone()
    assert row[0] == 10.2  # updated

def test_sync_status():
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    update_sync_status(conn, "000001.SZ", "A", "2024-06-01", 500)
    df = get_sync_status(conn, "A")
    assert len(df) == 1
    assert str(df.iloc[0]["last_sync"].date()) == "2024-06-01"
    assert df.iloc[0]["row_count"] == 500


def test_phase1_tables_exist():
    """验证 Phase 1 的 8 张新表存在"""
    from src.db import create_tables
    import duckdb
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    names = {t[0] for t in tables}
    expected_phase1 = {
        "northbound_flow", "margin_trading", "dragon_tiger",
        "block_trade", "holder_num", "dividend",
        "lockup_expiry", "stock_boards",
    }
    missing = expected_phase1 - names
    assert not missing, f"Missing tables: {missing}"


def test_margin_trading_upsert():
    """验证融资融券表 upsert"""
    from src.db import create_tables, upsert_daily
    import duckdb
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    df = pd.DataFrame({
        "ts_code": ["600519.SH"],
        "trade_date": ["2026-06-29"],
        "rzye": [1.5e9],
        "rzmre": [2e8],
        "rzche": [1e8],
        "rqye": [5e7],
        "rqmcl": [10000],
        "rqchl": [8000],
        "rzrqye": [1.55e9],
    })
    count = upsert_daily(conn, "margin_trading", df)
    assert count == 1
    row = conn.execute(
        "SELECT rzye FROM margin_trading WHERE ts_code='600519.SH'"
    ).fetchone()
    assert row[0] == 1.5e9
