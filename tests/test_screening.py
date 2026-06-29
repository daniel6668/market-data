"""筛选器测试"""
import duckdb
import pandas as pd
import pytest


@pytest.fixture
def db_conn():
    conn = duckdb.connect(":memory:")
    from src.db import create_tables
    create_tables(conn)
    conn.execute("INSERT INTO stock_info VALUES ('000001.SZ', 'Test1', 'A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, now())")
    conn.execute("INSERT INTO stock_info VALUES ('600519.SH', 'Test2', 'A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, now())")
    df = pd.DataFrame({
        "ts_code": ["000001.SZ", "600519.SH"],
        "trade_date": ["2026-06-29", "2026-06-29"],
        "pe": [8.5, 35.0],
        "pb": [0.7, 12.0],
        "turnover_rate": [1.2, 0.8],
    })
    conn.register("_tmp_b", df)
    conn.execute("INSERT OR REPLACE INTO a_daily_basic (ts_code, trade_date, pe, pb, turnover_rate) SELECT * FROM _tmp_b")
    conn.execute("""
        INSERT INTO stock_factors (ts_code, trade_date, rsi14, vol_ratio, ret_20d)
        VALUES ('000001.SZ', '2026-06-29', 28.5, 1.8, 8.0)
    """)
    conn.execute("""
        INSERT INTO stock_factors (ts_code, trade_date, rsi14, vol_ratio, ret_20d)
        VALUES ('600519.SH', '2026-06-29', 65.0, 0.7, -3.0)
    """)
    return conn


def test_by_conditions_low_pe(db_conn):
    """测试低PE筛选"""
    from src.screening.screener import StockScreener
    screener = StockScreener(db_conn)
    df = screener.by_conditions([
        {"factor": "pe_ttm", "op": "lt", "value": 30},
    ], trade_date="2026-06-29")
    assert len(df) == 1
    assert df.iloc[0]["ts_code"] == "000001.SZ"


def test_by_conditions_multiple(db_conn):
    """测试多条件 AND 筛选"""
    from src.screening.screener import StockScreener
    screener = StockScreener(db_conn)
    df = screener.by_conditions([
        {"factor": "pe_ttm", "op": "lt", "value": 30},
        {"factor": "pb", "op": "lt", "value": 1},
    ], trade_date="2026-06-29")
    assert len(df) == 1


def test_by_template(db_conn):
    """测试预设模板"""
    from src.screening.screener import StockScreener
    screener = StockScreener(db_conn)
    df = screener.by_template("oversold_bounce", trade_date="2026-06-29")
    assert isinstance(df, pd.DataFrame)
    if not df.empty:
        assert "000001.SZ" in df["ts_code"].values
