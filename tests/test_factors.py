"""因子引擎测试"""
import duckdb
import pandas as pd
import numpy as np
import pytest


@pytest.fixture
def db_conn():
    conn = duckdb.connect(":memory:")
    from src.db import create_tables
    create_tables(conn)
    dates = pd.date_range("2026-01-01", periods=100, freq="B")
    np.random.seed(42)
    close = 100 + np.cumsum(np.random.randn(100) * 2)
    df = pd.DataFrame({
        "ts_code": "000001.SZ",
        "trade_date": dates,
        "open": close + np.random.randn(100),
        "high": close + abs(np.random.randn(100) * 3),
        "low": close - abs(np.random.randn(100) * 3),
        "close": close,
        "vol": np.random.randint(100000, 1000000, 100),
    })
    conn.register("_tmp_daily", df)
    conn.execute("INSERT OR REPLACE INTO a_daily (ts_code, trade_date, open, high, low, close, vol) SELECT * FROM _tmp_daily")
    return conn


def test_factor_registry_has_all_factors():
    """验证因子注册表包含预期的 20 个因子"""
    from src.factors.registry import FACTOR_REGISTRY
    expected = {"ma5", "ma10", "ma20", "ma60", "ema12", "ema26",
                "macd_dif", "macd_dea", "macd_bar",
                "ret_5d", "ret_10d", "ret_20d",
                "rsi6", "rsi14", "atr14",
                "boll_upper", "boll_mid", "boll_lower",
                "vol_ratio", "avg_vol_5d", "avg_vol_20d"}
    assert set(FACTOR_REGISTRY.keys()) == expected


def test_compute_single_returns_data(db_conn):
    """验证单股因子计算返回非空 DataFrame"""
    from src.factors.engine import FactorEngine
    engine = FactorEngine(db_conn)
    df = engine.compute_single("000001.SZ", "2026-01-01", "2026-06-01")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "ma5" in df.columns
    assert "rsi14" in df.columns
    assert df["ma20"].iloc[-1] > 0


def test_compute_all_writes_to_db(db_conn):
    """验证全市场因子计算写入 stock_factors 表"""
    db_conn.execute(
        "INSERT INTO stock_info (ts_code, name, market) VALUES ('000001.SZ', 'Test', 'A')"
    )
    from src.factors.engine import FactorEngine
    engine = FactorEngine(db_conn)
    n = engine.compute_all("2026-02-10")
    assert n == 1
    row = db_conn.execute(
        "SELECT COUNT(*) FROM stock_factors WHERE ts_code='000001.SZ'"
    ).fetchone()
    assert row[0] == 1
