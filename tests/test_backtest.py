"""回测引擎测试"""
import duckdb
import pandas as pd
import numpy as np
import pytest


@pytest.fixture
def db_conn():
    conn = duckdb.connect(":memory:")
    from src.db import create_tables
    create_tables(conn)
    dates = pd.date_range("2025-01-01", periods=200, freq="B")
    np.random.seed(123)
    close = 100 + np.cumsum(np.random.randn(200) * 1.5)
    df = pd.DataFrame({
        "ts_code": "000001.SZ",
        "trade_date": dates,
        "open": close + np.random.randn(200),
        "high": close + abs(np.random.randn(200) * 3),
        "low": close - abs(np.random.randn(200) * 3),
        "close": close,
        "vol": np.random.randint(100000, 1000000, 200),
    })
    conn.register("_tmp", df)
    cols = "ts_code, trade_date, open, high, low, close, vol"
    conn.execute(f"INSERT OR REPLACE INTO a_daily ({cols}) SELECT {cols} FROM _tmp")
    return conn


def test_run_single_ma_cross(db_conn):
    """测试均线交叉策略回测"""
    from src.backtest.runner import BacktestRunner
    runner = BacktestRunner(db_conn)

    prices = runner._load_prices("000001.SZ", "2025-03-01", "2025-12-31")
    ma20 = prices.rolling(20).mean()
    ma60 = prices.rolling(60).mean()
    condition_buy = ma20 > ma60
    condition_sell = ma20 < ma60

    result = runner.run_single(
        "000001.SZ", condition_buy, condition_sell,
        "2025-03-01", "2025-12-31"
    )

    assert result.n_trades >= 0
    assert isinstance(result.total_return, float)
    assert result.max_drawdown >= 0  # vectorbt reports as positive
    d = result.to_dict()
    assert "sharpe_ratio" in d


def test_backtest_result_to_dict():
    """验证 BacktestResult.to_dict 格式"""
    from src.backtest.report import BacktestResult
    r = BacktestResult(total_return=15.5, annual_return=12.3,
                       sharpe_ratio=1.2, max_drawdown=-18.0,
                       win_rate=55.0, profit_factor=2.1, n_trades=20)
    d = r.to_dict()
    assert d["total_return"] == 15.5
    assert d["sharpe_ratio"] == 1.2
    assert "总收益" in r.summary()


def test_empty_data_returns_empty_result(db_conn):
    """测试无数据时返回空结果"""
    from src.backtest.runner import BacktestRunner
    runner = BacktestRunner(db_conn)
    prices = runner._load_prices("NONEXIST", "2020-01-01", "2020-12-31")
    assert prices.empty
