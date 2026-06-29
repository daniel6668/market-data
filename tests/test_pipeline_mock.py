"""Pipeline mock 测试 — 用内存 DuckDB + mock 数据源验证编排逻辑"""
import duckdb
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from src.db import get_connection, create_tables


def _make_config(db_path: str) -> dict:
    return {
        "database": {"path": db_path},
        "data": {"start_date": "2020-01-01"},
        "rate_limit": {
            "tushare": {"max_calls": 45, "period": 60},
            "akshare": {"max_calls": 48, "period": 60},
            "yfinance": {"max_calls": 200, "period": 3600},
        },
        "retry": {"max_attempts": 3, "backoff_base": 1},
        "logging": {"level": "INFO", "file": "data/test.log"},
        "tushare": {"token": ""},
    }


def _make_daily_df(code: str, dates: list[str]) -> pd.DataFrame:
    """构造测试用日线 DataFrame"""
    n = len(dates)
    return pd.DataFrame({
        "ts_code": [code] * n,
        "trade_date": dates,
        "open": [10.0] * n, "high": [11.0] * n, "low": [9.5] * n,
        "close": [10.5] * n, "pre_close": [10.0] * n,
        "change": [0.5] * n, "pct_chg": [5.0] * n,
        "vol": [100000.0] * n, "amount": [1050000.0] * n,
    })


class TestPipelineMock:
    """用 mock 数据源测试 Pipeline 编排逻辑"""

    def test_init_market_with_mock(self, tmp_path):
        """mock 数据源返回预设数据，验证 init_market 编排"""
        from src.pipeline import Pipeline

        config = _make_config(str(tmp_path / "test.duckdb"))
        pipeline = Pipeline(config)

        # mock 数据源
        mock_ts = MagicMock()
        mock_ts.get_stock_list.return_value = pd.DataFrame({
            "ts_code": ["000001.SZ", "000002.SZ"],
            "name": ["平安银行", "万科A"],
            "market": ["A", "A"],
            "list_date": ["1991-04-03", "1991-03-29"],
            "exchange": ["SZ", "SZ"],
            "is_hs": [None, None],
            "list_status": ["L", "L"],
        })
        mock_ts.get_daily.side_effect = lambda code, s, e: _make_daily_df(code, ["2026-06-24", "2026-06-25", "2026-06-26"])
        mock_ts.get_daily_basic.return_value = pd.DataFrame()
        mock_ts.get_adj_factor.return_value = pd.DataFrame()
        mock_ts.supports_extra = True

        pipeline._ts = mock_ts

        result = pipeline.init_market("A")

        assert result["total"] == 2
        assert result["success"] == 2
        assert result["failed"] == 0

        # 验证数据写入
        count = pipeline.conn.execute("SELECT COUNT(*) FROM a_daily").fetchone()[0]
        assert count == 6  # 2 stocks × 3 days

        # 验证 sync_status
        synced = pipeline.conn.execute(
            "SELECT COUNT(*) FROM sync_status WHERE market='A' AND last_sync IS NOT NULL"
        ).fetchone()[0]
        assert synced == 2

        pipeline.close()

    def test_update_market_with_mock(self, tmp_path):
        """mock 数据源返回增量数据，验证 update_market"""
        from src.pipeline import Pipeline

        config = _make_config(str(tmp_path / "test.duckdb"))
        pipeline = Pipeline(config)

        # 先写入 sync_status（模拟已初始化但需要更新）
        from src.db import update_sync_status
        update_sync_status(pipeline.conn, "000001.SZ", "A", "2026-06-20", 100)
        update_sync_status(pipeline.conn, "000002.SZ", "A", "2026-06-20", 100)

        # mock 数据源
        mock_ts = MagicMock()
        mock_ts.get_daily.side_effect = lambda code, s, e: _make_daily_df(code, ["2026-06-24", "2026-06-25", "2026-06-26"])
        mock_ts.get_daily_basic.return_value = pd.DataFrame()
        mock_ts.get_adj_factor.return_value = pd.DataFrame()
        mock_ts.supports_extra = True
        pipeline._ts = mock_ts

        result = pipeline.update_market("A")

        assert result["success"] == 2

        # 验证 last_sync 更新
        last = pipeline.conn.execute(
            "SELECT last_sync FROM sync_status WHERE ts_code='000001.SZ'"
        ).fetchone()[0]
        assert str(last) == "2026-06-26"

        pipeline.close()

    def test_validator_rejects_bad_data(self, tmp_path):
        """数据校验拦截高空值数据"""
        from src.pipeline import Pipeline

        config = _make_config(str(tmp_path / "test.duckdb"))
        pipeline = Pipeline(config)

        # mock 返回全是空值的数据
        mock_ts = MagicMock()
        mock_ts.get_stock_list.return_value = pd.DataFrame({
            "ts_code": ["000001.SZ"], "name": ["test"], "market": ["A"],
        })
        bad_df = pd.DataFrame({
            "ts_code": ["000001.SZ"] * 3,
            "trade_date": ["2026-06-24", "2026-06-25", "2026-06-26"],
            "open": [10.0, None, None],
            "high": [11.0, None, None],
            "low": [9.5, None, None],
            "close": [10.5, None, None],
        })
        mock_ts.get_daily.return_value = bad_df
        mock_ts.get_daily_basic.return_value = pd.DataFrame()
        mock_ts.get_adj_factor.return_value = pd.DataFrame()
        mock_ts.supports_extra = True
        pipeline._ts = mock_ts

        result = pipeline.init_market("A")

        # 校验失败，0 成功
        assert result["success"] == 0
        # 数据未写入
        count = pipeline.conn.execute("SELECT COUNT(*) FROM a_daily").fetchone()[0]
        assert count == 0

        pipeline.close()
