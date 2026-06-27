"""集成测试 — 验证 DB + Pipeline 协同工作"""
import duckdb
import pandas as pd
import pytest
from src.db import get_connection, create_tables, upsert_daily, get_sync_status, update_sync_status
from src.utils import load_config


class TestDBIntegration:
    """数据库模块集成测试"""
    
    def test_full_cycle(self, tmp_path):
        db_path = tmp_path / "test.duckdb"
        config = {
            "database": {"path": str(db_path)},
            "data": {"start_date": "2015-01-01"},
            "rate_limit": {
                "tushare": {"max_calls": 190, "period": 60},
                "akshare": {"max_calls": 48, "period": 60},
                "yfinance": {"max_calls": 1900, "period": 3600},
            },
            "retry": {"max_attempts": 3, "backoff_base": 5},
            "logging": {"level": "INFO", "file": str(tmp_path / "test.log")},
            "tushare": {"token": ""},
        }
        
        conn = get_connection(config)
        
        # 1. 表已创建
        tables = conn.execute("""
            SELECT name FROM sqlite_master WHERE type='table'
        """).fetchall()
        table_names = {t[0] for t in tables}
        assert "a_daily" in table_names
        assert "sync_status" in table_names
        
        # 2. 写入日线数据
        df = pd.DataFrame({
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["2024-01-01", "2024-01-01"],
            "open": [10.0, 20.0],
            "high": [11.0, 21.0],
            "low": [9.5, 19.5],
            "close": [10.5, 20.5],
            "pre_close": [10.1, 20.1],
            "change": [0.4, 0.4],
            "pct_chg": [3.96, 1.99],
            "vol": [100000.0, 200000.0],
            "amount": [1050000.0, 4100000.0],
        })
        count = upsert_daily(conn, "a_daily", df)
        assert count == 2
        
        # 3. 更新 sync_status
        update_sync_status(conn, "000001.SZ", "A", "2024-01-01", 1)
        update_sync_status(conn, "000002.SZ", "A", "2024-01-01", 1)
        
        # 4. 读取 sync_status
        status = get_sync_status(conn, "A")
        assert len(status) == 2
        assert status.iloc[0]["row_count"] == 1
        
        # 5. 幂等写入
        df2 = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["2024-01-01"],
            "open": [10.5], "high": [11.5], "low": [10.0], "close": [11.0],
            "pre_close": [10.1], "change": [0.9], "pct_chg": [8.91],
            "vol": [110000.0], "amount": [1150000.0],
        })
        upsert_daily(conn, "a_daily", df2)
        row = conn.execute("""
            SELECT open, close FROM a_daily 
            WHERE ts_code='000001.SZ' AND trade_date='2024-01-01'
        """).fetchone()
        assert row[0] == 10.5  # 覆盖了
        assert row[1] == 11.0
        
        conn.close()

    def test_multimarket(self, tmp_path):
        db_path = tmp_path / "multi.duckdb"
        config = {
            "database": {"path": str(db_path)},
            "data": {"start_date": "2015-01-01"},
            "rate_limit": {
                "tushare": {"max_calls": 190, "period": 60},
                "akshare": {"max_calls": 48, "period": 60},
                "yfinance": {"max_calls": 1900, "period": 3600},
            },
            "retry": {"max_attempts": 3, "backoff_base": 5},
            "logging": {"level": "INFO", "file": str(tmp_path / "test.log")},
            "tushare": {"token": ""},
        }
        
        conn = get_connection(config)
        
        # A股
        df_a = pd.DataFrame({
            "ts_code": ["000001.SZ"], "trade_date": ["2024-01-01"],
            "open": [10.0], "high": [12.0], "low": [9.0], "close": [11.0],
            "pre_close": [10.0], "change": [1.0], "pct_chg": [10.0],
            "vol": [1e6], "amount": [1.1e7],
        })
        upsert_daily(conn, "a_daily", df_a)
        
        # 港股
        df_hk = pd.DataFrame({
            "ts_code": ["00700.HK"], "trade_date": ["2024-01-01"],
            "open": [300.0], "high": [310.0], "low": [295.0], "close": [305.0],
            "vol": [5e6], "amount": [1.5e9],
        })
        upsert_daily(conn, "hk_daily", df_hk)
        
        # 美股
        df_us = pd.DataFrame({
            "ts_code": ["AAPL"], "trade_date": ["2024-01-01"],
            "open": [185.0], "high": [190.0], "low": [184.0], "close": [188.0],
            "adj_close": [188.5], "volume": [50_000_000],
        })
        upsert_daily(conn, "us_daily", df_us)
        
        # 验证各表独立
        a_count = conn.execute("SELECT COUNT(*) FROM a_daily").fetchone()[0]
        hk_count = conn.execute("SELECT COUNT(*) FROM hk_daily").fetchone()[0]
        us_count = conn.execute("SELECT COUNT(*) FROM us_daily").fetchone()[0]
        assert a_count == 1
        assert hk_count == 1
        assert us_count == 1
        
        conn.close()
