"""监控+维护引擎测试"""
import tempfile, os
from src.utils import load_config
from src.db import get_connection, upsert_daily
import pandas as pd
import numpy as np


def test_monitor_engine_creation():
    cfg = load_config()
    cfg["database"]["path"] = os.path.join(tempfile.mkdtemp(), "test_m1.duckdb")
    conn = get_connection(cfg)
    from src.monitor.engine import MonitorEngine
    engine = MonitorEngine(conn, cfg)
    # 无关注列表时不报错
    result = engine.run("2026-07-01")
    assert result["watchlist_returns"] == 0
    assert result["sell_signals"] == 0
    conn.close()


def test_maintain_engine_price_below_ma():
    cfg = load_config()
    cfg["database"]["path"] = os.path.join(tempfile.mkdtemp(), "test_m2.duckdb")
    conn = get_connection(cfg)

    # 插入模拟数据：连续多日低于 MA60
    dates = pd.date_range("2026-06-01", "2026-07-01", freq="B")
    close_vals = [10.0 - 0.1 * i for i in range(len(dates))]  # 持续下跌
    df = pd.DataFrame({
        "ts_code": "600519.SH", "trade_date": dates,
        "open": 0, "high": 0, "low": 0,
        "close": close_vals, "pre_close": 0, "change": 0,
        "pct_chg": 0, "vol": 1e6, "amount": 1e7,
    })
    upsert_daily(conn, "a_daily", df)

    # stock_factors: DIF 持续低于 DEA
    for i, d in enumerate(dates[-6:]):
        conn.execute("""
            INSERT OR REPLACE INTO stock_factors (ts_code, trade_date, ma20, ma60, macd_dif, macd_dea, rsi14)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, ["600519.SH", d.date(), 8.0, 9.0, -0.1, 0.05, 40.0])

    conn.execute("INSERT OR REPLACE INTO stock_info (ts_code, name, market) VALUES ('600519.SH','贵州茅台','A')")
    conn.execute("INSERT OR REPLACE INTO watchlist (ts_code, name, market, status, entry_price, entry_date) VALUES ('600519.SH','贵州茅台','A','active',10.5,'2026-06-01')")

    from src.monitor.maintainer import MaintainEngine
    engine = MaintainEngine(conn, cfg)
    suggestions = engine.run("2026-07-01")

    assert len(suggestions) >= 1
    # suggestions dict keys come from _check_price_below_ma (ts_code, name, market, reason, consecutive_days)
    # The "action" key is added later by upsert_suggested_action, not by the check function
    assert suggestions[0]["ts_code"] == "600519.SH"
    assert "reason" in suggestions[0]
    conn.close()
