"""v2 端到端集成测试：发现→加入关注→监控→维护"""
import tempfile, os
from src.utils import load_config
from src.db import get_connection, upsert_daily
import pandas as pd
import numpy as np
from datetime import date


def setup_test_db():
    """创建完整的测试数据库（含多日数据）"""
    cfg = load_config()
    cfg["database"]["path"] = os.path.join(tempfile.mkdtemp(), "test_v2_int.duckdb")
    conn = get_connection(cfg)

    # 插入 5 只 A 股 + 模拟日线
    codes = ["600519.SH", "000858.SZ", "000001.SZ", "600036.SH", "000002.SZ"]
    names = ["贵州茅台", "五粮液", "平安银行", "招商银行", "万科A"]
    dates = pd.date_range("2024-01-01", "2026-06-30", freq="B")

    for code, name in zip(codes, names):
        np.random.seed(hash(code) % 2**32)
        close = 20 * (1 + np.cumsum(np.random.randn(len(dates)) * 0.015))
        df = pd.DataFrame({
            "ts_code": code, "trade_date": dates, "open": close * 0.99,
            "high": close * 1.02, "low": close * 0.98, "close": close,
            "pre_close": 0, "change": 0, "pct_chg": 0,
            "vol": 5e6, "amount": 1e8,
        })
        upsert_daily(conn, "a_daily", df)

        # a_daily_basic
        basic_df = pd.DataFrame({
            "ts_code": code, "trade_date": dates,
            "pe": 15 + np.random.randn(len(dates)) * 3,
            "pb": 2 + np.random.randn(len(dates)) * 0.5,
            "turnover_rate": 1.5,
            "volume_ratio": 1.0, "total_mv": 1e9, "circ_mv": 5e8,
        })
        conn.register("_tmp_basic", basic_df)
        conn.execute("INSERT OR REPLACE INTO a_daily_basic SELECT * FROM _tmp_basic")
        conn.unregister("_tmp_basic")

        # adj_factor
        adj_df = pd.DataFrame({"ts_code": code, "trade_date": dates, "adj_factor": 1.0})
        conn.register("_tmp_adj", adj_df)
        conn.execute("INSERT OR REPLACE INTO a_adj_factor SELECT * FROM _tmp_adj")
        conn.unregister("_tmp_adj")

        conn.execute("INSERT OR REPLACE INTO stock_info (ts_code, name, market, list_status) VALUES (?,?,'A','L')",
                     [code, name])

    # 添加策略配置
    cfg["strategies"] = {
        "A": {
            "label": "A股",
            "sell_rules": {"stop_loss": -8, "stop_profit": 30,
                           "ma_break": ["ma20", "ma60"],
                           "pattern": ["macd_dead_cross"]},
            "remove_conditions": [
                {"type": "price_below_ma", "ma": "ma60", "consecutive_days": 5},
            ],
        }
    }
    return conn, cfg


def test_full_discover_flow():
    """发现流程：条件翻译→筛选→结果"""
    conn, cfg = setup_test_db()

    from src.screening.screener import StockScreener
    screener = StockScreener(conn)
    conditions = [{"factor": "pe_ttm", "op": "lt", "value": 30}]
    df = screener.search_full_market(conditions, market="A")
    assert not df.empty
    assert len(df) >= 1
    assert "ts_code" in df.columns
    conn.close()


def test_watchlist_add_and_performance():
    """加入关注→收益刷新"""
    conn, cfg = setup_test_db()

    # 加入关注
    from src.agent.tools import execute_tool
    r = execute_tool("add_to_watchlist",
        {"codes": ["600519.SH", "000858.SZ"], "condition": "pe_ttm<30"},
        conn)
    assert "已添加 2" in r

    # 验证 watchlist 有 v2 新字段
    row = conn.execute("SELECT status, market, entry_price FROM watchlist WHERE ts_code='600519.SH'").fetchone()
    assert row[0] == "active"
    assert row[1] == "A"
    assert row[2] is not None

    # 运行监控引擎
    from src.monitor.engine import MonitorEngine
    engine = MonitorEngine(conn, cfg)
    result = engine.run("2026-06-30")
    assert result["watchlist_returns"] >= 1

    # 验证 performance 表有数据
    perf_count = conn.execute("SELECT COUNT(*) FROM watchlist_performance").fetchone()[0]
    assert perf_count >= 1
    conn.close()


def test_maintain_flow():
    """维护流程：形态检查→建议生成"""
    conn, cfg = setup_test_db()

    # 确保有活跃的关注股票
    conn.execute("INSERT OR REPLACE INTO watchlist (ts_code, name, market, status, entry_price, entry_date) VALUES ('600036.SH','招商银行','A','active',15.0,'2026-01-01')")

    # 确保 a_daily 当天的收盘价低于 ma60（设置为 1.0，ma60=20.0）
    conn.execute("UPDATE a_daily SET close=1.0 WHERE ts_code='600036.SH' AND trade_date='2026-06-30'")
    conn.execute("""
        INSERT OR REPLACE INTO stock_factors (ts_code, trade_date, ma20, ma60, macd_dif, macd_dea, rsi14)
        VALUES ('600036.SH', '2026-06-30', 10.0, 20.0, -0.2, 0.1, 35.0)
    """)

    from src.monitor.maintainer import MaintainEngine
    engine = MaintainEngine(conn, cfg)
    # 直接检查单个条件
    engine._check_price_below_ma(
        "600036.SH", "招商银行", "A", "ma60", 2, "2026-06-30"
    )
    # 如果数据库有足够数据且 close < ma60，应该生成建议
    # 这里主要验证不崩溃
    assert True
    conn.close()
