"""交易日历测试"""
import duckdb
from src.db import create_tables, save_trade_calendar, has_trade_calendar, get_trading_days
from src.utils import TradingCalendar


def test_weekend_fallback():
    """未加载日历时 fallback 到周末检查"""
    TradingCalendar._trading_days = set()  # 清空缓存
    assert TradingCalendar.is_trading_day("A", "2026-06-28") is False  # 周日
    assert TradingCalendar.is_trading_day("A", "2026-06-27") is False  # 周六
    assert TradingCalendar.is_trading_day("A", "2026-06-26") is True   # 周五


def test_load_from_db():
    """从数据库加载交易日历"""
    conn = duckdb.connect(":memory:")
    create_tables(conn)

    # 写入测试数据
    import pandas as pd
    df = pd.DataFrame({
        "cal_date": ["2026-06-23", "2026-06-24", "2026-06-25", "2026-06-26",
                     "2026-06-27", "2026-06-28"],  # 周末两天
        "is_open": [True, True, True, True, False, False],
        "exchange": ["SSE"] * 6,
    })
    save_trade_calendar(conn, df)

    # 清空缓存后加载
    TradingCalendar._trading_days = set()
    count = TradingCalendar.load_from_db(conn)
    assert count == 4  # 4 个交易日

    # 周末不是交易日
    assert TradingCalendar.is_trading_day("A", "2026-06-28") is False
    # 工作日是交易日
    assert TradingCalendar.is_trading_day("A", "2026-06-26") is True

    conn.close()


def test_get_trading_days():
    """查询交易日列表"""
    conn = duckdb.connect(":memory:")
    create_tables(conn)

    import pandas as pd
    df = pd.DataFrame({
        "cal_date": ["2026-06-23", "2026-06-24", "2026-06-25", "2026-06-26"],
        "is_open": [True, True, False, True],  # 6/25 非交易日（节假日）
        "exchange": ["SSE"] * 4,
    })
    save_trade_calendar(conn, df)

    days = get_trading_days(conn, "2026-06-23", "2026-06-26")
    assert len(days) == 3  # 排除 6/25
    assert "2026-06-25" not in [str(d) for d in days]

    conn.close()


def test_has_trade_calendar():
    """检查是否有交易日历数据"""
    conn = duckdb.connect(":memory:")
    create_tables(conn)

    assert has_trade_calendar(conn) is False

    import pandas as pd
    df = pd.DataFrame({
        "cal_date": ["2026-06-23"], "is_open": [True], "exchange": ["SSE"],
    })
    save_trade_calendar(conn, df)

    assert has_trade_calendar(conn) is True
    assert has_trade_calendar(conn, "SZSE") is False

    conn.close()
