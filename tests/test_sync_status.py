"""sync_status 的 ON CONFLICT 行为测试 — 验证 first_date/error_count 不被覆盖"""
import duckdb
import pandas as pd
from src.db import get_connection, create_tables, update_sync_status, record_sync_error


def test_first_date_preserved():
    """首次同步设置 first_date，后续同步不覆盖"""
    conn = duckdb.connect(":memory:")
    create_tables(conn)

    # 首次同步
    update_sync_status(conn, "000001.SZ", "A", "2024-01-01", 100)
    row = conn.execute(
        "SELECT first_date, last_sync, row_count, error_count FROM sync_status "
        "WHERE ts_code='000001.SZ'"
    ).fetchone()
    assert str(row[0]) == "2024-01-01"  # first_date = 首次同步日期
    assert str(row[1]) == "2024-01-01"  # last_sync = 首次同步日期
    assert row[2] == 100
    assert row[3] == 0  # error_count 初始为 0

    # 第二次同步（日期更新）
    update_sync_status(conn, "000001.SZ", "A", "2024-06-01", 200)
    row = conn.execute(
        "SELECT first_date, last_sync, row_count, error_count FROM sync_status "
        "WHERE ts_code='000001.SZ'"
    ).fetchone()
    assert str(row[0]) == "2024-01-01"  # first_date 不变！
    assert str(row[1]) == "2024-06-01"  # last_sync 更新
    assert row[2] == 200               # row_count 更新

    conn.close()


def test_error_count_preserved():
    """error_count 不被 update_sync_status 清零"""
    conn = duckdb.connect(":memory:")
    create_tables(conn)

    # 首次同步
    update_sync_status(conn, "000002.SZ", "A", "2024-01-01", 50)

    # 记录错误
    record_sync_error(conn, "000002.SZ", "A", "网络超时")
    record_sync_error(conn, "000002.SZ", "A", "限流")
    row = conn.execute(
        "SELECT error_count, last_error FROM sync_status "
        "WHERE ts_code='000002.SZ'"
    ).fetchone()
    assert row[0] == 2  # 两次错误

    # 再次成功同步 — error_count 不应被清零
    update_sync_status(conn, "000002.SZ", "A", "2024-06-01", 100)
    row = conn.execute(
        "SELECT first_date, last_sync, row_count, error_count FROM sync_status "
        "WHERE ts_code='000002.SZ'"
    ).fetchone()
    assert str(row[0]) == "2024-01-01"  # first_date 不变
    assert str(row[1]) == "2024-06-01"  # last_sync 更新
    assert row[2] == 100               # row_count 更新
    assert row[3] == 2                 # error_count 保留！

    conn.close()


def test_multiple_stocks():
    """多只股票的 sync_status 互不影响"""
    conn = duckdb.connect(":memory:")
    create_tables(conn)

    update_sync_status(conn, "000001.SZ", "A", "2024-01-01", 100)
    update_sync_status(conn, "000002.SZ", "A", "2024-03-01", 200)
    update_sync_status(conn, "600519.SH", "A", "2024-06-01", 300)

    # 更新第一只
    update_sync_status(conn, "000001.SZ", "A", "2024-12-01", 150)

    rows = conn.execute(
        "SELECT ts_code, first_date, last_sync FROM sync_status "
        "ORDER BY ts_code"
    ).fetchall()

    assert len(rows) == 3
    # 第一只的 first_date 不变
    assert str(rows[0][1]) == "2024-01-01"
    assert str(rows[0][2]) == "2024-12-01"
    # 其他不受影响
    assert str(rows[1][1]) == "2024-03-01"
    assert str(rows[2][1]) == "2024-06-01"

    conn.close()
