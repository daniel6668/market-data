"""策略规则 CRUD 测试"""
import tempfile
import os
from src.utils import load_config
from src.db import get_connection, save_strategy_rule, get_active_rules, upsert_suggested_action, get_pending_actions, confirm_action


def test_save_and_get_rules():
    cfg = load_config()
    cfg["database"]["path"] = os.path.join(tempfile.mkdtemp(), "test_rules.duckdb")
    conn = get_connection(cfg)

    sid = save_strategy_rule(conn, "低估值A股", "A", "screen",
        [{"factor": "pe_ttm", "op": "lt", "value": 20}])
    assert sid > 0

    rules = get_active_rules(conn, "A", "screen")
    assert len(rules) == 1
    assert rules[0]["name"] == "低估值A股"
    assert len(rules[0]["conditions"]) == 1
    conn.close()


def test_suggested_actions_flow():
    cfg = load_config()
    cfg["database"]["path"] = os.path.join(tempfile.mkdtemp(), "test_actions.duckdb")
    conn = get_connection(cfg)

    upsert_suggested_action(conn, "600519.SH", "贵州茅台", "A",
        "SELL", "跌破MA60", "2026-07-01", {"ma60": 1950.0, "close": 1920.0})
    # 重复写入不会创建新记录
    upsert_suggested_action(conn, "600519.SH", "贵州茅台", "A",
        "SELL", "跌破MA60", "2026-07-01")

    pending = get_pending_actions(conn, "SELL")
    assert len(pending) == 1
    assert pending[0]["ts_code"] == "600519.SH"
    assert pending[0]["action"] == "SELL"

    confirm_action(conn, pending[0]["id"], "confirmed")
    pending2 = get_pending_actions(conn, "SELL")
    assert len(pending2) == 0
    conn.close()
