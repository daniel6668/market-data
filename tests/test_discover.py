"""发现引擎测试"""
import tempfile, os, json
from src.utils import load_config
from src.db import get_connection
from src.discover.compiler import validate_plan


def test_validate_plan_valid():
    plan = {
        "conditions": [{"factor": "pe_ttm", "op": "lt", "value": 20}],
        "universe": "A",
        "action": "screen"
    }
    ok, err = validate_plan(plan)
    assert ok, f"Should be valid: {err}"


def test_validate_plan_invalid_op():
    plan = {
        "conditions": [{"factor": "pe_ttm", "op": "INVALID", "value": 20}],
        "universe": "A"
    }
    ok, err = validate_plan(plan)
    assert not ok


def test_validate_plan_empty():
    ok, err = validate_plan({"conditions": [], "universe": "A"})
    assert not ok


def test_validate_plan_unknown_market():
    plan = {
        "conditions": [{"factor": "pe_ttm", "op": "lt", "value": 20}],
        "universe": "CRYPTO"
    }
    ok, err = validate_plan(plan)
    assert not ok


def test_translate_schema():
    """验证翻译 schema 包含必要字段"""
    from src.discover.translator import TRANSLATE_SYSTEM_PROMPT
    assert "cross_above" in TRANSLATE_SYSTEM_PROMPT
    assert "cross_below" in TRANSLATE_SYSTEM_PROMPT
    assert "pe_ttm" in TRANSLATE_SYSTEM_PROMPT
    assert "JSON" in TRANSLATE_SYSTEM_PROMPT
