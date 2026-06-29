"""数据校验模块测试"""
import pandas as pd
from src.validator import validate_daily


def test_empty_df():
    """空表应该校验失败"""
    result = validate_daily(pd.DataFrame(), "000001.SZ")
    assert not result["valid"]
    assert "空表" in result["issues"]


def test_none_df():
    """None 应该校验失败"""
    result = validate_daily(None, "000001.SZ")
    assert not result["valid"]


def test_valid_daily():
    """正常日线数据应该校验通过"""
    df = pd.DataFrame({
        "ts_code": ["000001.SZ"] * 5,
        "trade_date": pd.to_datetime(["2026-06-20", "2026-06-23", "2026-06-24", "2026-06-25", "2026-06-26"]).date,
        "open": [10.0, 10.5, 10.3, 10.8, 11.0],
        "high": [10.5, 10.8, 10.6, 11.0, 11.2],
        "low": [9.8, 10.2, 10.0, 10.5, 10.8],
        "close": [10.2, 10.6, 10.4, 10.9, 11.1],
    })
    result = validate_daily(df, "000001.SZ")
    assert result["valid"], f"Expected valid, got issues: {result['issues']}"
    assert result["stats"]["rows"] == 5


def test_high_null_ratio():
    """空值比例过高应该校验失败"""
    df = pd.DataFrame({
        "ts_code": ["000001.SZ"] * 4,
        "trade_date": pd.to_datetime(["2026-06-23", "2026-06-24", "2026-06-25", "2026-06-26"]).date,
        "open": [10.0, None, None, None],
        "high": [10.5, None, None, None],
        "low": [9.8, None, None, None],
        "close": [10.2, None, None, None],
    })
    result = validate_daily(df, "000001.SZ")
    assert not result["valid"]
    assert any("空值" in issue for issue in result["issues"])
