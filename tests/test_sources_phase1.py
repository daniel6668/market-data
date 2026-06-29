"""Phase 1 数据源 smoke tests — 东财 datacenter 6合1"""
import pytest
import pandas as pd


def test_eastmoney_datacenter_source_exists():
    """验证源类可以导入"""
    from src.sources.eastmoney_datacenter import EastMoneyDatacenterSource
    source = EastMoneyDatacenterSource()
    assert source is not None


def test_margin_trading_returns_data():
    """验证融资融券数据拉取（smoke test）"""
    from src.sources.eastmoney_datacenter import EastMoneyDatacenterSource
    source = EastMoneyDatacenterSource()
    df = source.get_margin_trading("600519", page_size=3)
    assert isinstance(df, pd.DataFrame)
    if not df.empty:
        assert "trade_date" in df.columns
        assert "rzye" in df.columns


def test_dragon_tiger_returns_data():
    """验证龙虎榜数据拉取（smoke test）"""
    from src.sources.eastmoney_datacenter import EastMoneyDatacenterSource
    source = EastMoneyDatacenterSource()
    df = source.get_dragon_tiger("002475", "", look_back=30)
    assert isinstance(df, pd.DataFrame)
    if not df.empty:
        assert "trade_date" in df.columns
        assert "reason" in df.columns


def test_block_trade_returns_data():
    """验证大宗交易数据拉取（smoke test）"""
    from src.sources.eastmoney_datacenter import EastMoneyDatacenterSource
    source = EastMoneyDatacenterSource()
    df = source.get_block_trade("600519", page_size=5)
    assert isinstance(df, pd.DataFrame)


def test_holder_num_returns_data():
    """验证股东户数数据拉取（smoke test）"""
    from src.sources.eastmoney_datacenter import EastMoneyDatacenterSource
    source = EastMoneyDatacenterSource()
    df = source.get_holder_num("600519", page_size=5)
    assert isinstance(df, pd.DataFrame)


def test_dividend_returns_data():
    """验证分红送转数据拉取（smoke test）"""
    from src.sources.eastmoney_datacenter import EastMoneyDatacenterSource
    source = EastMoneyDatacenterSource()
    df = source.get_dividend("600519", page_size=5)
    assert isinstance(df, pd.DataFrame)


def test_lockup_expiry_returns_data():
    """验证限售解禁数据拉取（smoke test）"""
    from src.sources.eastmoney_datacenter import EastMoneyDatacenterSource
    source = EastMoneyDatacenterSource()
    df = source.get_lockup_expiry("002475", "")
    assert isinstance(df, pd.DataFrame)


def test_northbound_source_exists():
    """验证北向资金源可以导入"""
    from src.sources.ths_northbound import ThsNorthboundSource
    source = ThsNorthboundSource()
    assert source is not None


def test_northbound_realtime_returns_data():
    """验证北向资金实时数据拉取（smoke test）"""
    from src.sources.ths_northbound import ThsNorthboundSource
    source = ThsNorthboundSource()
    df = source.get_daily_flow()
    assert isinstance(df, pd.DataFrame)
    if not df.empty:
        assert "date" in df.columns
        assert "hgt_yi" in df.columns
        assert "sgt_yi" in df.columns


def test_northbound_cache():
    """验证北向资金本地缓存读写"""
    from src.sources.ths_northbound import (
        _northbound_cache_path, _save_snapshot, _load_history
    )
    path = _northbound_cache_path()
    assert path is not None
    _save_snapshot("2026-01-01", 10.5, -3.2)
    df = _load_history(10)
    assert isinstance(df, pd.DataFrame)


def test_concept_blocks_returns_data():
    """验证概念板块归属数据拉取（smoke test）"""
    from src.sources.eastmoney_source import EastMoneySource
    source = EastMoneySource()
    df = source.get_concept_blocks("600519")
    assert isinstance(df, pd.DataFrame)
    if not df.empty:
        assert "board_name" in df.columns
        assert "board_code" in df.columns
        assert "ts_code" in df.columns
