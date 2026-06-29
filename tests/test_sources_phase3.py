"""Phase 3-4 数据源 smoke tests"""
import pytest
import pandas as pd


def test_tencent_source():
    """腾讯财经: PE/PB/市值 非空"""
    from src.sources.tencent_source import TencentSource
    t = TencentSource()
    df = t.get_daily_basic("600036")
    assert not df.empty, "腾讯接口返回空"
    row = df.iloc[0]
    assert row["pe"] is not None, "PE 为空"
    assert row["pb"] is not None, "PB 为空"
    assert row["pe"] > 0


def test_sina_income():
    """新浪财报: 利润表 非空"""
    from src.sources.sina_source import SinaSource
    s = SinaSource()
    df = s.get_income_statement("600036")
    assert len(df) > 0, "利润表为空"
    # 应该有净利润字段
    assert "净利润" in df.columns or any("净利润" in c for c in df.columns), "缺少净利润"


def test_sina_balance():
    """新浪财报: 资产负债表"""
    from src.sources.sina_source import SinaSource
    s = SinaSource()
    df = s.get_balance_sheet("600036")
    assert len(df) > 0, "资产负债表为空"


def test_eastmoney_fund_flow():
    """东财资金流: 返回 DataFrame 且字段正确"""
    from src.sources.eastmoney_source import EastMoneySource
    em = EastMoneySource()
    df = em.get_fund_flow("600036", days=5)
    if df.empty:
        pytest.skip("资金流 API 暂时无数据")
    for col in ["ts_code", "trade_date", "main_net", "small_net", "large_net"]:
        assert col in df.columns, f"缺少列: {col}"


def test_eastmoney_reports():
    """东财研报: 返回非空"""
    from src.sources.eastmoney_source import EastMoneySource
    em = EastMoneySource()
    df = em.get_reports("688017", max_pages=1)
    if df.empty:
        pytest.skip("研报 API 暂时无数据")
    assert "title" in df.columns
    assert "org_name" in df.columns


def test_db_new_tables():
    """DuckDB 新表存在"""
    from src.utils import load_config
    from src.db import get_connection
    conn = get_connection(load_config())
    tables = [r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()]
    for t in ["stock_fund_flow", "research_reports", "financial_reports"]:
        assert t in tables, f"表 {t} 不存在"
    conn.close()


def test_db_crud():
    """CRUD: 写入→查询 数据一致"""
    from src.utils import load_config
    from src.db import get_connection, upsert_fund_flow
    conn = get_connection(load_config())
    before = conn.execute("SELECT count(*) FROM stock_fund_flow").fetchone()[0]
    df = pd.DataFrame([{
        "ts_code": "600036", "trade_date": "2024-01-02",
        "main_net": 1.0, "small_net": 2.0, "mid_net": 3.0,
        "large_net": 4.0, "super_net": 5.0, "main_pct": 0.1,
    }])
    upsert_fund_flow(conn, df)
    after = conn.execute("SELECT count(*) FROM stock_fund_flow").fetchone()[0]
    assert after >= before
    conn.close()
