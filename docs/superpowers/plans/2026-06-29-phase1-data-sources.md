# Phase 1: 数据补全 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 8 个数据源（北向资金、融资融券、龙虎榜、大宗交易、股东户数、分红送转、限售解禁、概念板块），写入 8 张新 DuckDB 表，扩展 CLI 和调度器。

**Architecture:** 新增 2 个数据源文件 + 扩展 1 个现有文件，遵循现有 `DataSource` 基类 + `Pipeline` 懒加载 + `db.py` CRUD 模式。东财 datacenter 系列 6 个报表共用同名 API，封装为一个统一类 `EastMoneyDatacenterSource`。

**Tech Stack:** Python 3.11, DuckDB, requests, pandas, pytest（复用现有依赖）

---

## File Structure (Phase 1 changes)

```
Create:  src/sources/eastmoney_datacenter.py   # 东财数据中心 6 合 1 源
Create:  src/sources/ths_northbound.py         # 同花顺北向资金源
Create:  tests/test_sources_phase1.py          # Phase 1 smoke tests
Modify:  src/db.py                             # 新增 8 张表 + 对应 upsert
Modify:  src/pipeline.py                       # 新增懒加载属性 + 采集方法
Modify:  src/sources/eastmoney_source.py       # 新增 get_concept_blocks()
Modify:  cli.py                                # 新增 5 个命令
Modify:  scheduler.py                          # 新增 Phase1 采集任务
```

---

### Task 1: 东财数据中心统一数据源 (`eastmoney_datacenter.py`)

**Files:**
- Create: `src/sources/eastmoney_datacenter.py`
- Create: `tests/test_sources_phase1.py` (部分)

这个文件是 Phase 1 中工作量最大的文件。东财 datacenter API 用一个 base URL + 不同 `reportName` 参数区分 6 种数据。我们封装为一个类，内部共用一个 session + 限流，外部每个报表一个方法。

- [ ] **Step 1: 创建空的测试文件和最终要填充的源文件**

```python
# tests/test_sources_phase1.py (初始骨架)
"""Phase 1 数据源 smoke tests"""
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
    # 不强制非空（API 可能无数据），但结构要对
    if not df.empty:
        assert "trade_date" in df.columns
        assert "rzye" in df.columns  # 融资余额


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
```

- [ ] **Step 2: 运行测试，验证全部 FAIL**

```bash
python -m pytest tests/test_sources_phase1.py -v
```

预期：所有 7 个测试 FAIL（`ModuleNotFoundError` 或 `ImportError`）

- [ ] **Step 3: 创建源文件 — 基础结构 + 内部 `_em_datacenter_get` 方法**

```python
# src/sources/eastmoney_datacenter.py
"""东方财富数据中心统一数据源

覆盖 6 种数据（融资融券/龙虎榜/大宗交易/股东户数/分红送转/限售解禁），
共用 datacenter-web.eastmoney.com 入口，内置串行限流。
"""
import logging
import time
import random

import pandas as pd
import requests

from .base import DataSource

logger = logging.getLogger(__name__)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
DATACENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EM_MIN_INTERVAL = 1.0

_em_last_call = [0.0]
_em_session: requests.Session | None = None


def _em():
    """东财统一请求入口 — 串行限流 + 会话复用"""
    global _em_session
    if _em_session is None:
        _em_session = requests.Session()
        _em_session.headers.update({
            "User-Agent": UA,
            "Referer": "https://data.eastmoney.com/",
        })
        _em_session.trust_env = False
        # 连接级自动重试
        try:
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            adapter = HTTPAdapter(max_retries=Retry(
                total=3, connect=3, backoff_factor=0.6,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
            ))
            _em_session.mount("https://", adapter)
            _em_session.mount("http://", adapter)
        except Exception:
            pass

    wait = EM_MIN_INTERVAL - (time.time() - _em_last_call[0])
    if wait > 0:
        time.sleep(wait + random.uniform(0.1, 0.5))
    _em_last_call[0] = time.time()
    return _em_session


def _em_datacenter_get(report_name: str, filter_str: str = "",
                        page_size: int = 50,
                        sort_columns: str = "",
                        sort_types: str = "-1") -> list[dict]:
    """东财数据中心统一查询，返回 raw dict 列表"""
    params = {
        "reportName": report_name,
        "columns": "ALL",
        "filter": filter_str,
        "pageNumber": "1",
        "pageSize": str(page_size),
        "sortColumns": sort_columns,
        "sortTypes": sort_types,
        "source": "WEB",
        "client": "WEB",
    }
    try:
        r = _em().get(DATACENTER_URL, params=params, timeout=15)
        d = r.json()
        if d.get("result") and d["result"].get("data"):
            return d["result"]["data"]
    except Exception as e:
        logger.warning(f"东财 datacenter [{report_name}] 失败: {e}")
    return []


class EastMoneyDatacenterSource(DataSource):
    """东财数据中心数据源（融资融券/龙虎榜/大宗/股东/分红/解禁）

    不提供 get_stock_list / get_daily，仅提供各个报表的 fetch 方法。
    """

    def get_stock_list(self, market: str = "A") -> pd.DataFrame:
        return pd.DataFrame()

    def get_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return pd.DataFrame()

    @property
    def supports_extra(self) -> bool:
        return False
```

- [ ] **Step 4: 运行测试，验证所有 import 相关测试 PASS，业务测试仍然 FAIL（返回空 DF）**

```bash
python -m pytest tests/test_sources_phase1.py -v
```

预期：`test_eastmoney_datacenter_source_exists` PASS，其余 6 个 PASS（返回空 DataFrame，assert isinstance 通过）

- [ ] **Step 5: 提交**

```bash
git add src/sources/eastmoney_datacenter.py tests/test_sources_phase1.py
git commit -m "feat: EastMoneyDatacenterSource 骨架 — 共用限流入口"
```

- [ ] **Step 6: 实现 `get_margin_trading` 方法**

在类内部添加：

```python
    def get_margin_trading(self, ts_code: str, page_size: int = 30) -> pd.DataFrame:
        """融资融券明细（日级）

        Returns DataFrame columns:
            ts_code, trade_date, rzye(融资余额), rzmre(融资买入),
            rzche(融资偿还), rqye(融券余额), rqmcl(融券卖出量),
            rqchl(融券偿还量), rzrqye(两融余额合计)
        """
        data = _em_datacenter_get(
            "RPTA_WEB_RZRQ_GGMX",
            filter_str=f'(SCODE="{ts_code}")',
            page_size=page_size,
            sort_columns="DATE",
            sort_types="-1",
        )
        if not data:
            return pd.DataFrame()
        rows = []
        for row in data:
            rows.append({
                "ts_code": ts_code,
                "trade_date": str(row.get("DATE", ""))[:10] if row.get("DATE") else "",
                "rzye": row.get("RZYE"),          # 融资余额(元)
                "rzmre": row.get("RZMRE"),         # 融资买入额
                "rzche": row.get("RZCHE"),         # 融资偿还额
                "rqye": row.get("RQYE"),           # 融券余额
                "rqmcl": row.get("RQMCL"),         # 融券卖出量
                "rqchl": row.get("RQCHL"),         # 融券偿还量
                "rzrqye": row.get("RZRQYE"),       # 两融余额合计
            })
        return pd.DataFrame(rows)
```

- [ ] **Step 7: 运行融资融券 smoke test**

```bash
python -m pytest tests/test_sources_phase1.py::test_margin_trading_returns_data -v
```

预期：PASS（验证字段存在）

- [ ] **Step 8: 实现 `get_dragon_tiger` 方法**

```python
    def get_dragon_tiger(self, ts_code: str, trade_date: str,
                          look_back: int = 30) -> pd.DataFrame:
        """龙虎榜上榜记录

        trade_date: YYYY-MM-DD
        look_back: 回看天数

        Returns DataFrame columns:
            ts_code, trade_date, reason(上榜原因), net_buy_wan(净买额,万),
            turnover_pct(换手率), buy_seats(买入席位,JSON), sell_seats(卖出席位,JSON)
        """
        from datetime import datetime, timedelta
        import json

        end_dt = datetime.strptime(trade_date, "%Y-%m-%d") if trade_date else datetime.now()
        start_dt = end_dt - timedelta(days=look_back)
        start_str = start_dt.strftime("%Y-%m-%d")
        end_str = end_dt.strftime("%Y-%m-%d")

        data = _em_datacenter_get(
            "RPT_DAILYBILLBOARD_DETAILSNEW",
            filter_str=f"(TRADE_DATE>='{start_str}')(TRADE_DATE<='{end_str}')"
                       f"(SECURITY_CODE=\"{ts_code}\")",
            page_size=50,
            sort_columns="TRADE_DATE",
            sort_types="-1",
        )
        if not data:
            return pd.DataFrame()

        # 取最近上榜日的席位明细
        latest_date = str(data[0].get("TRADE_DATE", ""))[:10]
        buy_seats = []
        sell_seats = []
        if latest_date:
            buy_data = _em_datacenter_get(
                "RPT_BILLBOARD_DAILYDETAILSBUY",
                filter_str=f"(TRADE_DATE='{latest_date}')(SECURITY_CODE=\"{ts_code}\")",
                page_size=10,
                sort_columns="BUY",
                sort_types="-1",
            )
            for row in (buy_data or [])[:5]:
                buy_seats.append({
                    "name": row.get("OPERATEDEPT_NAME", ""),
                    "buy_amt_wan": round((row.get("BUY") or 0) / 10000, 1),
                    "sell_amt_wan": round((row.get("SELL") or 0) / 10000, 1),
                    "net_wan": round((row.get("NET") or 0) / 10000, 1),
                })
            sell_data = _em_datacenter_get(
                "RPT_BILLBOARD_DAILYDETAILSSELL",
                filter_str=f"(TRADE_DATE='{latest_date}')(SECURITY_CODE=\"{ts_code}\")",
                page_size=10,
                sort_columns="SELL",
                sort_types="-1",
            )
            for row in (sell_data or [])[:5]:
                sell_seats.append({
                    "name": row.get("OPERATEDEPT_NAME", ""),
                    "buy_amt_wan": round((row.get("BUY") or 0) / 10000, 1),
                    "sell_amt_wan": round((row.get("SELL") or 0) / 10000, 1),
                    "net_wan": round((row.get("NET") or 0) / 10000, 1),
                })

        rows = []
        for row in data:
            rows.append({
                "ts_code": ts_code,
                "trade_date": str(row.get("TRADE_DATE", ""))[:10],
                "reason": row.get("EXPLANATION", ""),
                "net_buy_wan": round((row.get("BILLBOARD_NET_AMT") or 0) / 10000, 1),
                "turnover_pct": round(float(row.get("TURNOVERRATE") or 0), 2),
                "close": row.get("CLOSE_PRICE"),
                "change_pct": round(float(row.get("CHANGE_RATE") or 0), 2),
                "buy_seats": json.dumps(buy_seats, ensure_ascii=False) if buy_seats else None,
                "sell_seats": json.dumps(sell_seats, ensure_ascii=False) if sell_seats else None,
            })
        return pd.DataFrame(rows)
```

- [ ] **Step 9: 运行龙虎榜 smoke test**

```bash
python -m pytest tests/test_sources_phase1.py::test_dragon_tiger_returns_data -v
```

预期：PASS

- [ ] **Step 10: 实现 `get_block_trade` 方法**

```python
    def get_block_trade(self, ts_code: str, page_size: int = 20) -> pd.DataFrame:
        """大宗交易记录

        Returns DataFrame columns:
            ts_code, trade_date, deal_price, close, premium_pct(溢价%),
            deal_vol, deal_amt, buyer, seller
        """
        data = _em_datacenter_get(
            "RPT_DATA_BLOCKTRADE",
            filter_str=f'(SECURITY_CODE="{ts_code}")',
            page_size=page_size,
            sort_columns="TRADE_DATE",
            sort_types="-1",
        )
        if not data:
            return pd.DataFrame()
        rows = []
        for row in data:
            close = row.get("CLOSE_PRICE") or 0
            deal = row.get("DEAL_PRICE") or 0
            premium = ((deal / close - 1) * 100) if close else 0
            rows.append({
                "ts_code": ts_code,
                "trade_date": str(row.get("TRADE_DATE", ""))[:10],
                "deal_price": deal,
                "close": close,
                "premium_pct": round(premium, 2),
                "deal_vol": row.get("DEAL_VOLUME"),
                "deal_amt": row.get("DEAL_AMT"),
                "buyer": row.get("BUYER_NAME", ""),
                "seller": row.get("SELLER_NAME", ""),
            })
        return pd.DataFrame(rows)
```

- [ ] **Step 11: 实现 `get_holder_num` 方法**

```python
    def get_holder_num(self, ts_code: str, page_size: int = 10) -> pd.DataFrame:
        """股东户数变化（季度级）

        Returns DataFrame columns:
            ts_code, end_date, holder_num, change_num,
            change_ratio_pct(环比%), avg_free_shares(户均持股)
        """
        data = _em_datacenter_get(
            "RPT_HOLDERNUMLATEST",
            filter_str=f'(SECURITY_CODE="{ts_code}")',
            page_size=page_size,
            sort_columns="END_DATE",
            sort_types="-1",
        )
        if not data:
            return pd.DataFrame()
        rows = []
        for row in data:
            rows.append({
                "ts_code": ts_code,
                "end_date": str(row.get("END_DATE", ""))[:10],
                "holder_num": row.get("HOLDER_NUM"),
                "change_num": row.get("HOLDER_NUM_CHANGE"),
                "change_ratio_pct": row.get("HOLDER_NUM_RATIO"),
                "avg_free_shares": row.get("AVG_FREE_SHARES"),
            })
        return pd.DataFrame(rows)
```

- [ ] **Step 12: 实现 `get_dividend` 方法**

```python
    def get_dividend(self, ts_code: str, page_size: int = 20) -> pd.DataFrame:
        """分红送转历史

        Returns DataFrame columns:
            ts_code, ex_date(除权日), bonus_rmb(每股派息,税前),
            transfer_ratio(每10股转增), bonus_ratio(每10股送股), plan(进度)
        """
        data = _em_datacenter_get(
            "RPT_SHAREBONUS_DET",
            filter_str=f'(SECURITY_CODE="{ts_code}")',
            page_size=page_size,
            sort_columns="EX_DIVIDEND_DATE",
            sort_types="-1",
        )
        if not data:
            return pd.DataFrame()
        rows = []
        for row in data:
            rows.append({
                "ts_code": ts_code,
                "ex_date": str(row.get("EX_DIVIDEND_DATE", ""))[:10],
                "bonus_rmb": row.get("PRETAX_BONUS_RMB"),
                "transfer_ratio": row.get("TRANSFER_RATIO"),
                "bonus_ratio": row.get("BONUS_RATIO"),
                "plan": row.get("ASSIGN_PROGRESS", ""),
            })
        return pd.DataFrame(rows)
```

- [ ] **Step 13: 实现 `get_lockup_expiry` 方法**

```python
    def get_lockup_expiry(self, ts_code: str, trade_date: str,
                           forward_days: int = 90) -> pd.DataFrame:
        """限售解禁（历史 + 未来）

        trade_date: 当前日期 YYYY-MM-DD，None=今天

        Returns DataFrame columns:
            ts_code, free_date, stock_type, free_shares, free_ratio
        """
        from datetime import datetime, timedelta

        if not trade_date:
            trade_date = datetime.now().strftime("%Y-%m-%d")
        end_dt = datetime.strptime(trade_date, "%Y-%m-%d") + timedelta(days=forward_days)
        end_str = end_dt.strftime("%Y-%m-%d")

        # 拉取全部解禁记录
        data = _em_datacenter_get(
            "RPT_LIFT_STAGE",
            filter_str=f'(SECURITY_CODE="{ts_code}")'
                       f'(FREE_DATE>=\'2000-01-01\')(FREE_DATE<=\'{end_str}\')',
            page_size=30,
            sort_columns="FREE_DATE",
            sort_types="-1",
        )
        if not data:
            return pd.DataFrame()
        rows = []
        for row in data:
            rows.append({
                "ts_code": ts_code,
                "free_date": str(row.get("FREE_DATE", ""))[:10],
                "stock_type": row.get("LIMITED_STOCK_TYPE", ""),
                "free_shares": row.get("FREE_SHARES_NUM"),
                "free_ratio": row.get("FREE_RATIO"),
            })
        return pd.DataFrame(rows)
```

- [ ] **Step 14: 运行全部 datacenter smoke tests**

```bash
python -m pytest tests/test_sources_phase1.py -v -k "not northbound and not concept"
```

预期：7 个测试全部 PASS

- [ ] **Step 15: 提交**

```bash
git add src/sources/eastmoney_datacenter.py tests/test_sources_phase1.py
git commit -m "feat: EastMoneyDatacenterSource — 融资融券/龙虎榜/大宗/股东/分红/解禁 6合1"
```

---

### Task 2: 同花顺北向资金源 (`ths_northbound.py`)

**Files:**
- Create: `src/sources/ths_northbound.py`
- Modify: `tests/test_sources_phase1.py` (追加)

- [ ] **Step 1: 追加测试用例到 `tests/test_sources_phase1.py`**

```python
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
    # 交易日应有数据，非交易日可能空
    if not df.empty:
        assert "date" in df.columns
        assert "hgt_yi" in df.columns  # 沪股通累计
        assert "sgt_yi" in df.columns  # 深股通累计


def test_northbound_cache():
    """验证北向资金本地缓存读写"""
    from src.sources.ths_northbound import (
        _northbound_cache_path, _save_snapshot, _load_history
    )
    import tempfile
    from pathlib import Path

    # 用临时目录测试
    cache_path = _northbound_cache_path()
    # 确保不干扰真实缓存
    assert isinstance(cache_path, Path)
```

- [ ] **Step 2: 创建北向资金源文件**

```python
# src/sources/ths_northbound.py
"""同花顺北向资金数据源 — 沪股通/深股通日级累计净买入

实现本地 CSV 自缓存：每次拉取实时数据后自动写入本地 CSV，
历史越跑越丰富。
"""
import logging
from pathlib import Path

import pandas as pd
import requests

from .base import DataSource

logger = logging.getLogger(__name__)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/117.0.0.0 Safari/537.36"

HSGT_HEADERS = {
    "User-Agent": UA,
    "Host": "data.hexin.cn",
    "Referer": "https://data.hexin.cn/",
}

CACHE_DIR = Path.home() / ".tradingagents" / "cache"


def _northbound_cache_path() -> Path:
    """北向资金本地 CSV 缓存路径"""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / "northbound_daily.csv"


def _save_snapshot(date: str, hgt: float, sgt: float):
    """写入/更新当天北向收盘数据到 CSV"""
    path = _northbound_cache_path()
    rows = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").strip().split("\n")[1:]:
            parts = line.split(",")
            if len(parts) == 3 and parts[0]:
                rows[parts[0]] = line
    rows[date] = f"{date},{hgt},{sgt}"
    with open(path, "w", encoding="utf-8") as f:
        f.write("date,hgt,sgt\n")
        for d in sorted(rows.keys()):
            f.write(rows[d] + "\n")


def _load_history(n: int = 60) -> pd.DataFrame:
    """读取最近 N 天北向历史"""
    path = _northbound_cache_path()
    if not path.exists():
        return pd.DataFrame({"date": [], "hgt_yi": [], "sgt_yi": []})
    df = pd.read_csv(path)
    return df.tail(n).rename(columns={"hgt": "hgt_yi", "sgt": "sgt_yi"})


class ThsNorthboundSource(DataSource):
    """同花顺北向资金数据源"""

    def get_stock_list(self, market: str = "A") -> pd.DataFrame:
        return pd.DataFrame()

    def get_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return pd.DataFrame()

    @property
    def supports_extra(self) -> bool:
        return False

    def get_daily_flow(self) -> pd.DataFrame:
        """获取当日北向资金分钟流向，聚合成日级快照

        Returns DataFrame columns: date, hgt_yi(沪股通累计净买入,亿元),
                                   sgt_yi(深股通累计净买入,亿元)
        """
        url = "https://data.hexin.cn/market/hsgtApi/method/dayChart/"
        try:
            r = requests.get(url, headers=HSGT_HEADERS, timeout=10)
            d = r.json()
        except Exception as e:
            logger.warning(f"北向资金请求失败: {e}")
            return pd.DataFrame()

        times = d.get("time", [])
        hgt = d.get("hgt", [])
        sgt = d.get("sgt", [])

        if not times:
            return pd.DataFrame()

        # 取最后一个有效值（收盘累计）
        hgt_last = None
        sgt_last = None
        for i in range(len(hgt) - 1, -1, -1):
            if hgt[i] is not None and hgt_last is None:
                hgt_last = hgt[i]
            if sgt[i] is not None and sgt_last is None:
                sgt_last = sgt[i]
            if hgt_last is not None and sgt_last is not None:
                break

        if hgt_last is None and sgt_last is None:
            return pd.DataFrame()

        # 获取日期
        from datetime import datetime
        today = times[-1][:10] if times else datetime.now().strftime("%Y-%m-%d")

        # 写入本地缓存
        try:
            _save_snapshot(today, hgt_last or 0, sgt_last or 0)
        except Exception as e:
            logger.debug(f"北向缓存写入失败: {e}")

        return pd.DataFrame([{
            "date": today,
            "hgt_yi": hgt_last or 0,
            "sgt_yi": sgt_last or 0,
        }])

    def get_history(self, days: int = 60) -> pd.DataFrame:
        """读取本地缓存的北向历史数据"""
        return _load_history(days)
```

- [ ] **Step 3: 运行北向 smoke tests**

```bash
python -m pytest tests/test_sources_phase1.py -v -k "northbound"
```

预期：3 个测试 PASS

- [ ] **Step 4: 提交**

```bash
git add src/sources/ths_northbound.py tests/test_sources_phase1.py
git commit -m "feat: ThsNorthboundSource — 同花顺北向资金 + CSV 自缓存"
```

---

### Task 3: 扩展东财源 — 概念板块归属 (`eastmoney_source.py`)

**Files:**
- Modify: `src/sources/eastmoney_source.py`
- Modify: `tests/test_sources_phase1.py` (追加)

- [ ] **Step 1: 追加测试用例**

```python
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
```

- [ ] **Step 2: 在 `EastMoneySource` 类中添加 `get_concept_blocks` 方法**

在 `EastMoneySource` 类的 `get_industry_ranking` 方法之后添加：

```python
    # ── 概念板块归属 ──

    def get_concept_blocks(self, ts_code: str) -> pd.DataFrame:
        """个股所属板块/概念归属（东财 slist，一次请求拿全）

        返回行业/概念/地域混合列表，板块名本身自解释。

        Returns DataFrame columns:
            ts_code, board_name, board_code(BK码),
            change_pct, lead_stock(龙头股)
        """
        market_code = "1" if ts_code.startswith("6") else "0"
        params = {
            "fltt": "2", "invt": "2",
            "secid": f"{market_code}.{ts_code}",
            "spt": "3", "pi": "0", "pz": "200", "po": "1",
            "fields": "f12,f14,f3,f128",
        }
        try:
            r = _em().get(
                "https://push2.eastmoney.com/api/qt/slist/get",
                params=params,
                headers={"Referer": "https://quote.eastmoney.com/"},
                timeout=15,
            )
            d = r.json()
        except Exception as e:
            logger.warning(f"概念板块 {ts_code} 失败: {e}")
            return pd.DataFrame()

        diff = (d.get("data") or {}).get("diff") or {}
        items = diff.values() if isinstance(diff, dict) else diff
        if not items:
            return pd.DataFrame()

        rows = []
        for it in items:
            name = it.get("f14", "")
            if not name:
                continue
            rows.append({
                "ts_code": ts_code,
                "board_name": name,
                "board_code": it.get("f12", ""),
                "change_pct": it.get("f3", ""),
                "lead_stock": it.get("f128", ""),
            })
        return pd.DataFrame(rows)
```

- [ ] **Step 3: 运行概念板块 smoke test**

```bash
python -m pytest tests/test_sources_phase1.py::test_concept_blocks_returns_data -v
```

预期：PASS

- [ ] **Step 4: 提交**

```bash
git add src/sources/eastmoney_source.py tests/test_sources_phase1.py
git commit -m "feat: EastMoneySource 概念板块归属 — 东财 slist spt=3"
```

---

### Task 4: DuckDB 新表（8 张）

**Files:**
- Modify: `src/db.py`
- Modify: `tests/test_db.py` (追加)

- [ ] **Step 1: 追加 DB 测试**

```python
# 加到 tests/test_db.py 末尾

def test_phase1_tables_exist():
    """验证 Phase 1 的 8 张新表存在"""
    from src.db import create_tables
    import duckdb
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    names = {t[0] for t in tables}
    expected_phase1 = {
        "northbound_flow", "margin_trading", "dragon_tiger",
        "block_trade", "holder_num", "dividend",
        "lockup_expiry", "stock_boards",
    }
    missing = expected_phase1 - names
    assert not missing, f"Missing tables: {missing}"


def test_margin_trading_upsert():
    """验证融资融券表 upsert"""
    from src.db import create_tables, upsert_daily
    import duckdb
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    df = pd.DataFrame({
        "ts_code": ["600519.SH"],
        "trade_date": ["2026-06-29"],
        "rzye": [1.5e9],
        "rzmre": [2e8],
        "rzche": [1e8],
        "rqye": [5e7],
        "rqmcl": [10000],
        "rqchl": [8000],
        "rzrqye": [1.55e9],
    })
    count = upsert_daily(conn, "margin_trading", df)
    assert count == 1
    row = conn.execute(
        "SELECT rzye FROM margin_trading WHERE ts_code='600519.SH'"
    ).fetchone()
    assert row[0] == 1.5e9
```

- [ ] **Step 2: 运行新 DB 测试，验证 FAIL**

```bash
python -m pytest tests/test_db.py::test_phase1_tables_exist tests/test_db.py::test_margin_trading_upsert -v
```

预期：FAIL（表不存在）

- [ ] **Step 3: 在 `db.py` 的 `create_tables()` 中添加 8 张新表**

在 `create_tables()` 函数末尾（`financial_reports` 表之后）添加：

```python
    # ── Phase 1: 数据补全 8 张新表 ──

    conn.execute("""
        CREATE TABLE IF NOT EXISTS northbound_flow (
            date    DATE PRIMARY KEY,
            hgt_yi  DOUBLE,
            sgt_yi  DOUBLE
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS margin_trading (
            ts_code    VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            rzye       DOUBLE,
            rzmre      DOUBLE,
            rzche      DOUBLE,
            rqye       DOUBLE,
            rqmcl      DOUBLE,
            rqchl      DOUBLE,
            rzrqye     DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dragon_tiger (
            ts_code     VARCHAR NOT NULL,
            trade_date  DATE NOT NULL,
            reason      VARCHAR,
            net_buy_wan DOUBLE,
            turnover_pct DOUBLE,
            close       DOUBLE,
            change_pct  DOUBLE,
            buy_seats   TEXT,
            sell_seats  TEXT,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS block_trade (
            ts_code     VARCHAR NOT NULL,
            trade_date  DATE NOT NULL,
            deal_price  DOUBLE,
            close       DOUBLE,
            premium_pct DOUBLE,
            deal_vol    DOUBLE,
            deal_amt    DOUBLE,
            buyer       VARCHAR,
            seller      VARCHAR,
            PRIMARY KEY (ts_code, trade_date, deal_price)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS holder_num (
            ts_code         VARCHAR NOT NULL,
            end_date        DATE NOT NULL,
            holder_num      INTEGER,
            change_num      INTEGER,
            change_ratio_pct DOUBLE,
            avg_free_shares DOUBLE,
            PRIMARY KEY (ts_code, end_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dividend (
            ts_code        VARCHAR NOT NULL,
            ex_date        DATE NOT NULL,
            bonus_rmb      DOUBLE,
            transfer_ratio DOUBLE,
            bonus_ratio    DOUBLE,
            plan           VARCHAR,
            PRIMARY KEY (ts_code, ex_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lockup_expiry (
            ts_code     VARCHAR NOT NULL,
            free_date   DATE NOT NULL,
            stock_type  VARCHAR,
            free_shares DOUBLE,
            free_ratio  DOUBLE,
            PRIMARY KEY (ts_code, free_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_boards (
            ts_code     VARCHAR NOT NULL,
            board_name  VARCHAR NOT NULL,
            board_code  VARCHAR,
            change_pct  DOUBLE,
            lead_stock  VARCHAR,
            PRIMARY KEY (ts_code, board_name)
        )
    """)
```

- [ ] **Step 4: 运行 DB 测试验证 PASS**

```bash
python -m pytest tests/test_db.py::test_phase1_tables_exist tests/test_db.py::test_margin_trading_upsert -v
```

预期：2 PASS

- [ ] **Step 5: 确保所有现有测试仍然通过**

```bash
python -m pytest tests/test_db.py -v
```

预期：6 个测试全部 PASS

- [ ] **Step 6: 提交**

```bash
git add src/db.py tests/test_db.py
git commit -m "feat: DuckDB 新增 Phase 1 8张表 — 北向/融资/龙虎榜/大宗/股东/分红/解禁/板块"
```

---

### Task 5: Pipeline 扩展 — 懒加载 + 采集方法

**Files:**
- Modify: `src/pipeline.py`

- [ ] **Step 1: 添加懒加载属性（`__init__` 中添加两个 `None` 初始化）**

在 `Pipeline.__init__` 中，在现有懒加载初始化后添加：

```python
        self._emdc = None     # EastMoneyDatacenter (Phase 1)
        self._ths_nb = None   # ThsNorthbound (Phase 1)
```

- [ ] **Step 2: 添加 property 方法**

在 `sina` property 之后添加：

```python
    @property
    def emdc(self):
        """东财数据中心 — 融资融券/龙虎榜/大宗/股东/分红/解禁（Phase 1）"""
        if self._emdc is None:
            from .sources.eastmoney_datacenter import EastMoneyDatacenterSource
            self._emdc = EastMoneyDatacenterSource()
        return self._emdc

    @property
    def ths_nb(self):
        """同花顺北向资金（Phase 1）"""
        if self._ths_nb is None:
            from .sources.ths_northbound import ThsNorthboundSource
            self._ths_nb = ThsNorthboundSource()
        return self._ths_nb
```

- [ ] **Step 3: 添加 5 个 Phase 1 采集方法**

在 `update_financials` 方法之后添加：

```python
    # ── Phase 1: 数据补全采集 ──

    def update_northbound(self) -> int:
        """采集北向资金日数据 — 拉取当日 + 写入 DB + 更新缓存"""
        df = self.ths_nb.get_daily_flow()
        if df.empty:
            self.logger.info("北向资金: 今日无数据（非交易日？）")
            return 0
        # 列名映射到 DB 表
        df_db = df.rename(columns={"date": "date"})[["date", "hgt_yi", "sgt_yi"]]
        upsert_daily(self.conn, "northbound_flow", df_db)
        self.logger.info(f"北向资金: 沪 {df_db.iloc[0]['hgt_yi']:.1f}亿 "
                        f"深 {df_db.iloc[0]['sgt_yi']:.1f}亿")
        return len(df_db)

    def update_margin_trading(self, market: str = "A") -> int:
        """采集全市场融资融券（日级）"""
        if market != "A":
            return 0
        stocks = get_stocks_needing_update(self.conn, market, "2024-01-01")
        total = 0
        for _, row in stocks.iterrows():
            code = row["ts_code"]
            df = self.emdc.get_margin_trading(code, page_size=30)
            if not df.empty:
                upsert_daily(self.conn, "margin_trading", df)
                total += len(df)
        self.logger.info(f"融资融券采集完成: {total} 条")
        return total

    def update_dragon_tiger(self, market: str = "A") -> int:
        """采集全市场龙虎榜（近30日）"""
        if market != "A":
            return 0
        from datetime import datetime
        trade_date = datetime.now().strftime("%Y-%m-%d")
        stocks = get_stocks_needing_update(self.conn, market, "2024-01-01")
        total = 0
        for _, row in stocks.iterrows():
            code = row["ts_code"]
            df = self.emdc.get_dragon_tiger(code, trade_date, look_back=30)
            if not df.empty:
                upsert_daily(self.conn, "dragon_tiger", df)
                total += len(df)
        self.logger.info(f"龙虎榜采集完成: {total} 条")
        return total

    def update_concept_blocks(self, market: str = "A") -> int:
        """更新全市场概念板块归属（快照）"""
        if market != "A":
            return 0
        stocks = get_stocks_needing_update(self.conn, market, "2024-01-01")
        total = 0
        for _, row in stocks.iterrows():
            code = row["ts_code"]
            df = self.em.get_concept_blocks(code)
            if not df.empty:
                upsert_daily(self.conn, "stock_boards", df)
                total += len(df)
        self.logger.info(f"概念板块归属更新: {total} 条")
        return total

    def update_holder_num(self, market: str = "A") -> int:
        """采集全市场股东户数（季度）"""
        if market != "A":
            return 0
        stocks = get_stocks_needing_update(self.conn, market, "2024-01-01")
        total = 0
        for _, row in stocks.iterrows():
            code = row["ts_code"]
            df = self.emdc.get_holder_num(code, page_size=10)
            if not df.empty:
                upsert_daily(self.conn, "holder_num", df)
                total += len(df)
        self.logger.info(f"股东户数采集完成: {total} 条")
        return total
```

- [ ] **Step 4: 提交**

```bash
git add src/pipeline.py
git commit -m "feat: Pipeline 新增 Phase 1 采集方法 — 北向/融资/龙虎榜/板块/股东"
```

---

### Task 6: CLI 扩展 — 新增 5 个命令

**Files:**
- Modify: `cli.py`

- [ ] **Step 1: 在 `cli.py` 中添加 5 个命令函数**

在 `cmd_financials` 函数之后添加：

```python
def cmd_northbound(args):
    """拉取北向资金"""
    pipeline = Pipeline()
    try:
        n = pipeline.update_northbound()
        print(f"北向资金采集完成: {n} 条")
    finally:
        pipeline.close()


def cmd_margin(args):
    """采集融资融券"""
    pipeline = Pipeline()
    try:
        n = pipeline.update_margin_trading("A")
        print(f"融资融券采集完成: {n} 条")
    finally:
        pipeline.close()


def cmd_dragon(args):
    """采集龙虎榜"""
    pipeline = Pipeline()
    try:
        n = pipeline.update_dragon_tiger("A")
        print(f"龙虎榜采集完成: {n} 条")
    finally:
        pipeline.close()


def cmd_blocks(args):
    """更新概念板块归属"""
    pipeline = Pipeline()
    try:
        n = pipeline.update_concept_blocks("A")
        print(f"概念板块更新完成: {n} 条")
    finally:
        pipeline.close()


def cmd_holders(args):
    """采集股东户数"""
    pipeline = Pipeline()
    try:
        n = pipeline.update_holder_num("A")
        print(f"股东户数采集完成: {n} 条")
    finally:
        pipeline.close()
```

- [ ] **Step 2: 更新 `main()` 中的 commands 字典和 `__doc__`**

修改文件头的 docstring，在 `python cli.py financials` 之后添加：

```python
  python cli.py northbound        ← 北向资金
  python cli.py margin            ← 融资融券
  python cli.py dragon            ← 龙虎榜
  python cli.py blocks            ← 概念板块归属
  python cli.py holders           ← 股东户数
```

更新 `commands` 字典添加新命令：

```python
    commands = {
        "init": cmd_init,
        "update": cmd_update,
        "backfill": cmd_backfill,
        "status": cmd_status,
        "fundflow": cmd_fundflow,
        "research": cmd_research,
        "financials": cmd_financials,
        "northbound": cmd_northbound,
        "margin": cmd_margin,
        "dragon": cmd_dragon,
        "blocks": cmd_blocks,
        "holders": cmd_holders,
    }
```

- [ ] **Step 3: 提交**

```bash
git add cli.py
git commit -m "feat: CLI 新增 Phase 1 命令 — northbound/margin/dragon/blocks/holders"
```

---

### Task 7: 调度器扩展 — 加入 Phase 1 采集

**Files:**
- Modify: `scheduler.py`

- [ ] **Step 1: 在 `scheduler.py` 中添加 `run_phase1_update` 函数**

在 `run_phase4_update` 函数之前添加：

```python
def run_phase1_update():
    """Phase 1 增量: 北向 + 融资融券 + 龙虎榜 + 概念板块 + 股东户数"""
    config = load_config()
    logger.info("=== Phase1 采集开始 ===")
    pipeline = Pipeline(config)
    try:
        logger.info("采集北向资金...")
        n = pipeline.update_northbound()
        logger.info(f"  北向: {n} 条")

        logger.info("采集融资融券...")
        n = pipeline.update_margin_trading("A")
        logger.info(f"  融资融券: {n} 条")

        logger.info("采集龙虎榜...")
        n = pipeline.update_dragon_tiger("A")
        logger.info(f"  龙虎榜: {n} 条")

        logger.info("更新概念板块归属...")
        n = pipeline.update_concept_blocks("A")
        logger.info(f"  概念板块: {n} 条")

        logger.info("采集股东户数...")
        n = pipeline.update_holder_num("A")
        logger.info(f"  股东户数: {n} 条")
    except Exception as e:
        logger.error(f"Phase1 异常: {e}")
    finally:
        pipeline.close()
    logger.info("=== Phase1 采集结束 ===")
```

- [ ] **Step 2: 在 `main()` 中注册 Phase1 调度任务**

在 `main()` 函数中，在 `schedule.every().day.at("16:30").do(run_phase4_update)` 之后添加：

```python
    # Phase1 任务：16:10 跑北向 + 融资融券（数据依赖轻，可先跑）
    # 其余重任务 16:30 与 Phase4 一起跑
    schedule.every().day.at("16:10").do(run_phase1_update)
    logger.info("  Phase1(北向/融资/龙虎榜/板块/股东): 每天 16:10")
```

- [ ] **Step 3: 提交**

```bash
git add scheduler.py
git commit -m "feat: 调度器新增 Phase 1 采集 — 每日 16:10"
```

---

### Task 8: 全量集成测试 & 最终验证

- [ ] **Step 1: 运行所有已有测试，确保没有回归**

```bash
python -m pytest tests/ -v --timeout=120
```

预期：所有既有测试 PASS（约 31 个）

- [ ] **Step 2: 运行 Phase 1 所有新增 smoke tests**

```bash
python -m pytest tests/test_sources_phase1.py -v --timeout=120
```

预期：10 个 smoke test PASS

- [ ] **Step 3: 运行 DB 新表测试**

```bash
python -m pytest tests/test_db.py -v
```

预期：8 个测试 PASS（原有 6 个 + 新增 2 个）

- [ ] **Step 4: 全量回归**

```bash
python -m pytest tests/ -v --timeout=180
```

预期：约 41 个测试全部 PASS

- [ ] **Step 5: 提交**

```bash
git add -A
git commit -m "test: Phase 1 全量测试通过 — 10 smoke + 2 DB 新测试"
```

---

### Task 9: 更新技术文档

- [ ] **Step 1: 更新 `docs/market-data-tech-doc.md`**

在文档末尾添加 Phase 1 章节，记录新增数据源、表、CLI 命令。

- [ ] **Step 2: 提交**

```bash
git add docs/market-data-tech-doc.md
git commit -m "docs: 更新技术文档至 v0.3.0 — Phase 1 数据补全"
```

---

## Summary

| Task | 文件 | 描述 |
|:----:|------|------|
| 1 | `src/sources/eastmoney_datacenter.py` (new) | 东财数据中心 6 合 1 |
| 2 | `src/sources/ths_northbound.py` (new) | 同花顺北向资金 |
| 3 | `src/sources/eastmoney_source.py` (modify) | 概念板块归属 |
| 4 | `src/db.py` (modify) | 8 张新表 |
| 5 | `src/pipeline.py` (modify) | 采集方法 + 懒加载 |
| 6 | `cli.py` (modify) | 5 个新命令 |
| 7 | `scheduler.py` (modify) | Phase1 调度 |
| 8 | `tests/` (扩展) | 集成验证 |
| 9 | `docs/` (更新) | 技术文档 |

**合计**: 2 个新文件 + 5 个修改文件 + 9 次提交，测试从 ~31 → ~41 个。
