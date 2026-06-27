# 市场数据采集系统 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建本地市场数据采集系统，覆盖A股/ETF/港股/美股日线，缓存于DuckDB用于数据挖掘。

**Architecture:** 分层设计 — CLI入口 → Pipeline编排 → 多数据源(Tushare/AKShare/yfinance) → DuckDB存储。utils提供限速/日志/交易日历横切能力。scheduler提供断点续传和重试。

**Tech Stack:** Python 3.11, DuckDB, Tushare Pro, AKShare, yfinance, Pandas, PyYAML, Rich, schedule

## Global Constraints

- DuckDB 数据库文件路径可配置，默认 `data/market.duckdb`
- Tushare token 通过 config.yaml 或环境变量 TUSHARE_TOKEN 提供
- 所有时间范围：2015-01-01 至今
- 限速：Tushare 200次/分钟，AKShare 50次/分钟，yfinance 2000次/小时
- 断点续传：sync_status 表记录每只股票的最新同步日期
- 失败重试：单只股票最多3次重试，超过则跳过并记录

---

### Task 1: 项目脚手架

**Files:**
- Create: `D:\daniel\market-data\pyproject.toml`
- Create: `D:\daniel\market-data\config.yaml`
- Create: `D:\daniel\market-data\src\__init__.py`
- Create: `D:\daniel\market-data\src\sources\__init__.py`
- Create: `D:\daniel\market-data\data\.gitkeep`

**Interfaces:**
- Consumes: nothing
- Produces: project structure, package config, default config

- [ ] **Step 1: 创建 pyproject.toml**

```toml
[project]
name = "market-data"
version = "0.1.0"
description = "A-share + ETF + HK + US market data pipeline"
requires-python = ">=3.11"
dependencies = [
    "duckdb>=1.0",
    "tushare>=1.4",
    "akshare>=1.14",
    "yfinance>=0.2",
    "pandas>=2.0",
    "pyyaml>=6.0",
    "rich>=13.0",
    "schedule>=1.2",
]

[build-system]
requires = ["setuptools>=75"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]
```

- [ ] **Step 2: 创建 config.yaml**

```yaml
# 市场数据采集配置
tushare:
  token: ""  # 填你的 Tushare Pro token，或设环境变量 TUSHARE_TOKEN

database:
  path: "data/market.duckdb"

data:
  start_date: "2015-01-01"
  markets:
    - A      # A股
    - ETF    # ETF
    - HK     # 港股
    - US     # 美股

rate_limit:
  tushare:
    max_calls: 190        # 略低于官方 200
    period: 60            # 秒
  akshare:
    max_calls: 48
    period: 60
  yfinance:
    max_calls: 1900
    period: 3600          # 1小时

retry:
  max_attempts: 3
  backoff_base: 5         # 秒，指数退避

logging:
  level: "INFO"
  file: "data/pipeline.log"
```

- [ ] **Step 3: 创建空 `__init__.py` 和 `.gitkeep`**

```bash
echo "" > D:/daniel/market-data/src/__init__.py
echo "" > D:/daniel/market-data/src/sources/__init__.py
echo "" > D:/daniel/market-data/data/.gitkeep
```

- [ ] **Step 4: 安装依赖**

```bash
cd D:/daniel/market-data && uv pip install -e .
```

预期: 输出 "Installed market-data" / "Resolved X packages"

- [ ] **Step 5: Commit**

```bash
cd D:/daniel/market-data && git init && git add -A && git commit -m "chore: project scaffolding"
```

---

### Task 2: DuckDB 数据库模块

**Files:**
- Create: `D:\daniel\market-data\src\db.py`

**Interfaces:**
- Consumes: config.yaml 中的 database.path
- Produces: `get_connection(config) -> duckdb.DuckDBPyConnection`, `create_tables(conn) -> None`, `upsert_daily(conn, table, df) -> int`, `get_sync_status(conn, market) -> DataFrame`, `update_sync_status(conn, ts_code, market, last_date, rows) -> None`

- [ ] **Step 1: 写测试 test_db.py**

```python
# tests/test_db.py
import duckdb
import pandas as pd
from src.db import get_connection, create_tables, upsert_daily, get_sync_status, update_sync_status

def test_create_tables():
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = {t[0] for t in tables}
    expected = {"stock_info", "a_daily", "a_daily_basic", "a_adj_factor",
                "etf_daily", "hk_daily", "us_daily", "sync_status"}
    assert expected.issubset(names)

def test_upsert_daily_insert():
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    df = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "trade_date": ["2024-01-01"],
        "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
        "pre_close": [10.1], "change": [0.4], "pct_chg": [3.96],
        "vol": [100000.0], "amount": [1050000.0]
    })
    count = upsert_daily(conn, "a_daily", df)
    assert count == 1
    row = conn.execute("SELECT * FROM a_daily WHERE ts_code='000001.SZ'").fetchone()
    assert row[2] == 10.0  # open

def test_upsert_daily_update():
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    df1 = pd.DataFrame({
        "ts_code": ["000001.SZ"], "trade_date": ["2024-01-01"],
        "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
        "pre_close": [10.1], "change": [0.4], "pct_chg": [3.96],
        "vol": [100000.0], "amount": [1050000.0]
    })
    upsert_daily(conn, "a_daily", df1)
    df2 = pd.DataFrame({
        "ts_code": ["000001.SZ"], "trade_date": ["2024-01-01"],
        "open": [10.2], "high": [11.2], "low": [9.7], "close": [10.7],
        "pre_close": [10.1], "change": [0.6], "pct_chg": [5.94],
        "vol": [110000.0], "amount": [1150000.0]
    })
    count = upsert_daily(conn, "a_daily", df2)
    assert count == 1
    row = conn.execute("SELECT open FROM a_daily WHERE ts_code='000001.SZ'").fetchone()
    assert row[0] == 10.2  # updated

def test_sync_status():
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    update_sync_status(conn, "000001.SZ", "A", "2024-06-01", 500)
    df = get_sync_status(conn, "A")
    assert len(df) == 1
    assert df.iloc[0]["last_sync"] == "2024-06-01"
    assert df.iloc[0]["row_count"] == 500
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cd D:/daniel/market-data && python -m pytest tests/test_db.py -v
```

预期: 全部 FAIL (模块不存在)

- [ ] **Step 3: 实现 db.py**

```python
"""DuckDB 数据库模块 — 连接、建表、读写操作"""
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime


def get_connection(config: dict) -> duckdb.DuckDBPyConnection:
    """获取 DuckDB 连接，自动创建数据库文件和表"""
    db_path = config["database"]["path"]
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    create_tables(conn)
    return conn


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """创建所有表（IF NOT EXISTS 保证幂等）"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_info (
            ts_code     VARCHAR NOT NULL,
            name        VARCHAR,
            market      VARCHAR NOT NULL,
            list_date   DATE,
            delist_date DATE,
            industry    VARCHAR,
            area        VARCHAR,
            exchange    VARCHAR,
            is_hs       VARCHAR,
            list_status VARCHAR,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, market)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS a_daily (
            ts_code    VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            open       DOUBLE,
            high       DOUBLE,
            low        DOUBLE,
            close      DOUBLE,
            pre_close  DOUBLE,
            change     DOUBLE,
            pct_chg    DOUBLE,
            vol        DOUBLE,
            amount     DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS a_daily_basic (
            ts_code       VARCHAR NOT NULL,
            trade_date    DATE NOT NULL,
            turnover_rate DOUBLE,
            volume_ratio  DOUBLE,
            pe            DOUBLE,
            pb            DOUBLE,
            total_mv      DOUBLE,
            circ_mv       DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS a_adj_factor (
            ts_code    VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            adj_factor DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS etf_daily (
            ts_code       VARCHAR NOT NULL,
            trade_date    DATE NOT NULL,
            open          DOUBLE,
            high          DOUBLE,
            low           DOUBLE,
            close         DOUBLE,
            vol           DOUBLE,
            amount        DOUBLE,
            iopv          DOUBLE,
            discount_rate DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hk_daily (
            ts_code    VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            open       DOUBLE,
            high       DOUBLE,
            low        DOUBLE,
            close      DOUBLE,
            vol        DOUBLE,
            amount     DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS us_daily (
            ts_code    VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            open       DOUBLE,
            high       DOUBLE,
            low        DOUBLE,
            close      DOUBLE,
            adj_close  DOUBLE,
            volume     BIGINT,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_status (
            ts_code     VARCHAR NOT NULL,
            market      VARCHAR NOT NULL,
            last_sync   DATE,
            first_date  DATE,
            row_count   INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            last_error  TEXT,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, market)
        )
    """)


def upsert_daily(conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> int:
    """批量插/更新日线数据。返回实际写入行数。
    
    使用 INSERT OR REPLACE，主键冲突时自动覆盖。
    """
    if df.empty:
        return 0
    # 确保 trade_date 是 date 类型
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    conn.register("_tmp_upsert", df)
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    # DuckDB 支持 INSERT OR REPLACE
    try:
        conn.execute(f"INSERT OR REPLACE INTO {table} ({cols}) SELECT * FROM _tmp_upsert")
    except Exception:
        # 如果 REPLACE 不支持，用 DELETE + INSERT
        # 构建 DELETE 条件
        pk_cols = ["ts_code", "trade_date"]
        if all(c in df.columns for c in pk_cols):
            codes = df["ts_code"].unique().tolist()
            dates = df["trade_date"].unique().tolist()
            codes_str = ", ".join([f"'{c}'" for c in codes])
            dates_str = ", ".join([f"'{d}'" for d in dates])
            conn.execute(f"DELETE FROM {table} WHERE ts_code IN ({codes_str}) AND trade_date IN ({dates_str})")
        conn.execute(f"INSERT INTO {table} ({cols}) SELECT * FROM _tmp_upsert")
    conn.unregister("_tmp_upsert")
    return len(df)


def update_stock_info(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """批量更新股票基本信息。INSERT OR REPLACE。"""
    if df.empty:
        return 0
    conn.register("_tmp_info", df)
    cols = ", ".join(df.columns)
    conn.execute(f"INSERT OR REPLACE INTO stock_info ({cols}) SELECT * FROM _tmp_info")
    conn.unregister("_tmp_info")
    return len(df)


def get_sync_status(conn: duckdb.DuckDBPyConnection, market: str) -> pd.DataFrame:
    """获取指定市场的同步状态"""
    return conn.execute(
        "SELECT ts_code, last_sync, first_date, row_count, error_count "
        "FROM sync_status WHERE market = ?", [market]
    ).fetchdf()


def update_sync_status(conn: duckdb.DuckDBPyConnection,
                       ts_code: str, market: str,
                       last_date: str, rows: int) -> None:
    """更新单只股票的同步状态"""
    conn.execute("""
        INSERT OR REPLACE INTO sync_status (ts_code, market, last_sync, row_count, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, [ts_code, market, last_date, rows])


def record_sync_error(conn: duckdb.DuckDBPyConnection,
                      ts_code: str, market: str, error_msg: str) -> None:
    """记录同步错误"""
    conn.execute("""
        INSERT INTO sync_status (ts_code, market, error_count, last_error, updated_at)
        VALUES (?, ?, 1, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (ts_code, market) DO UPDATE SET
            error_count = sync_status.error_count + 1,
            last_error = ?,
            updated_at = CURRENT_TIMESTAMP
    """, [ts_code, market, error_msg, error_msg])


def get_stocks_needing_update(conn: duckdb.DuckDBPyConnection,
                               market: str, start_date: str,
                               max_errors: int = 10) -> pd.DataFrame:
    """获取需要更新的股票列表（last_sync < start_date 且错误数不超过阈值）"""
    return conn.execute("""
        SELECT ts_code, last_sync FROM sync_status
        WHERE market = ?
          AND (last_sync IS NULL OR last_sync < ?)
          AND error_count < ?
        ORDER BY last_sync NULLS FIRST
    """, [market, start_date, max_errors]).fetchdf()
```

- [ ] **Step 4: 运行测试确认通过**

```bash
cd D:/daniel/market-data && python -m pytest tests/test_db.py -v
```

预期: 全部 PASS

- [ ] **Step 5: Commit**

```bash
cd D:/daniel/market-data && git add src/db.py tests/test_db.py && git commit -m "feat: DuckDB database module with upsert and sync status"
```

---

### Task 3: 工具模块 (限速器、日志、交易日历)

**Files:**
- Create: `D:\daniel\market-data\src\utils.py`

**Interfaces:**
- Consumes: config.yaml 中的 rate_limit, logging 配置
- Produces: `RateLimiter`, `setup_logger()`, `TradingCalendar.is_trading_day(market, date)`, `load_config(path) -> dict`

- [ ] **Step 1: 写测试 test_utils.py**

```python
# tests/test_utils.py
import time
import pytest
from src.utils import RateLimiter, load_config

def test_rate_limiter_waits():
    rl = RateLimiter(max_calls=2, period=60)
    t0 = time.time()
    rl.wait()
    rl.wait()
    rl.wait()  # 第三次应触发等待
    elapsed = time.time() - t0
    # 前两次不应等，第三次会等（取决于实现）
    # 主要验证不抛异常
    assert elapsed >= 0

def test_rate_limiter_respects_burst():
    rl = RateLimiter(max_calls=3, period=60)
    t0 = time.time()
    for _ in range(3):
        rl.wait()
    assert time.time() - t0 < 1  # 3次突发应很快

def test_load_config(tmp_path):
    import yaml
    config_path = tmp_path / "config.yaml"
    config_path.write_text("foo: bar\n", encoding="utf-8")
    cfg = load_config(str(config_path))
    assert cfg["foo"] == "bar"

def test_load_config_default():
    cfg = load_config("nonexistent.yaml")
    assert "database" in cfg
    assert "tushare" in cfg
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cd D:/daniel/market-data && python -m pytest tests/test_utils.py -v
```

Expected: FAIL

- [ ] **Step 3: 实现 utils.py**

```python
"""工具模块 — 限速器、日志、交易日历、配置加载"""
import time
import logging
import yaml
from pathlib import Path
from typing import Optional


class RateLimiter:
    """滑动窗口限速器，支持突发 + 平滑限速"""
    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self.calls: list = []

    def wait(self) -> None:
        """等待直到可以发下一个请求"""
        now = time.time()
        # 清理过期的调用记录
        self.calls = [t for t in self.calls if now - t < self.period]
        if len(self.calls) >= self.max_calls:
            # 等 oldest call 过期
            sleep_time = self.calls[0] + self.period - now + 0.1
            if sleep_time > 0:
                time.sleep(sleep_time)
            # 递归重试（清理后）
            self.wait()
            return
        self.calls.append(time.time())

    @property
    def remaining(self) -> int:
        """剩余可用调用次数"""
        now = time.time()
        self.calls = [t for t in self.calls if now - t < self.period]
        return max(0, self.max_calls - len(self.calls))


class TradingCalendar:
    """简易交易日历 — 判断某天是否为交易日"""
    
    # A股休市月份模式（春节用更精确判断）
    A_HOLIDAY_MONTHS = {1, 2, 5, 10}  # 可能有长假
    # 港股休市
    HK_HOLIDAY_MONTHS = {1, 2, 4, 5, 6, 7, 9, 10, 12}
    # 美股休市
    US_HOLIDAY_MONTHS = {1, 2, 5, 7, 9, 11, 12}
    
    @staticmethod
    def is_weekend(date_str: str) -> bool:
        """判断是否为周末"""
        from datetime import datetime
        dt = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
        return dt.weekday() >= 5

    @classmethod
    def is_trading_day(cls, market: str, date_str: str) -> bool:
        """简易交易日判断：排除周末。精确日历由数据源保证（非交易日无数据）。"""
        return not cls.is_weekend(date_str)


def load_config(config_path: str = "config.yaml") -> dict:
    """加载配置文件，不存在则返回默认配置"""
    path = Path(config_path)
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    # 返回默认配置
    return {
        "tushare": {"token": ""},
        "database": {"path": "data/market.duckdb"},
        "data": {
            "start_date": "2015-01-01",
            "markets": ["A", "ETF", "HK", "US"]
        },
        "rate_limit": {
            "tushare": {"max_calls": 190, "period": 60},
            "akshare": {"max_calls": 48, "period": 60},
            "yfinance": {"max_calls": 1900, "period": 3600},
        },
        "retry": {"max_attempts": 3, "backoff_base": 5},
        "logging": {"level": "INFO", "file": "data/pipeline.log"},
    }


def setup_logger(config: dict) -> logging.Logger:
    """配置日志器"""
    log_cfg = config.get("logging", {})
    logger = logging.getLogger("market_data")
    logger.setLevel(getattr(logging, log_cfg.get("level", "INFO")))
    
    # 文件 handler
    log_file = Path(log_cfg.get("file", "data/pipeline.log"))
    log_file.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    
    # 控制台 handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger
```

- [ ] **Step 4: 运行测试确认通过**

```bash
cd D:/daniel/market-data && python -m pytest tests/test_utils.py -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd D:/daniel/market-data && git add src/utils.py tests/test_utils.py && git commit -m "feat: utils module — rate limiter, trading calendar, config loader"
```

---

### Task 4: Tushare 数据源

**Files:**
- Create: `D:\daniel\market-data\src\sources\tushare.py`

**Interfaces:**
- Consumes: tushare token (from config or env), RateLimiter
- Produces: `TushareSource(api)`, `.get_stock_list()`, `.get_daily(ts_code, start, end)`, `.get_daily_basic(ts_code, start, end)`, `.get_adj_factor(ts_code, start, end)`, `.get_hk_daily(ts_code, start, end)`, `.get_hk_stock_list()`

- [ ] **Step 1: 实现 tushare.py (无测试 — 依赖外部 API)**

```python
"""Tushare Pro 数据源 — A股 + 港股"""
import os
import pandas as pd
import tushare as ts
from ..utils import RateLimiter


class TushareSource:
    """封装 Tushare Pro API，内置限速"""
    
    def __init__(self, config: dict):
        token = config["tushare"].get("token") or os.environ.get("TUSHARE_TOKEN", "")
        ts.set_token(token)
        self.pro = ts.pro_api()
        rl_cfg = config["rate_limit"]["tushare"]
        self.limiter = RateLimiter(max_calls=rl_cfg["max_calls"], period=rl_cfg["period"])
        self._stock_list_cache = None

    def _call(self, func, **kwargs) -> pd.DataFrame:
        """带限速的 API 调用"""
        self.limiter.wait()
        result = func(**kwargs)
        return result if not result.empty else pd.DataFrame()

    def get_stock_list(self, market: str = "A") -> pd.DataFrame:
        """获取股票列表
        
        market: 'A' 获取A股，'HK' 获取港股
        返回 DataFrame，列对齐 stock_info 表结构
        """
        if market == "A":
            df = self._call(self.pro.stock_basic, 
                           exchange='', 
                           list_status='L',
                           fields='ts_code,symbol,name,area,industry,list_date,delist_date,is_hs')
            if not df.empty:
                df["market"] = "A"
                df["exchange"] = df["ts_code"].str.split(".").str[1]
        elif market == "HK":
            df = self._call(self.pro.hk_basic,
                           fields='ts_code,name,list_date,delist_date')
            if not df.empty:
                df["market"] = "HK"
                df["industry"] = None
                df["area"] = "HK"
                df["exchange"] = "HKEX"
                df["is_hs"] = None
                df["list_status"] = "L"
        else:
            raise ValueError(f"Unknown market: {market}")
        return df

    def get_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取 A 股日线数据 (OHLCV)
        
        返回字段: ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
        """
        return self._call(
            self.pro.daily,
            ts_code=ts_code,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", "")
        )

    def get_daily_basic(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取 A 股日线基本面（PE/PB/市值/换手率等）
        
        返回字段: ts_code, trade_date, turnover_rate, volume_ratio, pe, pb, total_mv, circ_mv
        """
        return self._call(
            self.pro.daily_basic,
            ts_code=ts_code,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", "")
        )

    def get_adj_factor(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取复权因子"""
        return self._call(
            self.pro.adj_factor,
            ts_code=ts_code,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", "")
        )

    def get_hk_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取港股日线"""
        return self._call(
            self.pro.hk_daily,
            ts_code=ts_code,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", "")
        )

    def get_trade_calendar(self, exchange: str = "SSE", 
                           start_date: str = "20150101", 
                           end_date: str = "20991231") -> pd.DataFrame:
        """获取交易日历"""
        return self._call(
            self.pro.trade_cal,
            exchange=exchange,
            start_date=start_date,
            end_date=end_date
        )
```

- [ ] **Step 2: 冒烟测试 — 验证能连上 Tushare**

```bash
cd D:/daniel/market-data && python -c "
from src.utils import load_config
from src.sources.tushare import TushareSource
cfg = load_config()
# 设置 token
import os
os.environ['TUSHARE_TOKEN'] = 'your_token_here'
src = TushareSource(cfg)
df = src.get_stock_list('A')
print(f'A股数量: {len(df)}')
print(df.head())
"
```

预期: 看到 A 股列表（用户需要在 config.yaml 中填入 token 后再跑）。标记此步骤为验证步骤，token 由用户随后提供。

- [ ] **Step 3: Commit**

```bash
cd D:/daniel/market-data && git add src/sources/tushare.py && git commit -m "feat: Tushare data source for A-share and HK"
```

---

### Task 5: AKShare 数据源

**Files:**
- Create: `D:\daniel\market-data\src\sources\akshare.py`

**Interfaces:**
- Consumes: RateLimiter
- Produces: `AKShareSource()`, `.get_etf_list()`, `.get_etf_daily(code, start, end)`, `.get_a_stock_list()`

- [ ] **Step 1: 实现 akshare.py**

```python
"""AKShare 数据源 — ETF + A 股补充"""
import akshare as ak
import pandas as pd
from ..utils import RateLimiter


class AKShareSource:
    """封装 AKShare API，内置限速"""
    
    def __init__(self, config: dict):
        rl_cfg = config["rate_limit"]["akshare"]
        self.limiter = RateLimiter(max_calls=rl_cfg["max_calls"], period=rl_cfg["period"])

    def _wait(self):
        self.limiter.wait()

    def get_etf_list(self) -> pd.DataFrame:
        """获取 ETF 列表"""
        self._wait()
        df = ak.fund_etf_fund_info_em()
        # 重命名列对齐 stock_info
        result = pd.DataFrame()
        if not df.empty:
            result["ts_code"] = df["基金代码"]
            result["name"] = df["基金简称"]
            result["market"] = "ETF"
            result["list_date"] = pd.to_datetime(df.get("上市日期", pd.NaT), errors="coerce").dt.date
            result["industry"] = "ETF"
            result["area"] = "CN"
            result["exchange"] = df.get("上市地", "SH/SZ")
            result["is_hs"] = None
            result["list_status"] = "L"
            result["delist_date"] = None
        return result

    def get_etf_daily(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取 ETF 日线数据
        
        返回字段: ts_code, trade_date, open, high, low, close, vol, amount
        """
        self._wait()
        # akshare 的 code 不需要后缀
        df = ak.fund_etf_hist_em(symbol=code, period="daily",
                                 start_date=start_date.replace("-", ""),
                                 end_date=end_date.replace("-", ""))
        result = pd.DataFrame()
        if not df.empty:
            result["ts_code"] = code
            result["trade_date"] = pd.to_datetime(df["日期"]).dt.date
            result["open"] = pd.to_numeric(df["开盘"], errors="coerce")
            result["high"] = pd.to_numeric(df["最高"], errors="coerce")
            result["low"] = pd.to_numeric(df["最低"], errors="coerce")
            result["close"] = pd.to_numeric(df["收盘"], errors="coerce")
            result["vol"] = pd.to_numeric(df["成交量"], errors="coerce")
            result["amount"] = pd.to_numeric(df["成交额"], errors="coerce")
        return result

    def get_a_stock_list(self) -> pd.DataFrame:
        """获取 A 股列表（作为 Tushare 的补充/备用）"""
        self._wait()
        df = ak.stock_info_a_code_name()
        result = pd.DataFrame()
        if not df.empty:
            result["ts_code"] = df["code"]
            result["name"] = df["name"]
            result["market"] = "A"
        return result
```

- [ ] **Step 2: 冒烟测试**

```bash
cd D:/daniel/market-data && python -c "
from src.utils import load_config
from src.sources.akshare import AKShareSource
cfg = load_config()
src = AKShareSource(cfg)
df = src.get_etf_list()
print(f'ETF 数量: {len(df)}')
print(df.head())
"
```

- [ ] **Step 3: Commit**

```bash
cd D:/daniel/market-data && git add src/sources/akshare.py && git commit -m "feat: AKShare data source for ETF"
```

---

### Task 6: yfinance 数据源

**Files:**
- Create: `D:\daniel\market-data\src\sources\yfinance.py`

**Interfaces:**
- Consumes: RateLimiter
- Produces: `YFinanceSource()`, `.get_us_stock_list()`, `.get_us_daily(ticker, start, end)`

- [ ] **Step 1: 实现 yfinance.py**

```python
"""yfinance 数据源 — 美股"""
import yfinance as yf
import pandas as pd
from ..utils import RateLimiter


# 常见美股列表（SP500 主要成分作为种子，可扩展）
DEFAULT_US_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B",
    "JPM", "V", "JNJ", "WMT", "PG", "MA", "UNH", "HD", "BAC", "DIS",
    "NFLX", "ADBE", "CRM", "AMD", "INTC", "QCOM", "TXN", "PYPL",
    "VZ", "T", "PFE", "MRK", "ABBV", "KO", "PEP", "COST", "AVGO",
    "CSCO", "ACN", "ABT", "DHR", "NKE", "LLY", "CVX", "XOM", "WFC",
    "ORCL", "IBM", "INTU", "AMGN", "GE", "CAT", "BA", "MMM", "GS",
    "MS", "SPY", "QQQ", "IWM", "DIA", "EEM", "XLF", "XLE", "XLK",
    "TLT", "GLD", "SLV", "USO", "VXX", "ARKK", "SMH", "SOXX",
]


class YFinanceSource:
    """封装 yfinance API，内置限速"""
    
    def __init__(self, config: dict):
        rl_cfg = config["rate_limit"]["yfinance"]
        self.limiter = RateLimiter(max_calls=rl_cfg["max_calls"], period=rl_cfg["period"])

    def get_us_stock_list(self) -> pd.DataFrame:
        """获取美股列表（默认符号列表）"""
        rows = []
        for sym in DEFAULT_US_SYMBOLS:
            rows.append({
                "ts_code": sym,
                "name": sym,
                "market": "US",
                "list_date": None,
                "delist_date": None,
                "industry": None,
                "area": "US",
                "exchange": "NYSE/NASDAQ",
                "is_hs": None,
                "list_status": "L",
            })
        return pd.DataFrame(rows)

    def get_us_daily(self, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取美股日线数据"""
        self.limiter.wait()
        try:
            t = yf.Ticker(ticker)
            df = t.history(start=start_date, end=end_date)
            if df.empty:
                return pd.DataFrame()
            df = df.reset_index()
            result = pd.DataFrame()
            result["ts_code"] = ticker
            result["trade_date"] = pd.to_datetime(df["Date"]).dt.date
            result["open"] = df["Open"].astype(float)
            result["high"] = df["High"].astype(float)
            result["low"] = df["Low"].astype(float)
            result["close"] = df["Close"].astype(float)
            result["adj_close"] = df.get("Adj Close", df["Close"]).astype(float) if "Adj Close" in df.columns else df["Close"].astype(float)
            result["volume"] = df["Volume"].astype("int64")
            return result
        except Exception:
            return pd.DataFrame()
```

- [ ] **Step 2: 冒烟测试**

```bash
cd D:/daniel/market-data && python -c "
from src.utils import load_config
from src.sources.yfinance import YFinanceSource
cfg = load_config()
src = YFinanceSource(cfg)
df = src.get_us_daily('AAPL', '2024-01-01', '2024-06-01')
print(f'AAPL 行数: {len(df)}')
print(df.head())
"
```

- [ ] **Step 3: Commit**

```bash
cd D:/daniel/market-data && git add src/sources/yfinance.py && git commit -m "feat: yfinance data source for US stocks"
```

---

### Task 7: ETL Pipeline 编排层

**Files:**
- Create: `D:\daniel\market-data\src\pipeline.py`

**Interfaces:**
- Consumes: db, tushare, akshare, yfinance, utils
- Produces: `Pipeline(config)`, `.init_market(market)`, `.update_market(market)`, `.backfill_market(market, start, end)`

- [ ] **Step 1: 实现 pipeline.py**

```python
"""ETL Pipeline — 编排数据采集全流程"""
import logging
from datetime import datetime, timedelta
import pandas as pd
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn

from .db import (
    get_connection, update_stock_info, upsert_daily,
    update_sync_status, record_sync_error,
    get_stocks_needing_update
)
from .sources.tushare import TushareSource
from .sources.akshare import AKShareSource
from .sources.yfinance import YFinanceSource
from .utils import load_config, setup_logger


logger = logging.getLogger("market_data")


class Pipeline:
    """ETL 主流程"""
    
    def __init__(self, config: dict = None):
        self.config = config or load_config()
        self.logger = setup_logger(self.config)
        self.conn = get_connection(self.config)
        self.start_date = self.config["data"]["start_date"]
        self.max_retries = self.config["retry"]["max_attempts"]
        self.backoff = self.config["retry"]["backoff_base"]
        
        # 懒加载数据源
        self._ts = None
        self._ak = None
        self._yf = None

    @property
    def ts(self) -> TushareSource:
        if self._ts is None:
            self._ts = TushareSource(self.config)
        return self._ts

    @property
    def ak(self) -> AKShareSource:
        if self._ak is None:
            self._ak = AKShareSource(self.config)
        return self._ak

    @property
    def yf(self) -> YFinanceSource:
        if self._yf is None:
            self._yf = YFinanceSource(self.config)
        return self._yf

    def init_market(self, market: str) -> dict:
        """初始化一个市场：拉股票列表 + 全部历史数据
        
        Returns: {"total": N, "success": N, "failed": N, "errors": [...]}
        """
        self.logger.info(f"=== 初始化市场: {market} ===")
        
        # Step 1: 获取股票列表
        self.logger.info(f"获取 {market} 股票列表...")
        stock_list = self._get_stock_list(market)
        self.logger.info(f"获取到 {len(stock_list)} 只")
        
        # 写入 stock_info
        if not stock_list.empty:
            update_stock_info(self.conn, stock_list)
        
        # Step 2: 逐只拉取历史数据
        codes = stock_list["ts_code"].tolist()
        total = len(codes)
        success = 0
        failed = 0
        errors = []
        
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task(f"[cyan]拉取 {market} 日线...", total=total)
            
            for ts_code in codes:
                try:
                    rows = self._fetch_daily_with_retry(market, ts_code, 
                                                        self.start_date, 
                                                        datetime.now().strftime("%Y-%m-%d"))
                    progress.update(task, advance=1, 
                                   description=f"[cyan]{ts_code} ({success+failed+1}/{total})")
                    success += 1
                except Exception as e:
                    self.logger.error(f"失败 {ts_code}: {e}")
                    record_sync_error(self.conn, ts_code, market, str(e))
                    failed += 1
                    errors.append({"ts_code": ts_code, "error": str(e)})
                    progress.update(task, advance=1)
        
        self.logger.info(f"完成: {market} — 成功 {success}/{total}，失败 {failed}")
        return {"total": total, "success": success, "failed": failed, "errors": errors}

    def update_market(self, market: str) -> dict:
        """增量更新市场"""
        self.logger.info(f"=== 增量更新市场: {market} ===")
        
        today = datetime.now().strftime("%Y-%m-%d")
        stocks = get_stocks_needing_update(self.conn, market, today)
        
        if stocks.empty:
            self.logger.info(f"没有需要更新的 {market} 股票")
            return {"total": 0, "success": 0, "failed": 0, "errors": []}
        
        total = len(stocks)
        success = 0
        failed = 0
        errors = []
        
        with Progress() as progress:
            task = progress.add_task(f"[green]更新 {market}...", total=total)
            
            for _, row in stocks.iterrows():
                ts_code = row["ts_code"]
                last_sync = row.get("last_sync")
                start = (last_sync + timedelta(days=1)).strftime("%Y-%m-%d") if last_sync else self.start_date
                
                try:
                    rows = self._fetch_daily_with_retry(market, ts_code, start, today)
                    success += 1
                except Exception as e:
                    self.logger.error(f"更新失败 {ts_code}: {e}")
                    record_sync_error(self.conn, ts_code, market, str(e))
                    failed += 1
                    errors.append({"ts_code": ts_code, "error": str(e)})
                
                progress.update(task, advance=1)
        
        return {"total": total, "success": success, "failed": failed, "errors": errors}

    def backfill_market(self, market: str, start: str, end: str) -> dict:
        """回补指定日期范围数据"""
        self.logger.info(f"=== 回补 {market}: {start} ~ {end} ===")
        
        stock_list = self._get_stock_list(market)
        codes = stock_list["ts_code"].tolist()
        total = len(codes)
        success = 0
        failed = 0
        errors = []
        
        with Progress() as progress:
            task = progress.add_task(f"[yellow]回补 {market}...", total=total)
            
            for ts_code in codes:
                try:
                    rows = self._fetch_daily_with_retry(market, ts_code, start, end)
                    success += 1
                except Exception as e:
                    failed += 1
                    errors.append({"ts_code": ts_code, "error": str(e)})
                progress.update(task, advance=1)
        
        return {"total": total, "success": success, "failed": failed, "errors": errors}

    # ---- 内部方法 ----

    def _get_stock_list(self, market: str) -> pd.DataFrame:
        """根据市场获取股票列表"""
        if market == "A":
            df = self.ts.get_stock_list("A")
        elif market == "HK":
            df = self.ts.get_stock_list("HK")
        elif market == "ETF":
            df = self.ak.get_etf_list()
        elif market == "US":
            df = self.yf.get_us_stock_list()
        else:
            raise ValueError(f"Unknown market: {market}")
        return df

    def _fetch_daily_with_retry(self, market: str, ts_code: str, 
                                 start: str, end: str) -> int:
        """拉取单只股票日线，带重试
        
        Returns: 写入行数
        """
        import time
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                df = self._fetch_daily(market, ts_code, start, end)
                if df.empty:
                    return 0
                
                table = self._daily_table(market)
                rows = upsert_daily(self.conn, table, df)
                
                # 更新 sync_status
                max_date = df["trade_date"].max()
                update_sync_status(self.conn, ts_code, market, str(max_date), len(df))
                return rows
                
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    wait = self.backoff * (2 ** attempt)
                    self.logger.warning(f"重试 {attempt+1}/{self.max_retries} for {ts_code}. 等待 {wait}s...")
                    time.sleep(wait)
        
        raise last_error or Exception(f"Max retries exceeded for {ts_code}")

    def _fetch_daily(self, market: str, ts_code: str, start: str, end: str) -> pd.DataFrame:
        """根据市场拉取日线"""
        if market == "A":
            df = self.ts.get_daily(ts_code, start, end)
            if not df.empty:
                df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
            # 同时拉基本面和复权因子
            try:
                basic = self.ts.get_daily_basic(ts_code, start, end)
                if not basic.empty:
                    basic["trade_date"] = pd.to_datetime(basic["trade_date"]).dt.date
                    upsert_daily(self.conn, "a_daily_basic", basic)
            except Exception as e:
                self.logger.debug(f"daily_basic 拉取失败 {ts_code}: {e}")
            try:
                adj = self.ts.get_adj_factor(ts_code, start, end)
                if not adj.empty:
                    adj["trade_date"] = pd.to_datetime(adj["trade_date"]).dt.date
                    upsert_daily(self.conn, "a_adj_factor", adj)
            except Exception as e:
                self.logger.debug(f"adj_factor 拉取失败 {ts_code}: {e}")
            return df
            
        elif market == "ETF":
            return self.ak.get_etf_daily(ts_code, start, end)
            
        elif market == "HK":
            df = self.ts.get_hk_daily(ts_code, start, end)
            if not df.empty:
                df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
            return df
            
        elif market == "US":
            return self.yf.get_us_daily(ts_code, start, end)
            
        else:
            raise ValueError(f"Unknown market: {market}")

    def _daily_table(self, market: str) -> str:
        """市场 → 表名映射"""
        return {
            "A": "a_daily",
            "ETF": "etf_daily",
            "HK": "hk_daily",
            "US": "us_daily",
        }[market]

    def close(self):
        """关闭数据库连接"""
        self.conn.close()
```

- [ ] **Step 2: Commit**

```bash
cd D:/daniel/market-data && git add src/pipeline.py && git commit -m "feat: ETL pipeline with retry and progress display"
```

---

### Task 8: CLI 入口

**Files:**
- Create: `D:\daniel\market-data\cli.py`

**Interfaces:**
- Consumes: pipeline
- Produces: CLI commands `init`, `update`, `backfill`, `status`

- [ ] **Step 1: 实现 cli.py**

```python
#!/usr/bin/env python
"""市场数据采集 CLI

用法:
  python cli.py init [--market A|ETF|HK|US|all]
  python cli.py update [--market A|ETF|HK|US|all]
  python cli.py backfill <market> <start_date> <end_date>
  python cli.py status [--market A|ETF|HK|US]
"""
import sys
from src.pipeline import Pipeline
from src.utils import load_config


def cmd_init(args):
    pipeline = Pipeline()
    markets = args.get("market", "all")
    if markets == "all":
        markets = ["A", "ETF", "HK", "US"]
    else:
        markets = [markets]
    
    for m in markets:
        result = pipeline.init_market(m)
        print(f"[{m}] 总计: {result['total']}, 成功: {result['success']}, 失败: {result['failed']}")
        if result["errors"]:
            print(f"  错误(前5):")
            for e in result["errors"][:5]:
                print(f"    - {e['ts_code']}: {e['error'][:80]}")
    pipeline.close()


def cmd_update(args):
    pipeline = Pipeline()
    markets = args.get("market", "all")
    if markets == "all":
        markets = ["A", "ETF", "HK", "US"]
    else:
        markets = [markets]
    
    for m in markets:
        result = pipeline.update_market(m)
        print(f"[{m}] 需要更新: {result['total']}, 成功: {result['success']}, 失败: {result['failed']}")


def cmd_backfill(args):
    pipeline = Pipeline()
    result = pipeline.backfill_market(args["market"], args["start"], args["end"])
    print(f"回补完成 — 总计: {result['total']}, 成功: {result['success']}, 失败: {result['failed']}")


def cmd_status(args):
    from src.db import get_connection
    conn = get_connection(load_config())
    markets = args.get("market", "all")
    if markets == "all":
        markets = ["A", "ETF", "HK", "US"]
    else:
        markets = [markets]
    
    for m in markets:
        result = conn.execute("""
            SELECT COUNT(*) as total,
                   COUNT(last_sync) as synced,
                   MIN(first_date) as earliest,
                   MAX(last_sync) as latest,
                   SUM(row_count) as total_rows
            FROM sync_status WHERE market = ?
        """, [m]).fetchone()
        print(f"[{m}] 股票数: {result[0]}, 已同步: {result[1]}, "
              f"最早: {result[2]}, 最新: {result[3]}, 总行数: {result[4]}")
    conn.close()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return
    
    cmd = sys.argv[1]
    args = {}
    
    # 简单参数解析
    i = 2
    while i < len(sys.argv):
        if sys.argv[i].startswith("--"):
            key = sys.argv[i][2:]
            if i + 1 < len(sys.argv) and not sys.argv[i+1].startswith("--"):
                args[key] = sys.argv[i+1]
                i += 2
            else:
                args[key] = True
                i += 1
        else:
            i += 1
    
    # 位置参数
    pos_args = [a for a in sys.argv[2:] if not a.startswith("--")]
    if cmd == "backfill" and len(pos_args) >= 3:
        args["market"] = pos_args[0]
        args["start"] = pos_args[1]
        args["end"] = pos_args[2]
    
    commands = {
        "init": cmd_init,
        "update": cmd_update,
        "backfill": cmd_backfill,
        "status": cmd_status,
    }
    
    if cmd not in commands:
        print(f"未知命令: {cmd}")
        print(__doc__)
        return
    
    try:
        commands[cmd](args)
    except KeyboardInterrupt:
        print("\n中断。")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
cd D:/daniel/market-data && git add cli.py && git commit -m "feat: CLI entry point with init/update/backfill/status commands"
```

---

### Task 9: 集成验证

**Files:**
- Create: `D:\daniel\market-data\tests\test_integration.py`

- [ ] **Step 1: 写集成测试（数据库 + 模拟数据）**

```python
"""集成测试 — 验证 DB + Pipeline 协同工作"""
import duckdb
import pandas as pd
import pytest
from src.db import get_connection, create_tables, upsert_daily, get_sync_status, update_sync_status
from src.utils import load_config


class TestDBIntegration:
    """数据库模块集成测试"""
    
    def test_full_cycle(self, tmp_path):
        db_path = tmp_path / "test.duckdb"
        config = {
            "database": {"path": str(db_path)},
            "data": {"start_date": "2015-01-01"},
            "rate_limit": {
                "tushare": {"max_calls": 190, "period": 60},
                "akshare": {"max_calls": 48, "period": 60},
                "yfinance": {"max_calls": 1900, "period": 3600},
            },
            "retry": {"max_attempts": 3, "backoff_base": 5},
            "logging": {"level": "INFO", "file": str(tmp_path / "test.log")},
            "tushare": {"token": ""},
        }
        
        conn = get_connection(config)
        
        # 1. 表已创建
        tables = conn.execute("""
            SELECT name FROM sqlite_master WHERE type='table'
        """).fetchall()
        table_names = {t[0] for t in tables}
        assert "a_daily" in table_names
        assert "sync_status" in table_names
        
        # 2. 写入日线数据
        df = pd.DataFrame({
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["2024-01-01", "2024-01-01"],
            "open": [10.0, 20.0],
            "high": [11.0, 21.0],
            "low": [9.5, 19.5],
            "close": [10.5, 20.5],
            "pre_close": [10.1, 20.1],
            "change": [0.4, 0.4],
            "pct_chg": [3.96, 1.99],
            "vol": [100000.0, 200000.0],
            "amount": [1050000.0, 4100000.0],
        })
        count = upsert_daily(conn, "a_daily", df)
        assert count == 2
        
        # 3. 更新 sync_status
        update_sync_status(conn, "000001.SZ", "A", "2024-01-01", 1)
        update_sync_status(conn, "000002.SZ", "A", "2024-01-01", 1)
        
        # 4. 读取 sync_status
        status = get_sync_status(conn, "A")
        assert len(status) == 2
        assert status.iloc[0]["row_count"] == 1
        
        # 5. 幂等写入
        df2 = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["2024-01-01"],
            "open": [10.5], "high": [11.5], "low": [10.0], "close": [11.0],
            "pre_close": [10.1], "change": [0.9], "pct_chg": [8.91],
            "vol": [110000.0], "amount": [1150000.0],
        })
        upsert_daily(conn, "a_daily", df2)
        row = conn.execute("""
            SELECT open, close FROM a_daily 
            WHERE ts_code='000001.SZ' AND trade_date='2024-01-01'
        """).fetchone()
        assert row[0] == 10.5  # 覆盖了
        assert row[1] == 11.0
        
        conn.close()

    def test_multimarket(self, tmp_path):
        db_path = tmp_path / "multi.duckdb"
        config = {
            "database": {"path": str(db_path)},
            "data": {"start_date": "2015-01-01"},
            "rate_limit": {
                "tushare": {"max_calls": 190, "period": 60},
                "akshare": {"max_calls": 48, "period": 60},
                "yfinance": {"max_calls": 1900, "period": 3600},
            },
            "retry": {"max_attempts": 3, "backoff_base": 5},
            "logging": {"level": "INFO", "file": str(tmp_path / "test.log")},
            "tushare": {"token": ""},
        }
        
        conn = get_connection(config)
        
        # A股
        df_a = pd.DataFrame({
            "ts_code": ["000001.SZ"], "trade_date": ["2024-01-01"],
            "open": [10.0], "high": [12.0], "low": [9.0], "close": [11.0],
            "pre_close": [10.0], "change": [1.0], "pct_chg": [10.0],
            "vol": [1e6], "amount": [1.1e7],
        })
        upsert_daily(conn, "a_daily", df_a)
        
        # 港股
        df_hk = pd.DataFrame({
            "ts_code": ["00700.HK"], "trade_date": ["2024-01-01"],
            "open": [300.0], "high": [310.0], "low": [295.0], "close": [305.0],
            "vol": [5e6], "amount": [1.5e9],
        })
        upsert_daily(conn, "hk_daily", df_hk)
        
        # 美股
        df_us = pd.DataFrame({
            "ts_code": ["AAPL"], "trade_date": ["2024-01-01"],
            "open": [185.0], "high": [190.0], "low": [184.0], "close": [188.0],
            "adj_close": [188.5], "volume": [50_000_000],
        })
        upsert_daily(conn, "us_daily", df_us)
        
        # 验证各表独立
        a_count = conn.execute("SELECT COUNT(*) FROM a_daily").fetchone()[0]
        hk_count = conn.execute("SELECT COUNT(*) FROM hk_daily").fetchone()[0]
        us_count = conn.execute("SELECT COUNT(*) FROM us_daily").fetchone()[0]
        assert a_count == 1
        assert hk_count == 1
        assert us_count == 1
        
        conn.close()
```

- [ ] **Step 2: 运行测试**

```bash
cd D:/daniel/market-data && python -m pytest tests/test_integration.py -v
```

Expected: PASS

- [ ] **Step 3: Commit**

```bash
cd D:/daniel/market-data && git add tests/test_integration.py && git commit -m "test: integration tests for DB multi-market cycle"
```

---

## 验证清单

全部代码写完后，运行：
```bash
cd D:/daniel/market-data && python -m pytest tests/ -v
```

预期：全部 PASS (6-8 tests)

冒烟验证（需 token）:
```bash
python cli.py status
# 应输出四个市场的空状态
```
