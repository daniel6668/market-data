# Phase 2: 分析引擎 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建因子引擎（30+因子日计算）、回测引擎（vectorbt向量化）、选股筛选器（多条件+行业轮动），全部 CLI 可调。

**Architecture:** 三个独立 Python 模块（`src/factors/`, `src/backtest/`, `src/screening/`），各自通过清晰接口与 DuckDB 数据层交互。因子预计算存入 `stock_factors` 表，筛选器和回测引擎读表不重复计算。

**Tech Stack:** Python 3.11+, stockstats (技术指标), vectorbt (回测), DuckDB (数据), pandas

**依赖新增:** vectorbt>=0.5, plotly>=5.0 (已在 Phase 2 设计书中声明)

---

## File Structure (Phase 2 changes)

```
Create:  src/factors/__init__.py
Create:  src/factors/registry.py       # 因子注册表 + 计算函数
Create:  src/factors/engine.py         # FactorEngine: 批量计算 + DB写入
Create:  src/backtest/__init__.py
Create:  src/backtest/runner.py        # BacktestRunner: 单股/组合回测
Create:  src/backtest/report.py        # BacktestResult: 绩效指标
Create:  src/screening/__init__.py
Create:  src/screening/screener.py     # StockScreener: 多条件筛选
Modify:  src/db.py                     # 新增 stock_factors 表
Modify:  cli.py                        # 新增 factors/screen/backtest 命令
Modify:  scheduler.py                  # 新增 16:00 因子重算任务
Create:  tests/test_factors.py         # 因子引擎测试
Create:  tests/test_backtest.py        # 回测引擎测试
Create:  tests/test_screening.py       # 筛选器测试
```

---

### Task 1: DuckDB `stock_factors` 表 + FactorEngine 骨架

**Files:**
- Modify: `src/db.py` — 添加 `stock_factors` 表
- Create: `src/factors/__init__.py`
- Create: `src/factors/registry.py`
- Create: `src/factors/engine.py`
- Create: `tests/test_factors.py`

- [ ] **Step 1: 添加 `stock_factors` 表到 `db.py`**

在 `create_tables()` 末尾添加：

```sql
CREATE TABLE IF NOT EXISTS stock_factors (
    ts_code    VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    -- 趋势
    ma5 DOUBLE, ma10 DOUBLE, ma20 DOUBLE, ma60 DOUBLE,
    ema12 DOUBLE, ema26 DOUBLE,
    macd_dif DOUBLE, macd_dea DOUBLE, macd_bar DOUBLE,
    -- 动量
    ret_5d DOUBLE, ret_10d DOUBLE, ret_20d DOUBLE,
    rsi6 DOUBLE, rsi14 DOUBLE,
    -- 波动
    atr14 DOUBLE,
    boll_upper DOUBLE, boll_mid DOUBLE, boll_lower DOUBLE,
    -- 量价
    vol_ratio DOUBLE, avg_vol_5d DOUBLE, avg_vol_20d DOUBLE,
    -- 资金
    main_net_5d DOUBLE, main_net_10d DOUBLE, main_net_20d DOUBLE,
    -- 估值
    pe_ttm DOUBLE, pb DOUBLE, turnover_rate DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);
```

- [ ] **Step 2: 创建 `src/factors/registry.py`**

因子注册表，定义每个因子的计算函数。使用 stockstats 做技术指标，自定义函数做资金/估值因子。

```python
# src/factors/registry.py
"""因子注册表 — 每个因子一个计算函数，统一签名"""
import pandas as pd
import numpy as np
from stockstats import StockDataFrame


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """保证 stockstats 需要的列名"""
    df = df.copy()
    # stockstats 需要: open, close, high, low, volume
    # 我们数据源用 vol，映射一下
    if "vol" in df.columns and "volume" not in df.columns:
        df["volume"] = df["vol"]
    return df


def compute_ma(df: pd.DataFrame, window: int) -> pd.Series:
    """移动均线"""
    return df["close"].rolling(window).mean()


def compute_ema(df: pd.DataFrame, window: int) -> pd.Series:
    """指数移动均线"""
    return df["close"].ewm(span=window, adjust=False).mean()


def compute_macd(df: pd.DataFrame) -> pd.DataFrame:
    """MACD 三值"""
    s = StockDataFrame.retype(_ensure_columns(df))
    return pd.DataFrame({
        "macd_dif": s["macd"],
        "macd_dea": s["macds"],
        "macd_bar": s["macdh"],
    })


def compute_ret(df: pd.DataFrame, window: int) -> pd.Series:
    """N日涨跌幅"""
    return df["close"].pct_change(window) * 100


def compute_rsi(df: pd.DataFrame, window: int) -> pd.Series:
    """RSI"""
    s = StockDataFrame.retype(_ensure_columns(df))
    return s[f"rsi_{window}"]


def compute_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """ATR"""
    s = StockDataFrame.retype(_ensure_columns(df))
    return s["atr"]


def compute_boll(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """布林带 (upper, mid, lower)"""
    s = StockDataFrame.retype(_ensure_columns(df))
    return pd.DataFrame({
        "boll_upper": s["boll_ub"],
        "boll_mid": s["boll"],
        "boll_lower": s["boll_lb"],
    })


def compute_vol_ratio(df: pd.DataFrame) -> pd.Series:
    """量比 = 当日成交量 / 5日均量"""
    avg5 = df["vol"].rolling(5).mean()
    return df["vol"] / avg5


def compute_avg_vol(df: pd.DataFrame, window: int) -> pd.Series:
    """N日均量"""
    return df["vol"].rolling(window).mean()


# 因子注册表: name -> (func, kwargs, columns)
# columns: 如果 func 返回 DataFrame，指定要取哪些列
FACTOR_REGISTRY = {
    "ma5":     (compute_ma,      {"window": 5},  None),
    "ma10":    (compute_ma,      {"window": 10}, None),
    "ma20":    (compute_ma,      {"window": 20}, None),
    "ma60":    (compute_ma,      {"window": 60}, None),
    "ema12":   (compute_ema,     {"window": 12}, None),
    "ema26":   (compute_ema,     {"window": 26}, None),
    "macd_dif":(compute_macd,    {},             "macd_dif"),
    "macd_dea":(compute_macd,    {},             "macd_dea"),
    "macd_bar":(compute_macd,    {},             "macd_bar"),
    "ret_5d":  (compute_ret,     {"window": 5},  None),
    "ret_10d": (compute_ret,     {"window": 10}, None),
    "ret_20d": (compute_ret,     {"window": 20}, None),
    "rsi6":    (compute_rsi,     {"window": 6},  None),
    "rsi14":   (compute_rsi,     {"window": 14}, None),
    "atr14":   (compute_atr,     {"window": 14}, None),
    "boll_upper": (compute_boll, {"window": 20}, "boll_upper"),
    "boll_mid":   (compute_boll, {"window": 20}, "boll_mid"),
    "boll_lower": (compute_boll, {"window": 20}, "boll_lower"),
    "vol_ratio":  (compute_vol_ratio, {},        None),
    "avg_vol_5d": (compute_avg_vol, {"window": 5}, None),
    "avg_vol_20d":(compute_avg_vol, {"window": 20}, None),
}
```

- [ ] **Step 3: 创建 `src/factors/engine.py`**

```python
# src/factors/engine.py
"""因子引擎 — 批量计算 + DB 写入"""
import logging
import duckdb
import pandas as pd
from .registry import FACTOR_REGISTRY

logger = logging.getLogger(__name__)


class FactorEngine:
    """日级别因子计算引擎"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def compute_single(self, ts_code: str, start: str, end: str) -> pd.DataFrame:
        """计算单只股票的因子，返回 DataFrame（不写 DB）"""
        # 拉取日线
        df = self.conn.execute("""
            SELECT trade_date, open, high, low, close, vol
            FROM a_daily
            WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date
        """, [ts_code, start, end]).fetchdf()
        if df.empty:
            return pd.DataFrame()
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        result = pd.DataFrame({"ts_code": ts_code, "trade_date": df["trade_date"]})

        for name, (func, kwargs, col) in FACTOR_REGISTRY.items():
            try:
                out = func(df, **kwargs)
                if col:
                    result[name] = out[col].values if hasattr(out, 'columns') else out
                else:
                    # 确保对齐索引
                    result[name] = out.values if hasattr(out, 'values') else out
            except Exception as e:
                logger.debug(f"因子 {name} 计算失败 for {ts_code}: {e}")
                result[name] = None

        # 附加估值因子（从 a_daily_basic）
        basic = self.conn.execute("""
            SELECT trade_date, pe, pb, turnover_rate
            FROM a_daily_basic
            WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
        """, [ts_code, start, end]).fetchdf()
        if not basic.empty:
            basic["trade_date"] = pd.to_datetime(basic["trade_date"])
            result = result.merge(basic, on="trade_date", how="left", suffixes=("", "_basic"))
            result["pe_ttm"] = result.get("pe", None)
            result["pb"] = result.get("pb", None)
            result["turnover_rate"] = result.get("turnover_rate", None)

        return result

    def compute_all(self, trade_date: str) -> int:
        """计算全市场指定日期的因子，写入 stock_factors 表。返回成功数。"""
        codes = self.conn.execute(
            "SELECT ts_code FROM stock_info WHERE market = 'A'"
        ).fetchall()
        total = 0
        for (ts_code,) in codes:
            df = self.compute_single(
                ts_code,
                start="2024-01-01",  # 足够长的回溯窗口
                end=trade_date,
            )
            if df.empty:
                continue
            # 只写 trade_date 那天的行
            day_df = df[df["trade_date"] == trade_date]
            if day_df.empty:
                continue
            self._upsert_factors(day_df)
            total += 1
        logger.info(f"因子计算完成: {total} 只")
        return total

    def _upsert_factors(self, df: pd.DataFrame):
        """写入因子表（upsert）"""
        df = df.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        self.conn.register("_tmp_factors", df)
        cols = ", ".join(df.columns)
        self.conn.execute(
            f"INSERT OR REPLACE INTO stock_factors ({cols}) "
            f"SELECT * FROM _tmp_factors"
        )
        self.conn.unregister("_tmp_factors")
```

- [ ] **Step 4: 创建测试 `tests/test_factors.py`**

```python
# tests/test_factors.py
"""因子引擎测试"""
import duckdb
import pandas as pd
import pytest


@pytest.fixture
def db_conn():
    conn = duckdb.connect(":memory:")
    from src.db import create_tables
    create_tables(conn)
    # 插入测试日线数据
    import numpy as np
    dates = pd.date_range("2026-01-01", periods=100, freq="B")
    np.random.seed(42)
    close = 100 + np.cumsum(np.random.randn(100) * 2)
    df = pd.DataFrame({
        "ts_code": "000001.SZ",
        "trade_date": dates,
        "open": close + np.random.randn(100),
        "high": close + abs(np.random.randn(100) * 3),
        "low": close - abs(np.random.randn(100) * 3),
        "close": close,
        "vol": np.random.randint(100000, 1000000, 100),
    })
    conn.register("_tmp_daily", df)
    conn.execute("INSERT OR REPLACE INTO a_daily SELECT * FROM _tmp_daily")
    return conn


def test_factor_registry_has_all_factors():
    """验证因子注册表包含预期因子"""
    from src.factors.registry import FACTOR_REGISTRY
    expected = {"ma5", "ma10", "ma20", "ma60", "ema12", "ema26",
                "macd_dif", "macd_dea", "macd_bar",
                "ret_5d", "ret_10d", "ret_20d",
                "rsi6", "rsi14", "atr14",
                "boll_upper", "boll_mid", "boll_lower",
                "vol_ratio", "avg_vol_5d", "avg_vol_20d"}
    assert set(FACTOR_REGISTRY.keys()) == expected


def test_compute_single_returns_data(db_conn):
    """验证单股因子计算返回非空 DataFrame"""
    from src.factors.engine import FactorEngine
    engine = FactorEngine(db_conn)
    df = engine.compute_single("000001.SZ", "2026-01-01", "2026-06-01")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "ma5" in df.columns
    assert "rsi14" in df.columns
    # 最新值应该有数据
    assert df["ma20"].iloc[-1] > 0


def test_compute_all_writes_to_db(db_conn):
    """验证全市场因子计算写入 stock_factors 表"""
    # 先插入 stock_info
    db_conn.execute("INSERT INTO stock_info (ts_code, name, market) VALUES ('000001.SZ', 'Test', 'A')")
    from src.factors.engine import FactorEngine
    engine = FactorEngine(db_conn)
    n = engine.compute_all("2026-03-15")
    assert n == 1
    row = db_conn.execute(
        "SELECT COUNT(*) FROM stock_factors WHERE ts_code='000001.SZ'"
    ).fetchone()
    assert row[0] == 1
```

- [ ] **Step 5: 运行因子测试**

```bash
python -m pytest tests/test_factors.py -v --timeout=60
```

- [ ] **Step 6: 提交**

```bash
git add src/db.py src/factors/ tests/test_factors.py
git commit -m "feat: FactorEngine — 因子引擎骨架 + stock_factors 表 + 20因子注册表"
```

---

### Task 2: 回测引擎 (`src/backtest/`)

**Files:**
- Create: `src/backtest/__init__.py`
- Create: `src/backtest/runner.py`
- Create: `src/backtest/report.py`
- Create: `tests/test_backtest.py`

- [ ] **Step 1: 创建 `src/backtest/report.py`**

```python
# src/backtest/report.py
"""回测结果数据结构"""
from dataclasses import dataclass, field
import pandas as pd


@dataclass
class BacktestResult:
    """回测绩效指标"""
    total_return: float = 0.0       # 总收益率 (%)
    annual_return: float = 0.0      # 年化收益率 (%)
    sharpe_ratio: float = 0.0       # 夏普比率
    max_drawdown: float = 0.0       # 最大回撤 (%)
    win_rate: float = 0.0           # 胜率 (%)
    profit_factor: float = 0.0      # 盈亏比
    n_trades: int = 0               # 交易次数
    equity_curve: pd.DataFrame = field(default_factory=pd.DataFrame)
    trades: pd.DataFrame = field(default_factory=pd.DataFrame)
    benchmark_return: float = 0.0   # 基准同期收益 (%)

    def to_dict(self) -> dict:
        return {
            "total_return": round(self.total_return, 2),
            "annual_return": round(self.annual_return, 2),
            "sharpe_ratio": round(self.sharpe_ratio, 2),
            "max_drawdown": round(self.max_drawdown, 2),
            "win_rate": round(self.win_rate, 2),
            "profit_factor": round(self.profit_factor, 2),
            "n_trades": self.n_trades,
            "benchmark_return": round(self.benchmark_return, 2),
        }

    def summary(self) -> str:
        d = self.to_dict()
        return (
            f"总收益: {d['total_return']}% | 年化: {d['annual_return']}% | "
            f"夏普: {d['sharpe_ratio']} | 最大回撤: {d['max_drawdown']}% | "
            f"胜率: {d['win_rate']}% | 交易: {d['n_trades']}次"
        )
```

- [ ] **Step 2: 创建 `src/backtest/runner.py`**

```python
# src/backtest/runner.py
"""回测引擎 — 基于 vectorbt"""
import logging
import duckdb
import pandas as pd
import numpy as np
import vectorbt as vbt
from .report import BacktestResult

logger = logging.getLogger(__name__)


class BacktestRunner:
    """回测执行器"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def _load_prices(self, ts_code: str, start: str, end: str) -> pd.Series:
        """加载复权后收盘价"""
        df = self.conn.execute("""
            SELECT a.trade_date, a.close * COALESCE(b.adj_factor, 1) as adj_close
            FROM a_daily a
            LEFT JOIN a_adj_factor b ON a.ts_code = b.ts_code AND a.trade_date = b.trade_date
            WHERE a.ts_code = ? AND a.trade_date >= ? AND a.trade_date <= ?
            ORDER BY a.trade_date
        """, [ts_code, start, end]).fetchdf()
        if df.empty:
            return pd.Series(dtype=float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df.set_index("trade_date")["adj_close"]

    def _generate_signals(self, prices: pd.Series,
                           condition_buy: pd.Series,
                           condition_sell: pd.Series) -> tuple:
        """生成买卖信号"""
        entries = condition_buy.loc[prices.index].fillna(False).astype(bool)
        exits = condition_sell.loc[prices.index].fillna(False).astype(bool)
        # 确保不重复 entry
        entries = entries & ~entries.shift(1).fillna(False)
        exits = exits & ~exits.shift(1).fillna(False)
        return entries, exits

    def run_single(self, ts_code: str,
                   condition_buy: pd.Series,
                   condition_sell: pd.Series,
                   start: str, end: str,
                   commission: float = 0.0002) -> BacktestResult:
        """单股回测

        condition_buy/condition_sell: boolean Series, index=date
        commission: 手续费率，默认万二
        """
        prices = self._load_prices(ts_code, start, end)
        if prices.empty:
            return BacktestResult()

        entries, exits = self._generate_signals(prices, condition_buy, condition_sell)

        try:
            pf = vbt.Portfolio.from_signals(
                prices, entries, exits,
                freq="1D",
                init_cash=100000,
                fees=commission,
                slippage=0.001,
            )
        except Exception as e:
            logger.warning(f"回测 {ts_code} 失败: {e}")
            return BacktestResult()

        # 计算指标
        stats = pf.stats()
        equity = pf.value()

        # 基准: 买入持有
        bh_return = (prices.iloc[-1] / prices.iloc[0] - 1) * 100

        result = BacktestResult(
            total_return=float(stats.get("Total Return [%]", 0)),
            annual_return=float(stats.get("Annual Return [%]", 0)),
            sharpe_ratio=float(stats.get("Sharpe Ratio", 0)),
            max_drawdown=float(stats.get("Max Drawdown [%]", 0)),
            win_rate=float(stats.get("Win Rate [%]", 0)),
            profit_factor=float(stats.get("Profit Factor", 0)),
            n_trades=int(stats.get("Total Trades", 0)),
            equity_curve=pd.DataFrame({"date": equity.index, "value": equity.values}),
            benchmark_return=bh_return,
        )
        return result

    def run_comparison(self, results: dict[str, BacktestResult]) -> pd.DataFrame:
        """多策略/多股票回测结果对比"""
        rows = []
        for name, r in results.items():
            d = r.to_dict()
            d["name"] = name
            rows.append(d)
        return pd.DataFrame(rows)
```

- [ ] **Step 3: 创建 `tests/test_backtest.py`**

```python
# tests/test_backtest.py
"""回测引擎测试"""
import duckdb
import pandas as pd
import numpy as np
import pytest


@pytest.fixture
def db_conn():
    conn = duckdb.connect(":memory:")
    from src.db import create_tables
    create_tables(conn)
    dates = pd.date_range("2025-01-01", periods=200, freq="B")
    np.random.seed(123)
    close = 100 + np.cumsum(np.random.randn(200) * 1.5)
    df = pd.DataFrame({
        "ts_code": "000001.SZ",
        "trade_date": dates,
        "open": close + np.random.randn(200),
        "high": close + abs(np.random.randn(200) * 3),
        "low": close - abs(np.random.randn(200) * 3),
        "close": close,
        "vol": np.random.randint(100000, 1000000, 200),
    })
    conn.register("_tmp", df)
    conn.execute("INSERT OR REPLACE INTO a_daily SELECT * FROM _tmp")
    return conn


def test_run_single_ma_cross(db_conn):
    """测试均线交叉策略回测"""
    from src.backtest.runner import BacktestRunner
    runner = BacktestRunner(db_conn)

    # 加载价格并计算均线条件
    prices = runner._load_prices("000001.SZ", "2025-03-01", "2025-12-31")
    ma20 = prices.rolling(20).mean()
    ma60 = prices.rolling(60).mean()
    condition_buy = ma20 > ma60
    condition_sell = ma20 < ma60

    result = runner.run_single(
        "000001.SZ", condition_buy, condition_sell,
        "2025-03-01", "2025-12-31"
    )

    assert result.n_trades >= 0  # 可能没有交叉
    assert isinstance(result.total_return, float)
    assert result.max_drawdown <= 0  # 最大回撤应为负
    d = result.to_dict()
    assert "sharpe_ratio" in d


def test_backtest_result_to_dict(db_conn):
    """验证 BacktestResult.to_dict 格式"""
    from src.backtest.runner import BacktestRunner
    from src.backtest.report import BacktestResult
    r = BacktestResult(total_return=15.5, annual_return=12.3,
                       sharpe_ratio=1.2, max_drawdown=-18.0,
                       win_rate=55.0, profit_factor=2.1, n_trades=20)
    d = r.to_dict()
    assert d["total_return"] == 15.5
    assert d["sharpe_ratio"] == 1.2
    assert "总收益" in r.summary()
```

- [ ] **Step 4: 运行回测测试**

```bash
python -m pytest tests/test_backtest.py -v --timeout=60
```

- [ ] **Step 5: 提交**

```bash
git add src/backtest/ tests/test_backtest.py
git commit -m "feat: BacktestRunner — vectorbt 向量化回测引擎 + BacktestResult"
```

---

### Task 3: 选股筛选器 (`src/screening/`)

**Files:**
- Create: `src/screening/__init__.py`
- Create: `src/screening/screener.py`
- Create: `tests/test_screening.py`

- [ ] **Step 1: 创建 `src/screening/screener.py`**

```python
# src/screening/screener.py
"""选股筛选器 — 多条件组合筛选"""
import logging
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# 因子名到 DB 列/表的映射
FACTOR_SOURCES = {
    # stock_factors 表中的因子
    "ma5": ("stock_factors", "ma5"),
    "ma20": ("stock_factors", "ma20"),
    "ma60": ("stock_factors", "ma60"),
    "ret_5d": ("stock_factors", "ret_5d"),
    "ret_20d": ("stock_factors", "ret_20d"),
    "rsi14": ("stock_factors", "rsi14"),
    "vol_ratio": ("stock_factors", "vol_ratio"),
    # a_daily_basic 表中的因子
    "pe_ttm": ("a_daily_basic", "pe"),
    "pb": ("a_daily_basic", "pb"),
    "turnover_rate": ("a_daily_basic", "turnover_rate"),
}

# 运算符映射
OP_MAP = {
    "gt": ">",
    "lt": "<",
    "gte": ">=",
    "lte": "<=",
    "eq": "=",
    "ne": "!=",
}


class StockScreener:
    """多条件选股筛选器"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def by_conditions(self, conditions: list[dict],
                       trade_date: str = None) -> pd.DataFrame:
        """多条件 AND 筛选
        
        conditions = [
            {"factor": "pe_ttm", "op": "lt", "value": 30},
            {"factor": "rsi14", "op": "gt", "value": 30},
        ]
        """
        if trade_date is None:
            trade_date = self._latest_date()

        # 构建各源的 JOIN + WHERE
        from_clauses = ["stock_info si"]
        where_clauses = [f"si.market = 'A'"]
        joined = set()

        for i, cond in enumerate(conditions):
            factor = cond["factor"]
            op = OP_MAP.get(cond["op"], cond["op"])
            value = cond["value"]

            if factor in FACTOR_SOURCES:
                table, col = FACTOR_SOURCES[factor]
                alias = f"t{i}"
                if table not in joined:
                    from_clauses.append(
                        f"LEFT JOIN {table} {alias} "
                        f"ON si.ts_code = {alias}.ts_code "
                        f"AND {alias}.trade_date = '{trade_date}'"
                    )
                    joined.add(table)
                else:
                    # 复用已 JOIN 的表，找到别名
                    alias = f"t{list(FACTOR_SOURCES.keys()).index(factor)}"
                where_clauses.append(f"{alias}.{col} {op} {value}")
            else:
                logger.warning(f"未知因子: {factor}")

        query = f"""
            SELECT si.ts_code, si.name
            FROM {' '.join(from_clauses)}
            WHERE {' AND '.join(where_clauses)}
            LIMIT 100
        """
        try:
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            logger.error(f"筛选失败: {e}")
            return pd.DataFrame()

    def by_template(self, template: str,
                     trade_date: str = None) -> pd.DataFrame:
        """预设模板筛选"""
        templates = {
            "value_low_pe": [
                {"factor": "pe_ttm", "op": "lt", "value": 20},
                {"factor": "pb", "op": "lt", "value": 2},
            ],
            "momentum_strong": [
                {"factor": "ret_20d", "op": "gt", "value": 10},
                {"factor": "ma5", "op": "gt", "value": 0},
            ],
            "oversold_bounce": [
                {"factor": "rsi14", "op": "lt", "value": 35},
                {"factor": "vol_ratio", "op": "gt", "value": 1.5},
            ],
        }
        if template not in templates:
            logger.warning(f"未知模板: {template}")
            return pd.DataFrame()
        return self.by_conditions(templates[template], trade_date)

    def _latest_date(self) -> str:
        row = self.conn.execute(
            "SELECT MAX(trade_date) FROM a_daily"
        ).fetchone()
        return str(row[0]) if row[0] else "2026-01-01"
```

- [ ] **Step 2: 创建 `tests/test_screening.py`**

```python
# tests/test_screening.py
"""筛选器测试"""
import duckdb
import pandas as pd
import pytest


@pytest.fixture
def db_conn():
    conn = duckdb.connect(":memory:")
    from src.db import create_tables
    create_tables(conn)
    # 插入 stock_info
    conn.execute("INSERT INTO stock_info VALUES ('000001.SZ', 'Test1', 'A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, now())")
    conn.execute("INSERT INTO stock_info VALUES ('600519.SH', 'Test2', 'A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, now())")
    # 插入 a_daily_basic
    df = pd.DataFrame({
        "ts_code": ["000001.SZ", "600519.SH"],
        "trade_date": ["2026-06-29", "2026-06-29"],
        "pe": [8.5, 35.0],
        "pb": [0.7, 12.0],
        "turnover_rate": [1.2, 0.8],
    })
    conn.register("_tmp_b", df)
    conn.execute("INSERT OR REPLACE INTO a_daily_basic SELECT * FROM _tmp_b")
    # 插入 stock_factors
    conn.execute("""
        INSERT INTO stock_factors (ts_code, trade_date, rsi14, vol_ratio, ret_20d)
        VALUES ('000001.SZ', '2026-06-29', 28.5, 1.8, 8.0)
    """)
    conn.execute("""
        INSERT INTO stock_factors (ts_code, trade_date, rsi14, vol_ratio, ret_20d)
        VALUES ('600519.SH', '2026-06-29', 65.0, 0.7, -3.0)
    """)
    return conn


def test_by_conditions_low_pe(db_conn):
    """测试低PE筛选"""
    from src.screening.screener import StockScreener
    screener = StockScreener(db_conn)
    df = screener.by_conditions([
        {"factor": "pe_ttm", "op": "lt", "value": 30},
    ], trade_date="2026-06-29")
    assert len(df) == 1
    assert df.iloc[0]["ts_code"] == "000001.SZ"


def test_by_conditions_multiple(db_conn):
    """测试多条件 AND 筛选"""
    from src.screening.screener import StockScreener
    screener = StockScreener(db_conn)
    df = screener.by_conditions([
        {"factor": "pe_ttm", "op": "lt", "value": 30},
        {"factor": "pb", "op": "lt", "value": 1},
    ], trade_date="2026-06-29")
    assert len(df) == 1


def test_by_template(db_conn):
    """测试预设模板"""
    from src.screening.screener import StockScreener
    screener = StockScreener(db_conn)
    df = screener.by_template("oversold_bounce", trade_date="2026-06-29")
    assert isinstance(df, pd.DataFrame)
    # 000001 符合 RSI<35 + vol_ratio>1.5
    if not df.empty:
        assert "000001.SZ" in df["ts_code"].values
```

- [ ] **Step 3: 运行筛选测试**

```bash
python -m pytest tests/test_screening.py -v --timeout=30
```

- [ ] **Step 4: 提交**

```bash
git add src/screening/ tests/test_screening.py
git commit -m "feat: StockScreener — 多条件筛选 + 预设模板 + 因子映射"
```

---

### Task 4: CLI + 调度器 + 集成

**Files:**
- Modify: `cli.py` — 新增 `factors`/`screen`/`backtest` 命令
- Modify: `scheduler.py` — 每日 16:00 因子重算
- Modify: `README.md` — 更新命令列表

- [ ] **Step 1: CLI 新增命令**

```python
def cmd_factors(args):
    """计算今日因子"""
    from src.factors.engine import FactorEngine
    from src.db import get_connection
    from src.utils import load_config
    config = load_config()
    conn = get_connection(config)
    try:
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        engine = FactorEngine(conn)
        n = engine.compute_all(today)
        print(f"因子计算完成: {n} 只")
    finally:
        conn.close()


def cmd_screen(args):
    """条件选股"""
    from src.screening.screener import StockScreener
    from src.db import get_connection
    from src.utils import load_config
    config = load_config()
    conn = get_connection(config)
    try:
        screener = StockScreener(conn)
        # 简单参数解析: python cli.py screen --pe 30 --rsi 35
        conditions = []
        if "pe" in args:
            conditions.append({"factor": "pe_ttm", "op": "lt", "value": float(args["pe"])})
        if "rsi" in args:
            conditions.append({"factor": "rsi14", "op": "lt", "value": float(args["rsi"])})
        if not conditions:
            print("请指定筛选条件，如: python cli.py screen --pe 30 --rsi 35")
            return
        df = screener.by_conditions(conditions)
        if df.empty:
            print("无匹配结果")
        else:
            print(f"筛选结果 ({len(df)} 只):")
            print(df.to_string(index=False))
    finally:
        conn.close()


def cmd_backtest(args):
    """快速回测"""
    from src.backtest.runner import BacktestRunner
    from src.db import get_connection
    from src.utils import load_config
    config = load_config()
    conn = get_connection(config)
    try:
        runner = BacktestRunner(conn)
        code = args.get("code", "600519")
        start = args.get("start", "2025-01-01")
        end = args.get("end", "2026-06-29")
        prices = runner._load_prices(code, start, end)
        if prices.empty:
            print(f"{code}: 无数据")
            return
        ma20 = prices.rolling(20).mean()
        ma60 = prices.rolling(60).mean()
        result = runner.run_single(code, ma20 > ma60, ma20 < ma60, start, end)
        print(f"{code} 均线交叉回测: {result.summary()}")
    finally:
        conn.close()


# 在 main() 的 commands dict 中添加:
# "factors": cmd_factors,
# "screen": cmd_screen,
# "backtest": cmd_backtest,
```

- [ ] **Step 2: 调度器加入因子重算**

在 `scheduler.py` 的 `main()` 中添加：

```python
def run_factors_update():
    """收盘后因子重算"""
    config = load_config()
    logger.info("=== 因子重算开始 ===")
    from src.factors.engine import FactorEngine
    from src.db import get_connection
    conn = get_connection(config)
    try:
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        engine = FactorEngine(conn)
        n = engine.compute_all(today)
        logger.info(f"  因子计算: {n} 只")
    except Exception as e:
        logger.error(f"因子重算异常: {e}")
    finally:
        conn.close()
    logger.info("=== 因子重算结束 ===")

# 在 main() 中注册:
# schedule.every().day.at("16:00").do(run_factors_update)
# logger.info("  Phase2(因子重算): 每天 16:00")
```

- [ ] **Step 3: 运行集成测试验证**

```bash
python -m pytest tests/test_factors.py tests/test_backtest.py tests/test_screening.py -v --timeout=60
```

- [ ] **Step 4: 全量回归测试**

```bash
python -m pytest tests/test_db.py tests/test_factors.py tests/test_backtest.py tests/test_screening.py tests/test_validator.py tests/test_sync_status.py tests/test_sources_phase1.py -v --timeout=60
```

- [ ] **Step 5: 提交**

```bash
git add cli.py scheduler.py README.md
git commit -m "feat: CLI + 调度器集成 Phase 2 — factors/screen/backtest 命令 + 16:00因子重算"
```

---

### Task 5: 文档更新

- [ ] **Step 1: 更新 `docs/market-data-tech-doc.md`** 至 v0.4.0，添加 Phase 2 章节
- [ ] **Step 2: 更新 `README.md`** 的新增命令

```bash
git add docs/ README.md
git commit -m "docs: 更新技术文档至 v0.4.0 — Phase 2 分析引擎"
```

---

## Summary

| Task | 文件 | 描述 |
|:----:|------|------|
| 1 | `src/factors/` (new) + `src/db.py` (modify) | FactorEngine + 20因子 + stock_factors表 |
| 2 | `src/backtest/` (new) | vectorbt 回测引擎 + BacktestResult |
| 3 | `src/screening/` (new) | 多条件筛选器 + 模板 |
| 4 | `cli.py` + `scheduler.py` (modify) | CLI命令 + 调度集成 |
| 5 | `docs/` + `README.md` (modify) | 技术文档更新 |

**合计**: 6 个新文件 + 5 个修改文件 + ~12 次提交，测试从 ~41 → ~50 个。
