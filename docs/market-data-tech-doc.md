# market-data 技术文档

**项目**: A股 + ETF + 港股 + 美股 历史数据采集管道  
**版本**: 0.3.0 (Phase 1 数据补全)  
**日期**: 2026-06-29  
**技术栈**: Python 3.11 / DuckDB / mootdx / 腾讯财经 / 东财 push2+datacenter / 新浪财报 / 同花顺北向 / Tushare / AKShare / yfinance  

---

## 1. 项目概览

本地市场数据采集与缓存系统，覆盖 A 股（深沪全市场 ~5500 只）、ETF（~1500 只）、港股、美股，日线精度，缓存于 DuckDB。支持首次初始化、增量更新、指定范围回补三种模式，具备断点续传、接口级限速、数据校验、事务一致性等特性。

### 改进总览

本项目经历了 P0-P8 八个优先级的完整改进周期，共 30+ 条改进项，测试从 10 个增长到 31 个。

| 优先级 | 类别 | 改进项数 | 核心目标 |
|--------|------|---------|---------|
| P0 | 数据正确性 | 4 | 修复会导致数据错误的 bug |
| P1 | 安全与规范 | 4 | 移除敏感信息泄露、添加工程规范 |
| P2 | 健壮性 | 5 | 限流优化、事务保护、副作用消除 |
| P3 | 架构与测试 | 5 | 抽象基类、数据校验、定时调度、测试补充 |
| P4 | 长期改进 | 5 | 交易日历、备选数据源、linting、类型标注 |
| **P5** | **多数据源** | **5** | **mootdx日线、腾讯PE/PB、东财资金流/研报、新浪财报、行业排名** |
| **P6** | **持久化+调度** | **3** | **新增3张DB表 + CLI命令 + 调度集成** |
| **P7** | **工程化** | **3** | **CLI扩展、README、GitHub发布** |
| **P8** | **健壮性增强** | **3** | **mootdx缓存+重连、push2his重试、行业排名日期修正** |
| **P9** | **数据补全** | **8** | **北向资金、融资融券、龙虎榜、大宗交易、股东户数、分红送转、限售解禁、概念板块** |

### Phase 1 新增数据源 (v0.3.0)

| 来源 | 数据 | 文件 |
|------|------|------|
| 同花顺 hsgtApi | 北向资金 (沪股通/深股通日级) | `src/sources/ths_northbound.py` |
| 东财 datacenter | 融资融券/龙虎榜/大宗/股东/分红/解禁 (6合1) | `src/sources/eastmoney_datacenter.py` |
| 东财 slist | 概念板块归属 | `src/sources/eastmoney_source.py` (扩展) |

新增 DuckDB 表: `northbound_flow`, `margin_trading`, `dragon_tiger`, `block_trade`, `holder_num`, `dividend`, `lockup_expiry`, `stock_boards`

新增 CLI 命令: `northbound`, `margin`, `dragon`, `blocks`, `holders`

---

## 2. 架构设计

```
┌─────────────────────────────────────────────────────────────────────┐
│                    cli.py / scheduler.py                            │
│              (命令行入口 / 定时调度 16:00 + 16:30)                    │
├─────────────────────────────────────────────────────────────────────┤
│                   pipeline.py                                        │
│            (ETL 编排：init/update/backfill)                          │
│    ┌──────────────┐  ┌──────────────┐                               │
│    │  validator.py │  │   utils.py   │                               │
│    │ (数据校验)     │  │ (限速/日志/  │                               │
│    │              │  │  交易日历)   │                               │
│    └──────────────┘  └──────────────┘                               │
├─────────────────────────────────────────────────────────────────────┤
│              sources/ (数据源层 — 优先级链)                           │
│  ┌───────────┐ ┌──────────┐ ┌──────────┐ ┌─────────┐ ┌─────────┐  │
│  │mootdx ★   │ │tencent ★ │ │eastmoney★│ │ sina ★  │ │tushare  │  │
│  │(A股日线   │ │(PE/PB/   │ │(资金流/  │ │(财报    │ │(港股/    │  │
│  │ OHLCV)    │ │ 市值)    │ │ 研报/行业)│ │ 三表)   │ │ 复权因子)│  │
│  ├───────────┤ ├──────────┤ ├──────────┤ ├─────────┤ ├─────────┤  │
│  │base(ABC)  │ │akshare   │ │yfinance  │                       │  │
│  └───────────┘ └──────────┘ └──────────┘                       │  │
├─────────────────────────────────────────────────────────────────────┤
│                    db.py                                              │
│        (DuckDB 存储：12张表 + 交易日历)                               │
├─────────────────────────────────────────────────────────────────────┤
│              data/market.duckdb (73MB)                                │
└─────────────────────────────────────────────────────────────────────┘
```

### 分层职责

- **数据源层** (`src/sources/`): 各 API 封装，继承 `DataSource` 抽象基类，内置限速。
- **管道层** (`src/pipeline.py`): ETL 编排，负责拉取-校验-写入-状态更新的全流程。
- **存储层** (`src/db.py`): DuckDB 连接管理、建表、CRUD 操作。
- **校验层** (`src/validator.py`): 写入前检查空表、空值比例、日期连续性。 **[P3-NewFeature]**
- **工具层** (`src/utils.py`): RateLimiter 限速器、TradingCalendar 交易日历、配置加载、日志。
- **入口层** (`cli.py` / `scheduler.py`): 命令行交互 / 定时自动调度。 **[P3-NewFeature]**

---

## 3. 模块详细说明

### 3.1 src/sources/base.py — DataSource 抽象基类 `[P3-Architecture]`

**新增文件**。定义所有数据源的统一接口，支持注册式分发，新增数据源只需继承并实现接口，不需要修改 Pipeline。

```python
class DataSource(ABC):
    @abstractmethod
    def get_stock_list(self, market: str = "A") -> pd.DataFrame
    @abstractmethod
    def get_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame
    def get_daily_basic(self, ...) -> pd.DataFrame  # 默认空，A股数据源覆盖
    def get_adj_factor(self, ...) -> pd.DataFrame   # 默认空，A股数据源覆盖
    @property
    def supports_extra(self) -> bool                # 默认 False，TushareSource 覆盖为 True
```

### 3.2 src/sources/tushare.py — Tushare 数据源

**改进点:**

| 改动 | 级别 | 说明 |
|------|------|------|
| 继承 DataSource | `[P3-Architecture]` | `class TushareSource(DataSource)` |
| 接口级限速器 | `[P2-Robustness]` | 不同接口分设独立 RateLimiter：stock_basic 1次/小时，daily/daily_basic/adj_factor 各 45次/分钟 |
| `_call` 方法新增 `api_name` 参数 | `[P2-Robustness]` | 按接口名选择对应限速器 |
| `supports_extra` 属性 | `[P3-Architecture]` | 返回 True，表示提供 daily_basic 和 adj_factor |
| `_API_GROUPS` 类属性 | `[P2-Robustness]` | 接口名 → 限速组映射（hk_basic 归入 stock_basic 组，hk_daily 归入 daily 组） |

**关键方法:**
- `get_stock_list(market)` — A股用 stock_basic，港股用 hk_basic
- `get_daily(ts_code, start, end)` — A股日线 OHLCV
- `get_daily_basic(ts_code, start, end)` — PE/PB/市值/换手率
- `get_adj_factor(ts_code, start, end)` — 复权因子
- `get_hk_daily(ts_code, start, end)` — 港股日线
- `get_trade_calendar(exchange, start, end)` — 交易日历 `[P4-NewFeature]`

### 3.3 src/sources/akshare.py — AKShare 数据源

**改进点:**

| 改动 | 级别 | 说明 |
|------|------|------|
| 继承 DataSource | `[P3-Architecture]` | `class AKShareSource(DataSource)` |
| `get_hk_stock_list()` 新增 | `[P4-NewFeature]` | 港股列表备选数据源（AKShare stock_hk_spot_em），作为 Tushare 失败时的 fallback |

**关键方法:**
- `get_etf_list()` — ETF 列表（fund_etf_spot_em）
- `get_etf_daily(code, start, end)` — ETF 日线
- `get_a_stock_list()` — A股列表（作为 Tushare 备选）
- `get_hk_stock_list()` — 港股列表（作为 Tushare 备选） `[P4-NewFeature]`

### 3.4 src/sources/yfinance.py — 美股数据源

**改进点:**

| 改动 | 级别 | 说明 |
|------|------|------|
| 继承 DataSource | `[P3-Architecture]` | `class YFinanceSource(DataSource)` |
| 限频配置调低 | `[P2-Robustness]` | 从 1900次/小时调低到 200次/小时（config.yaml） |

### 3.5 src/sources/mootdx_source.py — 通达信数据源 `[P5-MultiSource]`

**新增文件**。A 股日线 OHLCV 的首选来源（TCP 7709 协议，不限频、不封 IP、零 Token）。

- 服务器列表 10 个，随机探测 + 成功缓存
- `_check_alive()` 自动探测 + 故障时自动重连
- 返回标准化的 OHLCV DataFrame（ts_code, trade_date, open, high, low, close, vol, amount）
- ⚠️ 不复权（通达信原始数据），复权因子仍走 Tushare

### 3.6 src/sources/tencent_source.py — 腾讯财经数据源 `[P5-MultiSource]`

**新增文件**。PE/PB/市值/换手率等基本面数据来源（HTTP GBK，不限频、零 Token）。

- `get_daily_basic(ts_code)` 返回 PE_TTM, PB, total_mv, circ_mv, turnover_rate, close
- ⚠️ 仅提供实时数据，不支持历史查询

### 3.7 src/sources/eastmoney_source.py — 东方财富数据源 `[P5-MultiSource]`

**新增文件**。东财独有数据（push2/push2his/reportapi），已内置 `em_get()` 串行限流：

- `get_fund_flow(ts_code, days)` — 日级资金流向（主力/大单/中单/小单/超大单，120日内）
- `get_reports(ts_code, max_pages)` — 研报列表 + 三年 EPS 预测 + 评级
- `get_industry_ranking(top_n)` — 全行业涨跌幅排名

### 3.8 src/sources/sina_source.py — 新浪财报数据源 `[P5-MultiSource]`

**新增文件**。财报三表数据来源（HTTP JSON，不限频、零 Token）：

- `get_income_statement(ts_code)` — 利润表（近 8 期）
- `get_balance_sheet(ts_code)` — 资产负债表（近 8 期）
- `get_cashflow(ts_code)` — 现金流量表（近 8 期）

### 3.5 src/db.py — DuckDB 存储层

**改进点:**

| 改动 | 级别 | 说明 |
|------|------|------|
| `update_sync_status` 改用 `ON CONFLICT DO UPDATE` | `[P0-Bug]` | 不再覆盖 first_date/error_count/last_error |
| `update_sync_status` 首次设置 `first_date` | `[P0-Bug]` | INSERT 时 first_date = last_date，后续只更新 last_sync/row_count |
| `CURRENT_TIMESTAMP` 改为 `now()` | `[P0-Bug]` | DuckDB 1.5.x 在 VALUES 子句中把 CURRENT_TIMESTAMP 误解析为列名 |
| `upsert_daily` 用 `df.copy()` | `[P2-Robustness]` | 消除修改传入 DataFrame 的副作用 |
| `upsert_daily` fallback 用临时表 | `[P2-Security]` | 用 `conn.register` + 子查询替代字符串拼接 SQL |
| `trade_calendar` 表新增 | `[P4-NewFeature]` | 存储交易日历（cal_date, is_open, exchange） |
| `save_trade_calendar()` 新增 | `[P4-NewFeature]` | 批量写入交易日历 |
| `get_trading_days()` 新增 | `[P4-NewFeature]` | 查询交易日列表 |
| `has_trade_calendar()` 新增 | `[P4-NewFeature]` | 检查是否已有日历数据 |

**表结构 (12 张表):**

| 表名 | 主键 | 用途 |
|------|------|------|
| stock_info | (ts_code, market) | 股票基础信息 |
| a_daily | (ts_code, trade_date) | A股日线 OHLCV |
| a_daily_basic | (ts_code, trade_date) | A股基本面 (PE/PB/市值) |
| a_adj_factor | (ts_code, trade_date) | A股复权因子 |
| stock_fund_flow | (ts_code, trade_date) | 主力/大单/中单资金流 `[P6-NewTable]` |
| research_reports | (info_code) | 研报列表 + EPS 预测 `[P6-NewTable]` |
| financial_reports | (ts_code, period, rpt_type) | 财报三表数据 `[P6-NewTable]` |
| etf_daily | (ts_code, trade_date) | ETF 日线 |
| hk_daily | (ts_code, trade_date) | 港股日线 |
| us_daily | (ts_code, trade_date) | 美股日线 |
| sync_status | (ts_code, market) | 同步状态 (last_sync, first_date, error_count) |
| trade_calendar | (cal_date, exchange) | 交易日历 `[P4-NewFeature]` |

### 3.6 src/pipeline.py — ETL 管道

**改进点:**

| 改动 | 级别 | 说明 |
|------|------|------|
| `socket.setdefaulttimeout(60)` | `[P2-Robustness]` | 全局网络超时，防止 AKShare/yfinance 无限等待 |
| `_fetch_daily` 重构返回 dict | `[P0-Bug]` | 返回 `{"daily": df, "extra": [(table, df), ...]}`，不再在方法内直接写库 |
| `_fetch_daily_with_retry` 事务包裹 | `[P0-Bug]` | daily + basic + adj_factor 在 `BEGIN TRANSACTION/COMMIT` 中原子写入 |
| `update_market` last_sync 类型判断 | `[P0-Bug]` | 简化为 `last_sync is None or pd.isna(last_sync)`，增加字符串兜底 |
| 重试策略区分限流 | `[P2-Robustness]` | 限流错误等待 60 秒，其他错误指数退避 |
| 数据校验集成 | `[P3-NewFeature]` | 写入前调用 `validate_daily`，校验失败跳过写入 |
| `TradingCalendar.load_from_db` 初始化 | `[P4-NewFeature]` | Pipeline 初始化时加载交易日历 |
| init/update/backfill 返回 0 行计为失败 | `[P3-Bug]` | 校验失败或空数据不计为成功 |
| HK fallback 到 AKShare | `[P4-NewFeature]` | Tushare 港股列表失败时用 AKShare `get_hk_stock_list` |
| ETF fallback | `[P4-NewFeature]` | AKShare ETF 列表失败时优雅返回空 |
| 移除方法内 `import time` | `[P2-Quality]` | 文件顶部已有 `import time` |

**关键方法:**
- `init_market(market)` — 首次初始化：拉列表 + 全历史数据
- `update_market(market)` — 增量更新：读 sync_status → 拉增量 → 写入
- `backfill_market(market, start, end)` — 回补指定范围
- `_fetch_daily_with_retry(market, ts_code, start, end)` — 带重试+校验+事务的日线拉取
- `_fetch_daily(market, ts_code, start, end)` — 按市场分发到对应数据源

### 3.7 src/utils.py — 工具模块

**改进点:**

| 改动 | 级别 | 说明 |
|------|------|------|
| TradingCalendar 重写 | `[P4-NewFeature]` | 从数据库加载交易日历（含节假日），fallback 到周末检查 |
| `load_from_db(conn)` 类方法 | `[P4-NewFeature]` | 从 trade_calendar 表加载交易日至内存集合 |
| `ensure_calendar(conn, ts_source)` 类方法 | `[P4-NewFeature]` | 数据库无数据时自动从 Tushare 拉取 |
| 移除 HOLIDAY_MONTHS 死代码 | `[P4-Quality]` | 清理未使用的类属性 |
| 类型标注补充 | `[P4-Quality]` | `conn: Any`, `ts_source: Any` |

### 3.8 src/validator.py — 数据校验 `[P3-NewFeature]`

**新增文件**。写入前检查数据质量。

```python
def validate_daily(df: pd.DataFrame, ts_code: str = "") -> dict
```

返回 `{"valid": bool, "issues": [str], "stats": dict}`

| 检查项 | 阈值 | 失败行为 |
|--------|------|---------|
| 空表 | df 为 None 或 empty | 返回 invalid |
| 空值比例 | OHLC 空值 > 50% | 返回 invalid |
| 日期连续性 | 间隔 >10 天的异常 >5 处 | 返回 invalid |

### 3.9 cli.py — 命令行接口

**改进点:**

| 改动 | 级别 | 说明 |
|------|------|------|
| `cmd_update` 添加 `try/finally` | `[P1-Bug]` | 确保 `pipeline.close()` 被调用 |
| `cmd_backfill` 添加 `try/finally` | `[P1-Bug]` | 同上 |

### 3.10 scheduler.py — 定时调度 `[P3-NewFeature]`

**新增文件**。基于 schedule 库的定时更新。

```bash
python scheduler.py                          # 每天 16:00 更新全市场
python scheduler.py --time 17:00 --market A  # 每天 17:00 只更新 A 股
python scheduler.py --now                    # 立即跑一次
```

### 3.11 配置文件

| 文件 | 改动 | 级别 |
|------|------|------|
| `.gitignore` | 新增：排除 config.yaml, data/, __pycache__/, .pytest_cache/ | `[P1-Security]` |
| `config.yaml` | token 清空后由用户填入，已加 .gitignore | `[P1-Security]` |
| `config.example.yaml` | 新增模板文件（token 留空） | `[P1-Security]` |
| `config.yaml` | Tushare 接口级限速配置 (stock_basic/daily/daily_basic/adj_factor) | `[P2-Robustness]` |
| `config.yaml` | yfinance 限频从 1900 调低到 200 | `[P2-Robustness]` |
| `pyproject.toml` | 添加 ruff 配置 (line-length=120, select E/F/W/I/UP/B) | `[P4-Quality]` |
| `requirements.txt` | 新增：锁定全部依赖版本 | `[P3-Quality]` |

### 3.12 resume_a.py / resume_tick.py — 辅助脚本

| 改动 | 级别 | 说明 |
|------|------|------|
| `resume_a.py` TARGET 改为动态查询 | `[P4-Quality]` | 从 `stock_info` 表动态获取 A 股数量 |
| `resume_a.py` os.chdir 改为 `__file__` | `[P4-Quality]` | 不再硬编码 `D:\daniel\market-data` |
| `resume_tick.py` import 提到顶部 | `[P4-Quality]` | 移除循环内 `import pandas`、`from datetime import timedelta` |
| `resume_tick.py` os.chdir 改为 `__file__` | `[P4-Quality]` | 同上 |
| `resume_tick.py` 类型判断修复 | `[P0-Bug]` | 与 pipeline.py 统一 `last_sync is None or pd.isna(last_sync)` |

---

## 4. 数据流

### 4.1 首次初始化 (init)

```
Pipeline.init_market("A")
  → _get_stock_list("A")
    → TushareSource.get_stock_list("A")     [限速: stock_basic 1次/小时]
    → fallback: AKShareSource.get_a_stock_list()  [如果 Tushare 失败]
  → update_stock_info(conn, stock_list)
  → for each ts_code:
    → _fetch_daily_with_retry("A", ts_code, start, end)
      → _fetch_daily("A", ts_code, start, end)
        → TushareSource.get_daily()           [限速: daily 45次/分钟]
        → TushareSource.get_daily_basic()     [限速: daily_basic 45次/分钟]
        → TushareSource.get_adj_factor()      [限速: adj_factor 45次/分钟]
        → return {"daily": df, "extra": [("a_daily_basic", basic), ("a_adj_factor", adj)]}
      → validate_daily(daily_df)              [校验: 空值/行数/日期]
      → BEGIN TRANSACTION
        → upsert_daily(conn, "a_daily", daily_df)
        → upsert_daily(conn, "a_daily_basic", basic_df)
        → upsert_daily(conn, "a_adj_factor", adj_df)
      → COMMIT
      → update_sync_status(conn, ts_code, "A", max_date, rows)
```

### 4.2 增量更新 (update)

```
Pipeline.update_market("A")
  → get_stocks_needing_update(conn, "A", today)
    → SELECT ts_code, last_sync FROM sync_status
      WHERE market='A' AND (last_sync IS NULL OR last_sync < today)
      AND error_count < 10
  → for each stock:
    → 计算 start = last_sync + 1 day (或 start_date 如果 last_sync 为 NULL)
    → _fetch_daily_with_retry("A", ts_code, start, today)
    → [同上：校验 → 事务写入 → 更新 sync_status]
```

### 4.3 限流策略

```
TushareSource:
  stock_basic  ──→  RateLimiter(1次/3600秒)   ←  Tushare 限制 1次/小时
  daily        ──┐
  daily_basic  ──┼→  RateLimiter(45次/60秒)   ←  Tushare 限制 50次/分钟
  adj_factor   ──┘
  hk_daily     ──┘

AKShareSource:
  所有接口      ──→  RateLimiter(48次/60秒)

YFinanceSource:
  所有接口      ──→  RateLimiter(200次/3600秒)
```

### 4.4 重试策略

```
_fetch_daily_with_retry:
  for attempt in range(max_retries):
    try:
      fetch → validate → transaction write → update sync_status
    except Exception as e:
      if 限流错误 (msg 含 "频率超限"/"Rate"/"Too Many"/"limit"):
        wait = 60 秒
      else:
        wait = backoff * 2^attempt  (5s, 10s, 20s)
      sleep(wait)
  raise last_error
```

---

## 5. 测试

### 5.1 测试矩阵

| 测试文件 | 测试数 | 覆盖范围 | 改动级别 |
|---------|--------|---------|---------|
| test_db.py | 4 | 建表、upsert 插入/更新、sync_status | 原有 |
| test_integration.py | 2 | 完整周期、多市场共存 | 原有 |
| test_utils.py | 4 | RateLimiter 等待/突发、配置加载 | 原有 |
| test_sync_status.py | 3 | ON CONFLICT 不覆盖 first_date/error_count | `[P3-Test]` |
| test_validator.py | 4 | 空表/None/正常数据/高空值 | `[P3-Test]` |
| test_pipeline_mock.py | 3 | Pipeline 编排 + 校验拦截 | `[P3-Test]` |
| test_trading_calendar.py | 4 | 周末 fallback/DB 加载/查询/检查 | `[P3-Test]` |
| **合计** | **24** | | |

### 5.2 关键测试说明

**test_first_date_preserved** `[P3-Test]`: 验证 P0 修复——首次同步设置 first_date=2024-01-01，第二次同步后 first_date 仍为 2024-01-01 而非 2024-06-01。

**test_error_count_preserved** `[P3-Test]`: 验证 P0 修复——record_sync_error 累计 error_count=2 后，update_sync_status 不清零。

**test_validator_rejects_bad_data** `[P3-Test]`: 验证 P3 数据校验——mock 返回 67% 空值数据，validate_daily 拦截，init_market 返回 success=0。

**test_init_market_with_mock** `[P3-Test]`: 用 MagicMock 模拟数据源，验证 Pipeline 编排逻辑正确写入 a_daily 和 sync_status。

---

## 6. 安装与使用

### 6.1 环境准备

```bash
# 克隆项目
git clone <repo>
cd market-data

# 创建虚拟环境（推荐）
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac

# 安装依赖
pip install -r requirements.txt
# 或
pip install -e .
```

### 6.2 配置

```bash
# 复制配置模板
cp config.example.yaml config.yaml

# 编辑 config.yaml，填入 Tushare token
# token: "你的Tushare Pro token"
```

### 6.3 使用

```bash
# 首次初始化（全市场）
python cli.py init

# 只初始化 A 股
python cli.py init --market A

# 增量更新
python cli.py update --market A

# 回补指定范围
python cli.py backfill A 2026-01-01 2026-06-28

# 查看状态
python cli.py status

# 定时调度（每天 16:00 自动更新）
python scheduler.py --time 16:00
```

### 6.4 运行测试

```bash
python -m pytest tests/ -v
```

### 6.5 Linting

```bash
# 安装 ruff
pip install ruff

# 检查
ruff check src/ tests/ cli.py scheduler.py

# 自动修复
ruff check --fix src/ tests/ cli.py scheduler.py
```

---

## 7. 改动日志

### P0 — 数据正确性（Bug 修复）

| ID | 文件 | 改动 | 影响 |
|----|------|------|------|
| P0-1 | db.py | `update_sync_status` 改用 `ON CONFLICT DO UPDATE` | first_date/error_count 不再被覆盖 |
| P0-2 | pipeline.py | `_fetch_daily` 返回 dict，`_fetch_daily_with_retry` 事务包裹 | daily/basic/adj 原子写入 |
| P0-3 | pipeline.py, resume_tick.py | `last_sync` 类型判断简化 | 增量更新起始日期不再算错 |
| P0-4 | db.py | `update_sync_status` 首次设置 first_date | first_date 不再永远为 NULL |
| P0-fix | db.py | `CURRENT_TIMESTAMP` → `now()` | DuckDB 1.5.x VALUES 子句兼容 |

### P1 — 安全与工程规范

| ID | 文件 | 改动 | 影响 |
|----|------|------|------|
| P1-5 | .gitignore | 新增 | 排除敏感文件和 73MB 数据库 |
| P1-6 | config.yaml | token 留空 | API token 不进入版本控制 |
| P1-7 | config.example.yaml | 新增模板 | 新用户复制即可使用 |
| P1-8 | cli.py | cmd_update/cmd_backfill 加 try/finally | 数据库连接不再泄漏 |

### P2 — 健壮性

| ID | 文件 | 改动 | 影响 |
|----|------|------|------|
| P2-9 | tushare.py, config.yaml | 接口级限速器 | stock_basic 不再超限，daily 充分利用配额 |
| P2-10 | pipeline.py | 重试区分限流/其他 | 限流等 60 秒，避免无效重试 |
| P2-11 | db.py | upsert_daily 用 df.copy() | 调用方 DataFrame 不被篡改 |
| P2-12 | db.py | fallback SQL 用临时表 | 消除 SQL 注入风险 |
| P2-13 | pipeline.py | socket.setdefaulttimeout(60) | AKShare/yfinance 不再无限挂死 |

### P3 — 架构与测试

| ID | 文件 | 改动 | 影响 |
|----|------|------|------|
| P3-14 | base.py, 三个数据源 | DataSource 抽象基类 | 新增数据源不需改 Pipeline |
| P3-15 | 4 个测试文件 | 新增 14 个测试 | 覆盖率从 10 提升到 24 |
| P3-16 | requirements.txt | 锁定依赖版本 | 环境可复现 |
| P3-17 | scheduler.py | 定时调度 | 收盘后自动更新 |
| P3-18 | validator.py, pipeline.py | 数据校验 | 空值/空表不写入数据库 |

### P4 — 长期改进

| ID | 文件 | 改动 | 影响 |
|----|------|------|------|
| P4-19 | pyproject.toml | ruff linting 配置 | 代码风格统一 |
| P4-20 | utils.py | 类型标注 | conn: Any, ts_source: Any |
| P4-21 | resume_a.py, resume_tick.py | 清理硬编码 | TARGET 动态查询，路径基于 __file__ |
| P4-22 | db.py, utils.py, pipeline.py | 完整交易日历 | trade_calendar 表 + TradingCalendar 重写 |
| P4-23 | akshare.py, pipeline.py | ETF/港股 fallback | HK 主源失败时用 AKShare 备选 |

---

## 8. 项目文件结构

```
D:\daniel\market-data/
├── pyproject.toml          # 项目配置 + ruff linting [P4-19]
├── requirements.txt        # 依赖锁定 [P3-16]
├── .gitignore              # 排除敏感文件 [P1-5]
├── config.yaml             # 运行配置（.gitignore 排除）[P1-6, P2-9]
├── config.example.yaml     # 配置模板 [P1-7]
├── cli.py                  # 命令行入口 [P1-8]
├── scheduler.py            # 定时调度 [P3-17]
├── resume_a.py             # A股补齐脚本 [P4-21]
├── resume_tick.py          # 小批量拉取脚本 [P0-3, P4-21]
├── src/
│   ├── __init__.py
│   ├── db.py               # DuckDB 存储层 [P0-1, P0-4, P2-11, P2-12, P4-22]
│   ├── pipeline.py         # ETL 管道 [P0-2, P0-3, P2-10, P2-13, P3-18, P4-22]
│   ├── utils.py            # 限速/日志/交易日历 [P4-20, P4-22]
│   ├── validator.py        # 数据校验 [P3-18]
│   └── sources/
│       ├── __init__.py
│       ├── base.py         # DataSource ABC [P3-14]
│       ├── tushare.py      # Tushare 数据源 [P2-9, P3-14]
│       ├── akshare.py      # AKShare 数据源 [P3-14, P4-23]
│       └── yfinance.py     # yfinance 数据源 [P3-14]
├── tests/
│   ├── test_db.py          # 数据库测试 (4)
│   ├── test_integration.py # 集成测试 (2)
│   ├── test_utils.py       # 工具测试 (4)
│   ├── test_sync_status.py # ON CONFLICT 验证 (3) [P3-15]
│   ├── test_validator.py   # 校验测试 (4) [P3-15]
│   ├── test_pipeline_mock.py # Pipeline mock 测试 (3) [P3-15]
│   ├── test_trading_calendar.py # 交易日历测试 (4) [P3-15]
│   └── test_sources_phase3.py  # 新数据源 smoke tests (7) [P6]
├── data/
│   ├── market.duckdb       # DuckDB 数据库 (gitignore 排除)
│   └── pipeline.log        # 运行日志
└── docs/
    └── superpowers/
        ├── plans/          # 设计计划
        └── specs/          # 设计规格

---

## 9. P5-P8 新增变更日志

### P5 — 多数据源架构

| ID | 文件 | 改动 | 影响 |
|----|------|------|------|
| P5-24 | mootdx_source.py | A股日线 OHLCV（通达信 TCP 7709） | 替代 Tushare daily，不限频零 Token |
| P5-25 | tencent_source.py | PE/PB/市值/换手率（腾讯财经 HTTP） | 替代 Tushare daily_basic，不限频零 Token |
| P5-26 | eastmoney_source.py | 资金流+研报+行业排名（push2/push2his/reportapi） | 新增 3 类东财独有数据 |
| P5-27 | sina_source.py | 财报三表（新浪 quotes.sina.cn） | 新增利润表/资产负债表/现金流量表 |
| P5-28 | pipeline.py | 优先级链：mootdx→Tushare（日线），腾讯→Tushare（基本面） | 自动降级，零 Token 优先 |

### P6 — 持久化 + 调度

| ID | 文件 | 改动 | 影响 |
|----|------|------|------|
| P6-29 | db.py | stock_fund_flow / research_reports / financial_reports 3 张新表 + CRUD | 数据落地可查询 |
| P6-30 | pipeline.py | update_fund_flow / update_research / update_financials 采集方法 | 全市场批量采集 |
| P6-31 | scheduler.py | Phase 4 任务(16:30) + cli.py 新增 3 命令 | 自动化 + CLI 可独立触发 |

### P7 — 工程化

| ID | 改动 | 影响 |
|----|------|------|
| P7-32 | README.md | 项目文档 |
| P7-33 | GitHub 发布 | github.com/daniel6668/market-data |

### P8 — 健壮性增强

| ID | 文件 | 改动 | 影响 |
|----|------|------|------|
| P8-34 | mootdx_source.py | 服务器缓存 + 随机探测 + _check_alive 自动重连 | TCP 服务器切换时不停机 |
| P8-35 | eastmoney_source.py | push2his 重试 2 次 | 间歇性空返回自愈 |
| P8-36 | eastmoney_source.py | 行业排名日期修正（当天无数据往前找 3 日） | 非交易日自动容错 |

### 数据源优先级链

```
A股日线:       mootdx(TCP) → Tushare fallback
PE/PB/市值:    腾讯财经(HTTP) → Tushare daily_basic fallback
资金流:        东财 push2his (HTTP, 1s限流)
研报:          东财 reportapi (HTTP, 1s限流)
财报三表:      新浪 quotes.sina.cn (HTTP, 不限频)
行业排名:      东财 push2 clist (HTTP, 1s限流)
复权因子:      Tushare (HTTP, 45/min限流)
股票列表:      Tushare stock_basic → AKShare fallback
港股:          Tushare hk_basic/hk_daily
ETF:           AKShare fund_etf_spot_em
美股:          yfinance
│   └── pipeline.log        # 运行日志
└── docs/
    └── superpowers/
        ├── plans/          # 设计计划
        └── specs/          # 设计规格
```
