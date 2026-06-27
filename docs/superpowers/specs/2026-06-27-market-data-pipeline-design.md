# A股+ETF+港股+美股 历史数据采集系统 — 设计文档

**日期**: 2026-06-27
**状态**: 已批准

---

## 1. 目标

构建一个本地市场数据采集与缓存系统，覆盖 A 股（深沪全市场）、ETF、港股、美股，日线精度，缓存于 DuckDB，支持后续数据挖掘。

## 2. 数据范围

| 维度 | 范围 |
|------|------|
| A 股 | 沪深两市全部股票（~5500 只） |
| ETF | 全部 ETF（~1000 只） |
| 港股 | 全市场 |
| 美股 | 全市场 |
| 时间 | 2015-01-01 至今（近 10 年） |
| 字段 | OHLCV + 复权 + 成交额 + 换手率 + 市值 + PE/PB + 所有可获取字段 |

## 3. 数据源

| 市场 | 主数据源 | 备选 |
|------|----------|------|
| A 股 | Tushare Pro | AKShare |
| ETF | AKShare | Tushare |
| 港股 | Tushare | AKShare |
| 美股 | yfinance | — |

## 4. 存储架构 — DuckDB

表结构：
- `stock_info` — 股票基础信息（code, name, market, 上市/退市日期, 行业等）
- `a_daily` — A 股日线 OHLCV
- `a_daily_basic` — A 股日线基本面（换手率, PE, PB, 总市值, 流通市值等）
- `a_adj_factor` — 复权因子
- `etf_daily` — ETF 日线（额外含 IOPV, 折溢价率）
- `hk_daily` — 港股日线
- `us_daily` — 美股日线（含 adj_close）
- `sync_status` — 同步状态（每只股票的拉取进度/错误）

## 5. ETL 流程

```
首次: init → 拉列表 → 逐只拉取全部历史 → 写入 DuckDB → 更新 sync_status
日常: update → 读 sync_status → 拉增量 → 写入 → 更新
修复: backfill → 指定范围重拉 → 覆盖写入
```

## 6. 关键特性

- **断点续传**: sync_status 记录进度，中断后可从断点继续
- **智能重试**: 失败 3 次标记，下次周期尝试；连续 10 次报警
- **限速控制**: Tushare 200次/分钟，AKShare 50次/分钟
- **交易日历**: 避免非交易日请求
- **数据校验**: 写入前检查行数/日期连续性/空值比例

## 7. 项目结构

```
D:\daniel\market-data/
├── pyproject.toml
├── config.yaml
├── cli.py
├── src/
│   ├── db.py
│   ├── sources/
│   │   ├── tushare.py
│   │   ├── akshare.py
│   │   └── yfinance.py
│   ├── pipeline.py
│   ├── scheduler.py
│   └── utils.py
└── data/
    └── market.duckdb
```

## 8. 依赖

- duckdb, tushare, akshare, yfinance, pandas, pyyaml, rich（进度显示）, schedule（定时）
