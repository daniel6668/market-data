# market-data

本地市场数据采集管道 — A股 + ETF + 港股 + 美股历史数据，缓存于 DuckDB。

## 数据源

| 数据 | 来源 | Token |
|------|------|:---:|
| A股日线 OHLCV | **通达信 TCP** (mootdx) | ❌ |
| PE / PB / 市值 | **腾讯财经** HTTP | ❌ |
| 资金流(主力/大单) | **东财 push2his** | ❌ |
| 研报 | **东财 reportapi** | ❌ |
| 财报三表 | **新浪 quotes.sina.cn** | ❌ |
| 行业排名 | **东财 push2 clist** | ❌ |
| 复权因子 | Tushare Pro | ✅ |
| ETF 日线 | AKShare | ❌ |
| 港股日线 | Tushare Pro | ✅ |
| 美股日线 | yfinance | ❌ |

## 快速启动

```bash
# 安装依赖
pip install -r requirements.txt

# 配置（编辑 config.yaml 填入 Tushare token）
cp config.example.yaml config.yaml

# 初始化 A 股全量数据
python cli.py init --market A

# 增量更新
python cli.py update

# 资金流 + 研报 + 财报
python cli.py fundflow
python cli.py research
python cli.py financials

# 定时调度
python scheduler.py --time 16:00
```

## CLI 命令

```bash
python cli.py init [--market A|ETF|HK|US|all]     # 首次初始化
python cli.py update [--market A|ETF|HK|US|all]   # 增量更新
python cli.py backfill A 2026-01-01 2026-06-30     # 回补范围
python cli.py status                               # 查看状态
python cli.py fundflow                             # 全市场资金流
python cli.py research                             # 全市场研报
python cli.py financials                           # 全市场财报
```

## 测试

```bash
python -m pytest tests/ -v
```

## 存储

DuckDB 单文件 `data/market.duckdb`，12 张表：

| 表 | 内容 |
|----|------|
| stock_info | 股票基础信息 |
| a_daily | A股日线 OHLCV |
| a_daily_basic | PE/PB/市值 |
| a_adj_factor | 复权因子 |
| stock_fund_flow | 主力/大单/中单资金流 |
| research_reports | 研报列表 + EPS 预测 |
| financial_reports | 利润表/资产负债表/现金流 |
| etf_daily | ETF 日线 |
| hk_daily | 港股日线 |
| us_daily | 美股日线 |
| sync_status | 同步状态 |
| trade_calendar | 交易日历 |
