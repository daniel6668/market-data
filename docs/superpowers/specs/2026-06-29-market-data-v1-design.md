# market-data v1.0 设计规格书

**项目**: AI 驱动的 A 股中长线投资辅助系统  
**版本**: v1.0 (从当前 v0.2.0 数据管道演进)  
**日期**: 2026-06-29  
**作者**: daniel6668  

---

## 1. 产品愿景

面向中长线 A 股投资者的 AI 辅助系统。核心能力：

- **自然语言驱动**：用对话方式完成选股、回测、策略探索、信号监控
- **定时自动运行**：收盘后自动更新数据、扫描策略、推送买卖信号
- **策略可验证**：每个选股逻辑都可以回测验证，用数据说话
- **免费数据源**：所有数据来自免费公开接口，本地 DuckDB 缓存
- **Docker 部署**：本地运行，可随时迁移云端

### 1.1 用户画像

- 中长线投资者（持有周期 1 个月~半年）
- 关注 A 股、ETF 为主，参考美股和港股
- 年化收益目标 20%+
- 策略：行业/板块轮动，寻找近期可能上涨的标的

### 1.2 使用模式（混合式）

| 模式 | 触发 | 场景 |
|------|------|------|
| **定时推送** | 每日 16:30 自动 | 收盘后推送信号摘要和市场日报 |
| **主动对话** | 用户随时打开 | 策略探索、回测验证、深度分析 |

---

## 2. 总体架构

```
┌─────────────────────────────────────────────────────────────┐
│  Phase 3+4  Gradio Web UI (4 Tabs)                          │
│  ┌──────────┬──────────┬──────────┬──────────┐             │
│  │ 💬 对话  │ ⭐ 自选池│ 📊 策略  │ 🎯 信号  │             │
│  └──────────┴──────────┴──────────┴──────────┘             │
├─────────────────────────────────────────────────────────────┤
│  Phase 3     Agent 调度层 (LLM Function Calling)            │
│              工具: search / backtest / analyze / compare ... │
├─────────────────────────────────────────────────────────────┤
│  Phase 2     ┌───────────┐ ┌──────────┐ ┌──────────────┐   │
│              │ 因子引擎   │ │ 回测引擎  │ │ 选股筛选器    │   │
│              └───────────┘ └──────────┘ └──────────────┘   │
├─────────────────────────────────────────────────────────────┤
│  Phase 1+0   DuckDB 数据仓库 (~20 表，单文件)               │
│              现有: A股/ETF/港股/美股日线 + 资金流/研报/财报  │
│              新增: 北向/龙虎榜/融资融券/大宗/股东/分红/解禁  │
├─────────────────────────────────────────────────────────────┤
│  Phase 0     7 个数据源 + Phase 1 扩展至 10+                │
│              mootdx / tencent / eastmoney / sina / tushare  │
│              / akshare / yfinance / ths / cninfo            │
└─────────────────────────────────────────────────────────────┘
```

### 2.1 设计原则

| 做 | 不做 |
|---|------|
| DuckDB 单文件存储，零运维 | 实时行情（日线足够中长线） |
| 所有数据源免费 | 自动交易执行（只出信号） |
| Python 技术栈统一 | 多用户系统 |
| 每阶段产出独立可用 | 重型数据库（MySQL/PG） |
| Docker 部署可迁移 | 复杂前端框架（React/Vue） |

---

## 3. Phase 0: 现有基础 (v0.2.0)

当前已具备，作为后续阶段的底座。

### 3.1 数据源（7 个）

| 来源 | 数据 | 协议 | 限速 |
|------|------|------|------|
| mootdx | A股日线 OHLCV | TCP 7709 | 不限 |
| tencent | PE/PB/市值/换手率 | HTTP GBK | 不限 |
| eastmoney push2his | 资金流(主力/大单/小单) | HTTP | 1s 串行 |
| eastmoney reportapi | 研报列表+EPS预测 | HTTP | 1s 串行 |
| eastmoney push2 clist | 行业板块排名 | HTTP | 1s 串行 |
| sina quotes | 财报三表(利润/资产/现金流) | HTTP | 不限 |
| tushare | A股列表/港股/复权因子 | HTTP | 各接口限速 |
| akshare | ETF日线 + fallback | HTTP | 48次/分 |
| yfinance | 美股日线 | HTTP | 200次/时 |

### 3.2 DuckDB 表（12 张）

`stock_info`, `a_daily`, `a_daily_basic`, `a_adj_factor`, `stock_fund_flow`, `research_reports`, `financial_reports`, `etf_daily`, `hk_daily`, `us_daily`, `sync_status`, `trade_calendar`

### 3.3 工程能力

- CLI 7 个命令 (init/update/backfill/status/fundflow/research/financials)
- scheduler.py 定时调度
- RateLimiter 接口级限速
- 数据校验 (空表/空值率/日期连续性)
- 断点续传 (sync_status)
- 事务原子性 (单股 daily+basic+adj 在同一事务)
- 31 个测试

---

## 4. Phase 1: 数据补全

### 4.1 新增数据源

#### 高优先级（对中长线轮动直接有用）

| 数据 | 来源 | API | 粒度 | DB 表 |
|------|------|-----|------|-------|
| **北向资金** | 同花顺 hsgtApi | `data.hexin.cn/market/hsgtApi/method/dayChart/` | 日级累计(亿元) | `northbound_flow` |
| **融资融券** | 东财 datacenter | `RPTA_WEB_RZRQ_GGMX` | 日级(元) | `margin_trading` |
| **股东户数** | 东财 datacenter | `RPT_HOLDERNUMLATEST` | 季度 | `holder_num` |
| **概念板块归属** | 东财 slist | `push2.eastmoney.com/api/qt/slist/get` (spt=3) | 快照 | `stock_boards` |

#### 中优先级（增强信号质量）

| 数据 | 来源 | API | 粒度 | DB 表 |
|------|------|-----|------|-------|
| **龙虎榜** | 东财 datacenter | `RPT_DAILYBILLBOARD_DETAILSNEW` + BUY/SELL 明细 | 日级 | `dragon_tiger` |
| **大宗交易** | 东财 datacenter | `RPT_DATA_BLOCKTRADE` | 日级 | `block_trade` |
| **分红送转** | 东财 datacenter | `RPT_SHAREBONUS_DET` | 历史 | `dividend` |
| **限售解禁** | 东财 datacenter | `RPT_LIFT_STAGE` | 未来90天 | `lockup_expiry` |

#### 延后（短线/舆情类，Phase 2+ 需要时再加）

强势股题材归因、涨停板/炸板/跌停池、个股新闻/全球资讯、互动易问答、人气热榜

### 4.2 实现策略

**东财 datacenter 系列**（融资融券/龙虎榜/大宗/股东/分红/解禁）共用：
- 同一 base URL: `datacenter-web.eastmoney.com/api/data/v1/get`
- 同一限流入口: `em_get()` (串行 ≥1s 间隔)
- 同一 helper: `eastmoney_datacenter(report_name, filter_str, ...)`
- 不同 RPT 报表名作为参数区分

**北向资金**：
- 独立数据源 `ThsNorthboundSource`
- 本地 CSV 自缓存机制：每次拉取实时数据后写入 `~/.tradingagents/cache/northbound_daily.csv`
- 历史数据越跑越丰富

**概念板块**：
- 复用东财 push2 通道的 `em_get()` 限流
- 在现有 `eastmoney_source.py` 中扩展方法

### 4.3 项目结构（Phase 1 新增）

```
src/sources/
├── eastmoney_datacenter.py   # 新增：东财数据中心统一源
│   (融资融券/龙虎榜/大宗/股东/分红/解禁，6合1)
├── ths_northbound.py         # 新增：同花顺北向资金
├── eastmoney_source.py       # 扩展：概念板块归属方法
└── ...
```

### 4.4 新增 CLI 命令

```bash
python cli.py northbound          # 拉取北向资金历史+今日
python cli.py margin              # 全市场融资融券
python cli.py dragon              # 全市场龙虎榜
python cli.py blocks              # 更新概念板块归属
python cli.py holders             # 更新股东户数
```

### 4.5 新增 DB 表（8 张）

`northbound_flow`, `margin_trading`, `dragon_tiger`, `block_trade`, `dividend`, `lockup_expiry`, `stock_boards`, `holder_num`

---

## 5. Phase 2: 分析引擎

### 5.1 模块一：因子引擎 (`src/factors/`)

从 DuckDB 日线数据批量计算因子，结果写入 `stock_factors` 表。

#### 因子分类（7 大类，约 30+ 因子）

| 类别 | 因子 | 计算方式 |
|------|------|---------|
| 趋势 | MA5/10/20/60, EMA12/26, MACD, ADX | stockstats 库 |
| 动量 | 5/10/20/60日涨跌幅, RSI(6/14/24) | stockstats + 自定义 |
| 波动 | ATR(14), BOLL(20,2), 历史波动率 | stockstats |
| 量价 | 量比, 5日/20日均量, OBV, 换手率均线 | DuckDB 窗口函数 |
| 资金 | 主力净流入5/10/20日累计, 大单占比 | DuckDB 聚合 stock_fund_flow |
| 估值 | PE分位(1年/3年), PB分位, PEG, ROE | DuckDB 窗口函数 |
| 筹码 | 股东户数变化率, 融资余额趋势 | Phase 1 新增表 |

#### 接口设计

```python
# src/factors/engine.py
class FactorEngine:
    def compute_all(self, trade_date: str) -> int
        # 计算并写入当天全市场因子，返回成功数
    
    def compute_single(self, ts_code: str, start: str, end: str) -> pd.DataFrame
        # 单只股票因子计算
    
    def get_latest(self, ts_code: str) -> dict
        # 读取最新因子值
```

```python
# src/factors/registry.py
FACTOR_REGISTRY = {
    "ma5": {"category": "trend", "func": compute_ma5, "params": {"window": 5}},
    "rsi14": {"category": "momentum", "func": compute_rsi, "params": {"window": 14}},
    # ... 30+ 因子注册
}
```

#### `stock_factors` 表结构

```sql
CREATE TABLE stock_factors (
    ts_code VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    -- 趋势
    ma5 DOUBLE, ma10 DOUBLE, ma20 DOUBLE, ma60 DOUBLE,
    ema12 DOUBLE, ema26 DOUBLE, macd_dif DOUBLE, macd_dea DOUBLE, macd_bar DOUBLE,
    -- 动量
    ret_5d DOUBLE, ret_10d DOUBLE, ret_20d DOUBLE, ret_60d DOUBLE,
    rsi6 DOUBLE, rsi14 DOUBLE, rsi24 DOUBLE,
    -- 波动
    atr14 DOUBLE, boll_upper DOUBLE, boll_mid DOUBLE, boll_lower DOUBLE,
    -- 量价
    vol_ratio DOUBLE, avg_vol_5d DOUBLE, avg_vol_20d DOUBLE, turnover_ma5 DOUBLE,
    -- 资金 (关联 stock_fund_flow)
    main_net_5d DOUBLE, main_net_10d DOUBLE, main_net_20d DOUBLE,
    -- 估值 (关联 a_daily_basic)
    pe_ttm DOUBLE, pe_pct_1y DOUBLE, pe_pct_3y DOUBLE,
    pb DOUBLE, pb_pct_1y DOUBLE, peg DOUBLE, roe DOUBLE,
    -- 筹码 (Phase 1 表)
    holder_change_pct DOUBLE, margin_trend DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);
```

### 5.2 模块二：回测引擎 (`src/backtest/`)

基于 **vectorbt**（向量化回测，比逐条 loop 快 100x），封装适配 DuckDB 数据的接口。

#### 功能

| 功能 | 说明 |
|------|------|
| 单股回测 | 给定买卖条件 + 时间范围 → 收益曲线、胜率、夏普比、最大回撤 |
| 多股组合 | 给定选股条件 → 按期调仓 → 组合净值曲线 |
| 参数扫描 | 网格搜索最优参数组合 |
| 基准对比 | 自动对比沪深300同期表现 |
| 报告输出 | HTML（含交互图表）+ JSON（供 Agent 读取） |

#### 接口设计

```python
# src/backtest/runner.py
class BacktestRunner:
    def run_single(self, ts_code: str, strategy: dict, 
                   start: str, end: str) -> BacktestResult
    def run_portfolio(self, strategy: dict, rebalance: str, 
                      start: str, end: str) -> BacktestResult
    def scan_params(self, ts_code: str, strategy: dict,
                    param_grid: dict, start: str, end: str) -> list[BacktestResult]

# src/backtest/report.py  
class BacktestResult:
    total_return: float      # 总收益率
    annual_return: float     # 年化收益率
    sharpe_ratio: float      # 夏普比率
    max_drawdown: float      # 最大回撤
    win_rate: float          # 胜率
    profit_factor: float     # 盈亏比
    equity_curve: pd.DataFrame  # 净值曲线
    trades: pd.DataFrame     # 交易明细
    benchmark_return: float  # 基准收益
    
    def to_html(self, path: str) -> str  # 生成 HTML 报告
    def to_dict(self) -> dict            # 供 Agent 读取
```

#### 约束

- 使用复权价格：`a_daily.close × a_adj_factor.adj_factor`
- 手续费：默认万二，可配置
- 滑点：默认 0.1%
- 分红再投资：可选开关
- 涨停/跌停不可交易处理

### 5.3 模块三：选股筛选器 (`src/screening/`)

```python
# src/screening/screener.py
class StockScreener:
    def by_conditions(self, conditions: list[dict], 
                      trade_date: str = None) -> pd.DataFrame
        # 多条件 AND 筛选
        # conditions = [
        #     {"factor": "pe_ttm", "op": "lt", "value": 30},
        #     {"factor": "roe", "op": "gt", "value": 15},
        #     {"factor": "main_net_20d", "op": "gt", "value": 0},
        # ]
    
    def by_ranking(self, weights: dict, top_n: int = 30,
                   trade_date: str = None) -> pd.DataFrame
        # 加权排名筛选
    
    def by_industry_rotation(self, trade_date: str = None) -> pd.DataFrame
        # 先找强势行业 → 行业内选股
    
    def by_template(self, template_name: str,
                    trade_date: str = None) -> pd.DataFrame
        # 预设模板: "底部放量反转" "趋势延续" "价值低估" "动量突破"
```

### 5.4 CLI 扩展

```bash
python cli.py factors --update          # 更新今日因子
python cli.py screen --pe 30 --roe 15  # 条件筛选
python cli.py backtest --strategy ma_cross --code 600519 --start 2025-01-01
```

---

## 6. Phase 3: AI 对话系统

### 6.1 前端：Gradio 四 Tab 界面

| Tab | 功能 | 核心组件 |
|-----|------|---------|
| 💬 **对话** | NL 驱动选股/回测/分析/查询 | Chatbot + 内嵌图表/表格 |
| ⭐ **自选池** | 管理候选标的，跟踪关键指标 | Dataframe + 批量操作 + 排序 |
| 📊 **策略** | 向导式：设计→回测→评估→保存 | 参数面板 + 回测曲线 + 绩效卡片 |
| 🎯 **信号** | 当前信号总览 + 持仓跟踪 | 信号列表 + 条件说明 + 盈亏面板 |

### 6.2 Agent 层 (`src/agent/`)

#### LLM 后端抽象

通过 OpenAI 兼容接口统一接入，修改 `config.yaml` 即可切换：

```yaml
llm:
  provider: deepseek          # deepseek | glm | openai | anthropic | lmstudio
  api_key: sk-xxxx
  model: deepseek-chat
  base_url: https://api.deepseek.com
```

支持的 Provider：
- **DeepSeek** (`deepseek-chat`) — 便宜，中文好
- **GLM** (`glm-4-flash`) — 国产，免费额度
- **OpenAI** (`gpt-4o`) — 备用
- **Anthropic** (`claude-3.5-sonnet`) — 备用
- **LM Studio** (local) — 离线免费，`http://localhost:1234/v1`

#### Agent 工具集（8 个）

| 工具 | 功能 | 输入 | 输出 |
|------|------|------|------|
| `search_stocks` | 多条件选股 | NL条件 → 结构化参数 | 股票表格 + 评分 |
| `run_backtest` | 回测策略 | 策略定义 + 时间范围 | 绩效报告 + 收益曲线 |
| `analyze_stock` | 单股全面分析 | 股票代码 | 基本面+技术面+资金面+建议 |
| `compare_stocks` | 多股对比 | 股票列表 | 对比表 + 雷达图 |
| `add_to_watchlist` | 加入自选池 | 股票代码列表 | 确认 + 当前指标 |
| `check_signals` | 检查买卖信号 | 无/策略名 | 信号列表 + 触发原因 |
| `market_overview` | 市场概览 | 无 | 指数/行业/北向/涨跌分布 |
| `strategy_wizard` | 策略设计向导 | NL描述 | 引导式：条件→回测→评估→保存 |

#### 策略向导工作流

```
用户: "帮我设计一个底部反转策略"
  ↓
Agent: "好的，底部反转通常关注哪些特征？比如：
      ① 股价站上20日均线 ② 成交量放大至5日均量2倍
      ③ RSI从30以下回升 ④ 主力资金开始净流入
      你想关注哪些？或者我直接用常见组合？"
  ↓
用户: "用常见组合先看看"
  ↓
Agent: [调用 run_backtest → 展示结果]
      "过去3年：年化18.5%，夏普1.2，最大回撤22%，胜率55%
       主要亏损在2024年熊市。要调整什么参数吗？"
  ↓
用户: "把20日均线改成60日，再加一个PE<30的条件"
  ↓
Agent: [重新回测 → 对比]
      "年化提升到22%，但交易次数减少了40%…
       要保存这个策略吗？"
  ↓
用户: "保存，叫'底部反转v1'"
  ↓
Agent: "已保存。要把它加入每日信号扫描吗？"
```

### 6.3 Docker 部署

```dockerfile
# Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 7860
CMD ["python", "-m", "src.agent.app"]
```

```yaml
# docker-compose.yml
services:
  market-data:
    build: .
    ports:
      - "7860:7860"
    volumes:
      - ./data:/app/data          # DuckDB 持久化
      - ./config.yaml:/app/config.yaml  # 配置
    restart: unless-stopped
```

启动: `docker compose up -d` → 浏览器 `http://localhost:7860`

### 6.4 项目结构（Phase 3 新增）

```
src/
├── agent/
│   ├── __init__.py
│   ├── app.py          # Gradio 界面入口
│   ├── tools.py        # 8 个 Agent 工具函数
│   ├── llm.py          # LLM 后端抽象层
│   └── prompts.py      # 系统提示词模板
├── factors/            # Phase 2
├── backtest/           # Phase 2
└── screening/          # Phase 2
```

### 6.5 依赖新增

```
gradio>=4.0        # Web UI
openai>=1.0        # OpenAI 兼容客户端 (DeepSeek/GLM/LMStudio 共用)
vectorbt>=0.5      # 向量化回测
plotly>=5.0        # 交互图表
```

---

## 7. Phase 4: 信号输出

### 7.1 定时调度

扩展现有 `scheduler.py`，四个时间节点串联：

| 时间 | 任务 | 函数 |
|------|------|------|
| 15:30 | 数据更新 | `pipeline.update_market("A")` + Phase 1 新源更新 |
| 16:00 | 因子重算 | `FactorEngine.compute_all(today)` |
| 16:30 | 信号扫描 | 加载已保存策略 → 逐个筛选 → 生成信号 |
| 17:00 | 日报生成 + 推送 | 市场概览 + 信号摘要 + 自选池异动 |

### 7.2 信号定义 (`signal_log` 表)

```sql
CREATE TABLE signal_log (
    signal_id VARCHAR PRIMARY KEY,       -- UUID
    signal_type VARCHAR NOT NULL,        -- BUY / SELL / ALERT
    ts_code VARCHAR NOT NULL,
    strategy_name VARCHAR NOT NULL,
    reason TEXT NOT NULL,                -- 触发条件的自然语言描述
    confidence DOUBLE,                   -- 策略历史胜率
    triggered_at TIMESTAMP NOT NULL,
    status VARCHAR DEFAULT 'new',        -- new/acknowledged/expired/acted
    acknowledged_at TIMESTAMP,
    notes TEXT                           -- 用户备注
);
```

### 7.3 推送通道

| 通道 | Phase 4 | 说明 |
|------|:---:|------|
| 📧 邮件 | ✅ | smtplib，发送日报+信号摘要 |
| 🖥️ 浏览器通知 | ✅ | Gradio 前端 JS 弹窗 |
| 🔔 Webhook | ⏸ | 企微/飞书/钉钉，按需加 |
| 📱 微信推送 | ⏸ | PushPlus/Server酱，按需加 |

### 7.4 持仓跟踪

- 手动录入持仓（买入日期/价格/数量）或 CSV 导入
- 自动计算：浮动盈亏、持有天数、仓位占比
- 卖出预警：持仓股触发策略 SELL 信号时高亮
- 仓位分析：行业分布饼图、现金比例
- 调仓建议：基于信号 + 仓位给出建议

### 7.5 新增表

```sql
CREATE TABLE user_strategies (
    strategy_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description TEXT,
    conditions JSON NOT NULL,            -- 筛选条件定义
    backtest_result JSON,                -- 最近回测结果
    is_active BOOLEAN DEFAULT TRUE,      -- 是否参与每日扫描
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE watchlist (
    ts_code VARCHAR NOT NULL,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,
    PRIMARY KEY (ts_code)
);

CREATE TABLE portfolio (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_code VARCHAR NOT NULL,
    buy_date DATE NOT NULL,
    buy_price DOUBLE NOT NULL,
    shares INTEGER NOT NULL,
    current_price DOUBLE,
    pnl_pct DOUBLE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 7.6 CLI 扩展

```bash
python cli.py signals               # 扫描全部已激活策略，输出今日信号
python cli.py report                # 生成市场日报 (Markdown)
```

---

## 8. 完整项目结构 (v1.0)

```
market-data/
├── src/
│   ├── __init__.py
│   ├── db.py                       # DuckDB (扩展至 ~20 表)
│   ├── pipeline.py                 # ETL 编排 (扩展)
│   ├── utils.py                    # RateLimiter / TradingCalendar / Config
│   ├── validator.py                # 数据校验
│   ├── sources/                    # 数据源层
│   │   ├── base.py                 # DataSource ABC
│   │   ├── tushare.py              # Tushare (A股列表/港股/复权)
│   │   ├── akshare.py              # AKShare (ETF/fallback)
│   │   ├── yfinance.py             # yfinance (美股)
│   │   ├── mootdx_source.py        # mootdx (A股日线) ★
│   │   ├── tencent_source.py       # 腾讯 (PE/PB/市值) ★
│   │   ├── eastmoney_source.py     # 东财 push2 (资金流/研报/行业)
│   │   ├── eastmoney_datacenter.py # [新] 东财 datacenter (龙虎榜/融资/大宗等6合1)
│   │   ├── sina_source.py          # 新浪 (财报三表)
│   │   └── ths_northbound.py       # [新] 同花顺北向资金
│   ├── factors/                    # [新] 因子引擎 (Phase 2)
│   │   ├── __init__.py
│   │   ├── engine.py               # 因子计算
│   │   └── registry.py             # 因子注册表
│   ├── backtest/                   # [新] 回测引擎 (Phase 2)
│   │   ├── __init__.py
│   │   ├── runner.py               # 回测执行
│   │   └── report.py               # 绩效报告
│   ├── screening/                  # [新] 选股筛选 (Phase 2)
│   │   ├── __init__.py
│   │   └── screener.py             # 多条件筛选器
│   ├── agent/                      # [新] AI 对话 (Phase 3)
│   │   ├── __init__.py
│   │   ├── app.py                  # Gradio 入口
│   │   ├── tools.py                # Agent 工具函数
│   │   ├── llm.py                  # LLM 后端抽象
│   │   └── prompts.py              # 提示词
│   └── signals/                    # [新] 信号引擎 (Phase 4)
│       ├── __init__.py
│       ├── scanner.py              # 定时信号扫描
│       └── notifier.py             # 推送通知
├── tests/
│   └── ... (扩展测试覆盖新增模块)
├── data/                           # DuckDB + 日志 (持久化卷)
├── config.example.yaml
├── config.yaml                     # (gitignored, 含 Token)
├── cli.py                          # CLI (扩展)
├── scheduler.py                    # 定时调度 (扩展)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── pyproject.toml
└── README.md
```

---

## 9. 数据流总览

```
08:00  用户打开浏览器 → Gradio 对话/查看信号
       ↓
15:30  定时触发: 数据更新 (A股/ETF/港股/美股 + 北向/龙虎榜/融资融券...)
       ↓
16:00  因子引擎: 全市场因子计算 → stock_factors 表
       ↓
16:30  信号扫描: 遍历已激活策略 → search_stocks → 生成 BUY/SELL 信号
       ↓
17:00  日报生成 + 推送 (邮件/浏览器通知)
       ↓
次日  用户查看信号，追问 Agent → Agent 调用工具 → 返回分析/回测结果
```

---

## 10. 分阶段交付计划

| 阶段 | 预计工作量 | 核心交付物 | 可独立使用？ |
|------|:--:|------|:--:|
| Phase 1 | 2-3 周 | 8 个新数据源 + 8 张新表 + CLI 扩展 | ✅ 数据更全，供第三方分析 |
| Phase 2 | 3-4 周 | 因子引擎 + 回测 + 筛选器 + CLI | ✅ CLI 跑回测和筛选 |
| Phase 3 | 2-3 周 | Web Chat + Agent + 策略向导 + Docker | ✅ 浏览器对话操作一切 |
| Phase 4 | 1-2 周 | 定时信号 + 推送 + 持仓跟踪 | ✅ 全自动闭环 |

**总计预估**: 8-12 周（按业余时间每天 2 小时计）

---

## 11. 风险 & 约束

| 风险 | 缓解措施 |
|------|---------|
| 东财接口风控封 IP | 串行限流 1s+，批量调大至 2s；备选数据源 |
| 免费 API 不稳定 | 多源 fallback 链；本地缓存历史数据 |
| LLM API 成本 | 默认 DeepSeek/GLM（极便宜）；LM Studio 离线免费 |
| DuckDB 并发限制 | 单用户场景无问题；如需多进程可切 WAL 模式 |
| 数据量大导致慢 | 因子预计算；DuckDB 列式查询快速 |

---

## 12. 测试策略

沿用现有 pytest 体系，每阶段扩展测试：

| 阶段 | 测试重点 | 新增用例估计 |
|------|---------|:--:|
| Phase 1 | 新数据源 smoke test（每个源拉 1 只股票验证返回非空）；新表 CRUD；限流器行为 | +8 |
| Phase 2 | 因子计算结果数值范围校验；回测引擎输出格式；筛选器边界条件（空条件/无结果）；基准对比准确性 | +10 |
| Phase 3 | Agent 工具函数单元测试（mock LLM）；Gradio UI 组件渲染；LLM 后端切换；Docker 构建和启动 | +6 |
| Phase 4 | 信号扫描逻辑（模拟触发/不触发）；推送通道连通性；持仓盈亏计算精度；调度器任务链 | +6 |

**总计**: 31 (现有) + 30 (新增) ≈ **60 个测试**
