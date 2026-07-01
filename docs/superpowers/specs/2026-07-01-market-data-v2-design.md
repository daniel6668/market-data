# Market Data v2 — 策略研究-监控一体化设计

## 概述

从"模块集合"升级为**三段式策略研究-监控系统**：
发现（自然语言→筛选→回测）→ 监控（收盘后跟踪→卖出信号）→ 维护（形态破坏→移除建议）。

Agent 作为统一入口，职责为 **NL → 结构化条件 + 工具调度**，所有数据操作均在本地 DuckDB 完成。

## AI Agent 边界

| ✅ 可做 | ❌ 不可做 |
|---------|-----------|
| NL → SQL/Python 筛选表达式 | 调用 WebSearch/WebFetch |
| 调度已有工具（筛选/回测/因子/DB查询） | 从网上获取行情、新闻 |
| 对本地数据执行计算和分析 | 用外部 AI 服务做"分析" |
| 汇总结果、格式化呈现 | 任何联网"查答案" |

## 系统架构

```
用户自然语言
     │
     ▼
┌─────────────────────────────────────────────────┐
│              Agent 层 (统一调度入口)               │
│   理解意图 → 翻译条件 → 调度工具 → 汇总结果       │
└──────┬──────────────┬──────────────┬─────────────┘
       │              │              │
       ▼              ▼              ▼
  ┌─────────┐  ┌──────────┐  ┌──────────┐
  │ 发现引擎 │  │ 监控引擎  │  │ 维护引擎 │
  │ discover│  │ monitor  │  │ maintain │
  └────┬────┘  └────┬─────┘  └────┬─────┘
       │            │             │
       ▼            ▼             ▼
  ┌─────────────────────────────────────────────┐
  │              共享内核 (现有模块增强)            │
  │  筛选器 │ 回测器 │ 因子引擎 │ 数据管道 │ DB  │
  └─────────────────────────────────────────────┘
```

| 引擎 | 职责 | 触发方式 |
|------|------|---------|
| 发现引擎 (discover) | NL→条件翻译、全市场筛选、组合回测、结果→关注列表 | 用户交互 (Agent 对话) |
| 监控引擎 (monitor) | 收盘后关注列表收益跟踪、卖出信号扫描、新机会扫描 | 定时调度 (scheduler) |
| 维护引擎 (maintain) | 形态破坏检测、移除建议生成、关注列表健康检查 | 定时调度 + 用户审核 |

## 各模块详细设计

### 1. 发现引擎 (Discover Engine)

**交互流程：**

```
用户 NL → [LLM: NL→结构化条件] → [条件编译器: 条件→SQL/Python] → 执行筛选 → (可选)组合回测 → 结果呈现
```

**条件翻译（NL → Structured Conditions）：**

LLM 输出结构化条件对象，直接驱动后续执行：

```python
{
    "conditions": [
        {"factor": "pe_ttm",    "op": "lt", "value": 20},
        {"factor": "pb",        "op": "lt", "value": 2},
        {"factor": "macd_dif",  "op": "cross_above", "value": "macd_dea"},
    ],
    "universe": "A",
    "action": "backtest",
    "backtest": {"start": "2025-01-01", "end": "2026-06-30", "rebalance": "monthly", "weights": "equal"}
}
```

支持的操作符：`gt`, `lt`, `gte`, `lte`, `eq`, `cross_above`（金叉）, `cross_below`（死叉）

支持的市场范围：`A`, `ETF`, `HK`, `US`, `all`

**条件编译（Conditions → 执行）：**

结构化条件 → DuckDB SQL，去除现有硬编码限制：
- 搜索范围：500→全市场
- 回看窗口：固定 2026-03-01→可配置参数
- 结果数量：去除限制或设为可配置

长任务（>3秒）显示进度条。

**组合回测：**

| 项目 | v1 (现有) | v2 (升级后) |
|------|-----------|-------------|
| 回测对象 | 单只股票 | N 只股票组合 |
| 权重 | 无 | 等权重 / 按市值加权 |
| 再平衡 | 无 | 月度/季度/无 |
| 标的 | 仅 A 股 | A 股 + ETF + 港股 + 美股 |
| 基准 | 简单 buy-hold | 可配置基准指数 |
| 策略 | 仅 MA20/MA60 | 任意条件对 (buy_condition × sell_condition) |

### 2. 监控引擎 (Monitor Engine)

**运行时机：** 16:00 数据更新 → 16:10 因子重算 → 16:20 监控引擎

**监控流程：**

1. 加载关注列表（活跃状态）
2. 收益计算：累计收益、近期收益（5/10/20日）、相对基准表现 → 写入 `watchlist_performance`
3. 卖出信号扫描：按市场规则检查止损/止盈/均线/形态
4. 新机会扫描（可选）：用已保存策略条件全市场重新筛选
5. 生成操作建议：BUY / SELL / REDUCE / HOLD → 写入 `suggested_actions`

**卖出规则（按市场分类配置）：**

```yaml
strategies:
  A:
    label: "A股"
    sell_rules:
      stop_loss: -8%
      stop_profit: 30%
      ma_break: [ma20, ma60]
      pattern: [macd_dead_cross]
    remove_conditions:
      - type: price_below_ma
        ma: ma60
        consecutive_days: 5
      - type: macd_dead_cross
        consecutive_days: 3
  ETF:
    label: "ETF基金"
    sell_rules:
      stop_loss: -5%
      stop_profit: 15%
      ma_break: [ma20]
      pattern: [macd_dead_cross]
    remove_conditions:
      - type: price_below_ma
        ma: ma20
        consecutive_days: 3
  HK:
    label: "港股"
    sell_rules:
      stop_loss: -10%
      stop_profit: 20%
      ma_break: [ma60]
      pattern: [macd_dead_cross]
    remove_conditions:
      - type: price_below_ma
        ma: ma60
        consecutive_days: 5
  US:
    label: "美股"
    sell_rules:
      stop_loss: -10%
      stop_profit: 25%
      ma_break: [ma20, ma60]
      pattern: [macd_dead_cross]
    remove_conditions:
      - type: price_below_ma
        ma: ma60
        consecutive_days: 5
```

### 3. 维护引擎 (Maintain Engine)

**运行时机：** 收盘后 16:30（监控引擎之后）

**维护流程：**

1. 按市场分组遍历关注列表
2. 用对应市场的 `remove_conditions` 检查每只股票
3. 形态破坏检测：均线持续跌破、MACD死叉持续、连续下跌天数
4. 生成 REMOVE 建议（操作建议分级见下）
5. 用户下次打开界面时在"待审核"面板查看，确认后移除

### 4. 操作建议分级

| 级别 | 含义 | 示例触发条件 |
|------|------|-------------|
| BUY | 新出现匹配筛选条件的股票 | 全市场扫描新匹配 |
| SELL | 触发止损/技术面破坏，建议卖出 | 跌破 MA60 + MACD 死叉 |
| REDUCE | 信号走弱，可考虑减仓 | 跌破 MA20，但仍在 MA60 上方 |
| HOLD | 正常持有 | 无异常信号 |
| REMOVE | 建议移出关注列表 | 连续5日低于 MA60 |

### 5. 界面设计

5 个 Tab，以工作流为导向：

| Tab | 功能 | 改动 |
|-----|------|------|
| 🔍 发现 | NL 输入→筛选→回测→加入关注 | 重构现有 Chat Tab |
| 👁 关注列表 | 收益跟踪、信号状态、待审核面板 | 增强现有 Watchlist Tab |
| 📋 策略 | 已保存的筛选+卖出规则管理 | 原 placeholder → 实现 |
| ⚡ 信号 | 最新操作建议汇总 | 原 placeholder → 实现 |
| 📊 回测历史 | 回测记录对比 | 新增 |

### 6. 数据库新增/变更

| 表 | 内容 | 类型 |
|----|------|------|
| `watchlist_performance` | 每只关注股票每日收益快照 | 新增 |
| `strategy_rules` | 保存的筛选+卖出规则（按市场分类） | 新增 |
| `suggested_actions` | 操作建议历史 (BUY/SELL/REDUCE/REMOVE) | 新增 |
| `backtest_history` | 回测记录（条件、参数、结果） | 新增 |
| `watchlist` | 关注列表，新增字段：`entry_price`, `entry_date`, `strategy_name`, `status` (active/removed), `market` | 变更 |

### 7. 日程表

```
16:00  数据更新 (现有 pipeline)
16:10  因子重算 (现有)
16:20  监控引擎 (新增)
       ├─ 关注列表收益刷新
       ├─ 卖出信号扫描
       └─ 新机会扫描（如启用）
16:30  维护引擎 (新增)
       └─ 形态破坏检查 → 移除建议
```

## 与 v1 的主要改动

| 模块 | 改动类型 | 要点 |
|------|---------|------|
| `src/agent/` | 重构 | 从简单对话升级为三段调度器；NL→条件翻译；工具函数扩展 |
| `src/screening/` | 增强 | 去除硬编码限制（500→全市场）；支持动态条件编译 |
| `src/backtest/` | 重构 | 单股票→组合级回测；支持 ETF；可配置策略 |
| `src/factors/` | 扩展 | 新增卖出信号因子 |
| `src/monitor/` (新) | 新建 | 监控引擎 + 维护引擎 |
| `src/db.py` | 扩展 | 新增 4 张表，变更 watchlist 表 |
| `scheduler.py` | 增强 | 集成监控 + 维护任务 |
| `cli.py` | 增强 | 新增 monitor/maintain 命令 |
| `config.yaml` | 扩展 | 策略规则配置段 |
| UI (Gradio) | 重构 | 5 Tab 工作流布局 |

## 不改动的部分

- 数据源适配器 (src/sources/) — 保持现有逻辑
- 数据管道 (pipeline.py) — 保持现有采集流程
- 数据校验 (validator.py) — 保持
- 基础工具 (utils.py) — 保持

## 关键约束

- LLM 仅参与 NL→条件翻译，不参与任何数据查询
- 所有数据操作在 DuckDB 本地完成
- 长任务（>3 秒）必须显示进度条
- 系统只建议操作，不自动执行（需用户审核确认）
- 移除关注列表操作必须用户确认
