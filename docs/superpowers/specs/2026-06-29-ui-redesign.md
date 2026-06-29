# UI 交互重设计 — 从对话查看器到操作台

**日期**: 2026-06-29  
**目标**: 将 Gradio 界面从"纯聊天"升级为"可交互操作台"，筛选/选股/自选全流程可操作  

---

## 1. 核心变化

| | 旧设计 | 新设计 |
|---|--------|--------|
| 筛选结果 | Markdown 文本 | 可勾选交互表格 (gr.Dataframe) |
| 操作方式 | 只能看 | 勾选 → 加自选/回测/对比 |
| 结果过多 | 粗暴截断 | 分页 + "二次筛选" |
| 自选池 | 占位符 | 完整表格 + 点击详情 + 历史追溯 |
| 策略面板 | 占位符 | 独立筛选器 + 保存策略 |

---

## 2. Tab 设计

### 2.1 💬 对话 Tab

**布局**: 左 60% 聊天 + 右 40% 选中摘要面板

**对话中的筛选结果**:
- AI 工具返回 JSON 格式: `{"type":"table","columns":[...],"rows":[[...]],"total":N}`
- 前端在聊天消息下方嵌入 `gr.Dataframe`
- Dataframe 带行选择 (checkbox)
- 表格下方: "已选 N 只" + [加入自选池] [批量回测] [对比分析]

**二次筛选**:
- 当结果 > 200 行时，显示 [二次筛选 ▾] 折叠面板
- 展开后提供快速过滤输入: PB < ?, 涨跌幅 > ?, 主力净流入 > ?
- 仅在当前结果中客户端过滤 (不调 LLM)

**右侧面板**: 显示当前全局选中的股票简要信息

### 2.2 ⭐ 自选池 Tab

**表格列**: checkbox | 代码 | 名称 | PE | PB | 涨跌幅 | 主力净流入 | 加入日期 | 筛选条件

**点击行展开详情**: PE/PB/市值/MA20/MA60/RSI/主力资金 + 原始筛选条件

**操作**: [移出] [批量回测] [批量分析]

**数据持久化**: DuckDB `watchlist` 表

```sql
CREATE TABLE watchlist (
    ts_code VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_condition TEXT,  -- JSON: [{"factor":"pe_ttm","op":"lt","value":30}]
    notes TEXT
);
```

### 2.3 📊 策略 Tab

**左侧筛选面板**: 因子下拉 + 阈值输入 + 行业过滤 + 排序

**右侧结果表格**: 与对话 Tab 相同的数据表格 + 操作按钮

**保存策略**: 筛选条件 → 命名 → 存入 `user_strategies` 表 (供 Phase 4 自动扫描)

### 2.4 🎯 信号 Tab

Phase 4 预留，本阶段仅做 UI 骨架。

---

## 3. 技术方案

### 3.1 全局选中状态

```python
selected_codes = gr.State([])  # 跨 Tab 共享
```

所有表格的选中事件写入此 State。操作按钮根据 State 动态启用/禁用。

### 3.2 Agent 工具输出格式

`search_stocks` 工具返回 JSON (非文本):

```json
{
  "type": "table",
  "columns": ["代码","名称","PE","PB","涨跌幅%"],
  "rows": [
    ["600036.SH","招商银行",6.08,0.82,1.2],
    ["000001.SZ","平安银行",5.21,0.61,-0.3]
  ],
  "total": 1364
}
```

前端渲染为 gr.Dataframe。`analyze_stock`、`run_backtest` 同理。

### 3.3 二次筛选 (客户端)

第一次筛选结果缓存为 pandas DataFrame 在 State 中:

```python
current_results = gr.State(pd.DataFrame())
```

二次筛选用 `df.query("pb < 1.5 and change_pct > 0")` 过滤，不调 LLM。

### 3.4 自选池数据流

```
对话筛选结果  ─→ 勾选 ─→ [加自选] ─→ INSERT INTO watchlist
策略面板筛选  ─→ 勾选 ─→ [加自选] ─→ INSERT INTO watchlist
                                         ↓
                              自选池 Tab ← 读取 watchlist + JOIN 最新指标
```

---

## 4. 实施优先级

| 优先级 | 功能 | 说明 |
|:--:|------|------|
| P0 | 对话 Tab 结果表格化 | 核心体验改变，AI 工具返回 JSON → 前端渲染 Dataframe |
| P0 | 全局选中 State + 加自选 | 勾选 → 写入 watchlist 表 |
| P1 | 自选池 Tab 完整实现 | 表格 + 详情 + 删除 |
| P1 | 二次筛选面板 | 折叠面板 + 客户端过滤 |
| P2 | 策略 Tab 筛选器 | 独立面板 + 保存策略 |
| P3 | 信号 Tab 骨架 + 右侧面板 | UI 就位，功能 Phase 4 |

---

## 5. 不做的

- 不做复杂图表 (K 线图、收益曲线) — Phase 4+
- 不做实时行情推送
- 不做多设备同步
- 自选池不做分组/文件夹
