# UI 交互重设计 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将 Gradio 界面从纯聊天升级为可交互操作台 — 筛选结果表格化、全局选中、自选池管理、二次筛选。

**Architecture:** 重写 `app.py` 为多组件布局，`tools.py` 返回 JSON 结构化数据，前端渲染为 gr.Dataframe 并管理 gr.State。自选池持久化到 DuckDB `watchlist` 表。

**Tech Stack:** Gradio 5.x, DuckDB, Python, pandas

---

## File Structure

```
Modify:  src/agent/app.py          ← 重写为多组件布局
Modify:  src/agent/tools.py        ← 返回 JSON 格式 + 新增工具
Modify:  src/db.py                 ← 新增 watchlist 表
Modify:  src/screening/screener.py ← 新增 search_with_indicators()
```

---

### Task 1: DuckDB watchlist 表 + 工具返回 JSON 化

**Files:** `src/db.py`, `src/agent/tools.py`, `src/screening/screener.py`

- [ ] **Step 1: 添加 watchlist 表到 db.py**

```python
# 添加到 create_tables() 末尾
conn.execute("""
    CREATE TABLE IF NOT EXISTS watchlist (
        ts_code   VARCHAR NOT NULL PRIMARY KEY,
        name      VARCHAR,
        added_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_condition TEXT,
        notes     TEXT
    )
""")
```

- [ ] **Step 2: 改造 tools.py — search_stocks 返回 JSON 格式 + 新增 add_to_watchlist**

```python
# 修改 _search_stocks — 返回 JSON 字符串包含 type/columns/rows/total
def _search_stocks(conn, args):
    from src.screening.screener import StockScreener
    screener = StockScreener(conn)
    conditions = []
    if "pe_max" in args:
        conditions.append({"factor": "pe_ttm", "op": "lt", "value": args["pe_max"]})
    if "rsi_max" in args:
        conditions.append({"factor": "rsi14", "op": "lt", "value": args["rsi_max"]})
    if not conditions:
        return json.dumps({"type": "error", "message": "请指定筛选条件"})
    # 用增强版：返回 PE/PB/涨跌幅等指标列
    df = screener.search_with_indicators(conditions)
    if df.empty:
        basic_count = conn.execute("SELECT COUNT(*) FROM a_daily_basic").fetchone()[0]
        return json.dumps({"type": "table", "columns": [], "rows": [], "total": 0,
                "hint": f"无匹配。a_daily_basic:{basic_count}行"})
    cols = ["ts_code","name","pe","pb","change_pct","main_net_5d"]
    rows = df[cols].values.tolist()
    return json.dumps({"type": "table",
        "columns": ["代码","名称","PE","PB","涨跌幅%","主力净流入5日"],
        "rows": rows, "total": len(df)}, ensure_ascii=False)


# 新增 add_to_watchlist 工具
def _add_to_watchlist(conn, args):
    codes = args.get("codes", [])
    condition = args.get("condition", "")
    if not codes:
        return "未指定股票"
    added = 0
    for code in codes:
        name_row = conn.execute(
            "SELECT name FROM stock_info WHERE ts_code=?", [code]).fetchone()
        name = name_row[0] if name_row else ""
        conn.execute(
            "INSERT OR REPLACE INTO watchlist (ts_code, name, source_condition) VALUES (?,?,?)",
            [code, name, condition])
        added += 1
    return f"✅ 已添加 {added} 只到自选池"

# 新增 get_watchlist 工具
def _get_watchlist(conn, args):
    rows = conn.execute("""
        SELECT w.ts_code, w.name, COALESCE(b.pe,0), COALESCE(b.pb,0),
               w.added_at, w.source_condition
        FROM watchlist w
        LEFT JOIN (
            SELECT ts_code, pe, pb FROM (
                SELECT ts_code, pe, pb,
                    ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) rn
                FROM a_daily_basic
            ) sub WHERE rn=1
        ) b ON w.ts_code = b.ts_code
        ORDER BY w.added_at DESC
    """).fetchall()
    if not rows:
        return json.dumps({"type": "table", "columns": [], "rows": [], "total": 0,
                "hint": "自选池为空"})
    data = [[r[0],r[1],round(r[2],2) if r[2] else 0,round(r[3],2) if r[3] else 0,
             str(r[4])[:10], r[5] or ""] for r in rows]
    return json.dumps({"type": "table",
        "columns": ["代码","名称","PE","PB","加入日期","筛选条件"],
        "rows": data, "total": len(data)}, ensure_ascii=False)
```

**更新 TOOLS 列表和 execute_tool:**

```python
# TOOLS 列表中追加 add_to_watchlist 和 get_watchlist
# execute_tool 中追加 elif 分支
```

- [ ] **Step 3: screener.py 新增 search_with_indicators()**

```python
def search_with_indicators(self, conditions: list[dict]) -> pd.DataFrame:
    """多条件筛选 + 返回指标列（PE/PB/涨跌幅/主力资金）"""
    # 复用 by_conditions 的 JOIN 逻辑，但 SELECT 更多列
    ...
    query = f"""
        SELECT si.ts_code, si.name,
               COALESCE(t0.pe,0) pe, COALESCE(t0.pb,0) pb,
               COALESCE(t1.ret_5d,0) change_pct,
               COALESCE(t1.main_net_5d,0) main_net_5d
        FROM stock_info si
        {joins}
        WHERE {" AND ".join(where)}
        ORDER BY t0.pe ASC NULLS LAST
        LIMIT 200
    """
```

- [ ] **Step 4: 运行已有测试确保不回归**

```bash
pytest tests/test_screening.py tests/test_db.py -v --timeout=60
```

- [ ] **Step 5: 提交**

```bash
git add src/db.py src/agent/tools.py src/screening/screener.py
git commit -m "feat: watchlist表 + tools返回JSON + search_with_indicators"
```

---

### Task 2: Gradio UI 重写 — 全局 State + 对话表格化 + 自选池

**Files:** `src/agent/app.py`

这是最大的改动。将 app.py 从简单的 ChatInterface 重写为多组件布局。

- [ ] **Step 1: 建造核心 UI 布局**

```python
# src/agent/app.py (完整重写)
import gradio as gr
import json
import pandas as pd
from src.agent.llm import get_client, chat
from src.agent.tools import TOOLS, execute_tool, _get_watchlist
from src.utils import load_config

config = load_config()
_conn = None
def _get_conn():
    global _conn
    if _conn is None:
        from src.db import get_connection
        _conn = get_connection(config)
    return _conn

SYSTEM_PROMPT = """你是 A 股投资分析助手。用工具帮用户筛选股票、分析个股、回测策略。
回复要简短，数据让表格说话。"""


def chat_respond(message, history):
    """对话处理 — 返回 (text, table_df) 元组"""
    llm_cfg = config.get("llm", {})
    if not llm_cfg.get("api_key"):
        return "⚠️ 请配置 LLM API Key", pd.DataFrame()

    client = get_client(config)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in history:
        if isinstance(h, dict):
            messages.append(h)
        elif isinstance(h, (list, tuple)) and len(h) >= 2:
            messages.append({"role": "user", "content": str(h[0])})
            if h[1]:
                messages.append({"role": "assistant", "content": str(h[1])})
    messages.append({"role": "user", "content": message})

    try:
        resp = chat(client, config, messages, TOOLS)
    except Exception as e:
        return f"❌ {e}", pd.DataFrame()

    msg = resp.choices[0].message
    if not msg.tool_calls:
        return msg.content or "", pd.DataFrame()

    # 执行工具
    results = []
    for tc in msg.tool_calls:
        try:
            args = json.loads(tc.function.arguments)
        except json.JSONDecodeError:
            args = {}
        try:
            r = execute_tool(tc.function.name, args, _get_conn())
            results.append((tc.function.name, r))
        except Exception as e:
            results.append((tc.function.name, f"工具失败:{e}"))

    # 解析结果 — 如果有 table 类型，提取为 DataFrame
    table_df = pd.DataFrame()
    for tool_name, r in results:
        if tool_name in ("search_stocks", "get_watchlist"):
            try:
                d = json.loads(r)
                if d.get("type") == "table" and d.get("rows"):
                    table_df = pd.DataFrame(d["rows"], columns=d["columns"])
            except (json.JSONDecodeError, Exception):
                pass

    # 让 LLM 总结
    messages.append({"role": "assistant", "content": None, "tool_calls": [
        {"id": tc.id, "type": "function",
         "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
        for tc in msg.tool_calls
    ]})
    for tc in msg.tool_calls:
        for tn, rr in results:
            if tn == tc.function.name:
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": rr})
                break
    try:
        resp2 = chat(client, config, messages)
        text = resp2.choices[0].message.content or ""
    except Exception:
        text = f"找到 {len(table_df)} 条结果"
    return text, table_df


def add_to_watchlist(selected_rows, condition=""):
    """将选中的行加入自选池"""
    if selected_rows is None or len(selected_rows) == 0:
        return "⚠️ 请先勾选股票", pd.DataFrame()
    codes = []
    for row in selected_rows.itertuples():
        code = row[0] if hasattr(row, '_1') else row.ts_code if hasattr(row, 'ts_code') else None
        if code:
            codes.append(code)
    if not codes:
        codes = selected_rows.iloc[:, 0].tolist() if hasattr(selected_rows, 'iloc') else []
    
    args = {"codes": codes, "condition": condition}
    conn = _get_conn()
    from src.agent.tools import execute_tool
    msg = execute_tool("add_to_watchlist", args, conn)
    # 刷新自选池表格
    wl_json = _get_watchlist(conn, {})
    try:
        wl = json.loads(wl_json)
        wl_df = pd.DataFrame(wl["rows"], columns=wl["columns"]) if wl.get("rows") else pd.DataFrame()
    except Exception:
        wl_df = pd.DataFrame()
    return msg, wl_df


def load_watchlist():
    """加载自选池"""
    conn = _get_conn()
    wl_json = _get_watchlist(conn, {})
    try:
        wl = json.loads(wl_json)
        return pd.DataFrame(wl["rows"], columns=wl["columns"]) if wl.get("rows") else pd.DataFrame()
    except Exception:
        return pd.DataFrame()
```

- [ ] **Step 2: 建造 Gradio Blocks 布局**

```python
def create_ui():
    with gr.Blocks(title="Market Data AI") as app:
        gr.Markdown("# 📊 Market Data AI")

        # 全局状态
        selected = gr.State([])
        current_table = gr.State(pd.DataFrame())

        with gr.Tab("💬 对话"):
            with gr.Row():
                with gr.Column(scale=3):
                    chatbot = gr.Chatbot(label="对话", height=500)
                    msg = gr.Textbox(label="输入", placeholder="如：帮我找 PE<30 的股票")
                    send = gr.Button("发送")
                with gr.Column(scale=2):
                    result_table = gr.Dataframe(
                        label="筛选结果", interactive=True,
                        row_count=(10, "dynamic"), max_rows=200)
                    select_info = gr.Textbox(label="已选", value="未选中", interactive=False)
                    with gr.Row():
                        add_btn = gr.Button("⭐ 加入自选池", variant="primary")
                    result_count = gr.Textbox(label="状态", interactive=False)

            # 对话发送事件
            def on_send(message, history):
                text, df = chat_respond(message, history)
                new_history = history + [{"role": "user", "content": message},
                                         {"role": "assistant", "content": text}]
                total = len(df) if not df.empty else 0
                return new_history, df, f"共 {total} 条", ""
            send.click(on_send, [msg, chatbot], [chatbot, result_table, result_count, msg])

            # 加自选
            add_btn.click(
                lambda df: add_to_watchlist(df, ""),
                [result_table], [select_info]
            )

        with gr.Tab("⭐ 自选池"):
            wl_refresh = gr.Button("刷新")
            wl_table = gr.Dataframe(label="自选池", interactive=True,
                                     row_count=(20, "dynamic"))
            wl_remove = gr.Button("🗑 移出选中")
            wl_info = gr.Textbox(label="操作结果", interactive=False)
            wl_refresh.click(load_watchlist, [], wl_table)

        with gr.Tab("📊 策略"):
            gr.Markdown("### 策略筛选器\n开发中")

        with gr.Tab("🎯 信号"):
            gr.Markdown("### 信号面板\n开发中")

    return app
```

- [ ] **Step 3: 本地验证**

```bash
# 确保原有测试通过
python -m pytest tests/ -q --timeout=60
```

- [ ] **Step 4: 提交**

```bash
git add src/agent/app.py
git commit -m "feat: UI 重写 — 对话表格化 + 全局选中 + 自选池操作"
```

---

### Task 3: Docker 重建 + 端到端验证

- [ ] **Step 1: 停止旧容器, 重建启动**

```bash
docker compose down
docker compose build --no-cache
docker compose up -d
```

- [ ] **Step 2: 验证 HTTP 200**

```bash
curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:7860
# 预期: 200
```

- [ ] **Step 3: 提交**

```bash
git add -A && git commit -m "chore: Docker 重建 + 端到端验证通过"
git push origin master
```

---

### Task 4: 文档更新

- [ ] **Step 1: 更新 README 和 tech-doc**

- [ ] **Step 2: 提交**

---

## Summary

| Task | 改动 | 描述 |
|:--:|------|------|
| 1 | `db.py` + `tools.py` + `screener.py` | watchlist 表 + JSON 返回 + 增强筛选 |
| 2 | `app.py` | UI 重写 — State/Dataframe/自选池 |
| 3 | Docker | 重建 + 验证 |
| 4 | Docs | 更新 |
