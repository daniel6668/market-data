# Phase 3: AI Chat + Docker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans

**Goal:** Gradio 四Tab Web界面 + LLM Agent (DeepSeek/GLM/LM Studio) + 策略向导 + Docker 部署。

**Architecture:** `src/agent/` 模块包含：app.py (Gradio入口)、tools.py (8个工具函数)、llm.py (LLM后端抽象)、prompts.py (系统提示词)。工具函数调度 Phase 2 的因子/回测/筛选引擎。

**Tech Stack:** gradio, openai (兼容客户端), python-dotenv

---

## Tasks

### Task 1: LLM 后端抽象 (`src/agent/llm.py`)

**Files:** Create `src/agent/__init__.py`, `src/agent/llm.py`

```python
# src/agent/llm.py
"""LLM 后端抽象 — 支持 DeepSeek/GLM/OpenAI/LM Studio"""
import json
from openai import OpenAI

PROVIDERS = {
    "deepseek": {
        "base_url": "https://api.deepseek.com",
        "model": "deepseek-chat",
    },
    "glm": {
        "base_url": "https://open.bigmodel.cn/api/paas/v4",
        "model": "glm-4-flash",
    },
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "model": "gpt-4o-mini",
    },
    "lmstudio": {
        "base_url": "http://localhost:1234/v1",
        "model": "local-model",
    },
}

def get_client(config: dict) -> OpenAI:
    provider = config["llm"]["provider"]
    cfg = PROVIDERS.get(provider, PROVIDERS["deepseek"])
    return OpenAI(
        api_key=config["llm"]["api_key"],
        base_url=config["llm"].get("base_url", cfg["base_url"]),
    )

def chat(client: OpenAI, config: dict, messages: list, tools: list = None):
    provider = config["llm"]["provider"]
    cfg = PROVIDERS.get(provider, PROVIDERS["deepseek"])
    model = config["llm"].get("model", cfg["model"])
    kwargs = dict(model=model, messages=messages, temperature=0.3)
    if tools:
        kwargs["tools"] = tools
    return client.chat.completions.create(**kwargs)
```

### Task 2: Agent 工具函数 (`src/agent/tools.py`)

```python
# src/agent/tools.py
"""Agent 工具函数 — LLM Function Calling"""
import duckdb
import pandas as pd
from datetime import datetime

# 工具定义 (OpenAI function calling 格式)
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_stocks",
            "description": "多条件筛选股票。可指定 PE、RSI 等条件",
            "parameters": {
                "type": "object",
                "properties": {
                    "pe_max": {"type": "number", "description": "PE 上限"},
                    "rsi_max": {"type": "number", "description": "RSI 上限"},
                    "roe_min": {"type": "number", "description": "ROE 下限"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_backtest",
            "description": "回测均线交叉策略",
            "parameters": {
                "type": "object",
                "properties": {
                    "code": {"type": "string", "description": "股票代码，如 600519"},
                    "start": {"type": "string", "description": "开始日期 YYYY-MM-DD"},
                    "end": {"type": "string", "description": "结束日期"},
                },
                "required": ["code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "analyze_stock",
            "description": "分析单只股票的基本面和技术面",
            "parameters": {
                "type": "object",
                "properties": {
                    "code": {"type": "string", "description": "股票代码"},
                },
                "required": ["code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "market_overview",
            "description": "查看今日市场概况",
            "parameters": {"type": "object", "properties": {}},
        },
    },
]

def execute_tool(name: str, args: dict, conn: duckdb.DuckDBPyConnection) -> str:
    if name == "search_stocks":
        return _search_stocks(conn, args)
    elif name == "run_backtest":
        return _run_backtest(conn, args)
    elif name == "analyze_stock":
        return _analyze_stock(conn, args)
    elif name == "market_overview":
        return _market_overview(conn)
    return "未知工具"

def _search_stocks(conn, args):
    from src.screening.screener import StockScreener
    screener = StockScreener(conn)
    conditions = []
    if "pe_max" in args:
        conditions.append({"factor": "pe_ttm", "op": "lt", "value": args["pe_max"]})
    if "rsi_max" in args:
        conditions.append({"factor": "rsi14", "op": "lt", "value": args["rsi_max"]})
    if not conditions:
        return "请指定至少一个筛选条件"
    df = screener.by_conditions(conditions)
    if df.empty:
        return "无匹配结果"
    return df.head(20).to_markdown(index=False)

def _run_backtest(conn, args):
    from src.backtest.runner import BacktestRunner
    code = args["code"]
    start = args.get("start", "2025-01-01")
    end = args.get("end", datetime.now().strftime("%Y-%m-%d"))
    runner = BacktestRunner(conn)
    prices = runner._load_prices(code, start, end)
    if prices.empty:
        return f"{code}: 无数据"
    ma20 = prices.rolling(20).mean()
    ma60 = prices.rolling(60).mean()
    result = runner.run_single(code, ma20 > ma60, ma20 < ma60, start, end)
    return f"{code} 均线交叉回测:\n{result.summary()}\n同期买入持有: {result.benchmark_return:.2f}%"

def _analyze_stock(conn, args):
    code = args["code"]
    info = conn.execute("SELECT name, industry FROM stock_info WHERE ts_code LIKE ?", [f"{code}%"]).fetchone()
    basic = conn.execute("SELECT pe, pb FROM a_daily_basic WHERE ts_code LIKE ? ORDER BY trade_date DESC LIMIT 1", [f"{code}%"]).fetchone()
    factors = conn.execute("SELECT ma20, rsi14, ret_20d FROM stock_factors WHERE ts_code LIKE ? ORDER BY trade_date DESC LIMIT 1", [f"{code}%"]).fetchone()
    lines = [f"**{code}**" + (f" {info[0]}" if info else "")]
    if info: lines.append(f"行业: {info[1]}")
    if basic: lines.append(f"PE: {basic[0]:.1f}  PB: {basic[1]:.2f}")
    if factors: lines.append(f"MA20: {factors[0]:.2f}  RSI14: {factors[1]:.1f}  20日涨幅: {factors[2]:.1f}%")
    return "\n".join(lines) if len(lines) > 1 else f"{code}: 数据不足"

def _market_overview(conn):
    n_stocks = conn.execute("SELECT COUNT(*) FROM stock_info WHERE market='A'").fetchone()[0]
    nb = conn.execute("SELECT * FROM northbound_flow ORDER BY date DESC LIMIT 1").fetchone()
    lines = [f"A股上市: {n_stocks} 只"]
    if nb:
        lines.append(f"北向资金(最新): 沪 {nb[1]:.1f}亿  深 {nb[2]:.1f}亿")
    return "\n".join(lines)
```

### Task 3: Gradio 界面 (`src/agent/app.py`)

```python
# src/agent/app.py
"""Gradio Chat UI — 四 Tab 界面"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import gradio as gr
from src.agent.llm import get_client, chat, PROVIDERS
from src.agent.tools import TOOLS, execute_tool
from src.db import get_connection
from src.utils import load_config
import json

config = load_config()
conn = get_connection(config)

SYSTEM_PROMPT = """你是一个 A 股投资分析助手。你可以：
- 筛选股票 (search_stocks)
- 回测策略 (run_backtest)
- 分析个股 (analyze_stock)
- 查看市场 (market_overview)

请用中文简洁回复，重要数据用表格展示。"""

def respond(message, history):
    client = get_client(config)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in history:
        messages.append({"role": "user", "content": h[0]})
        if h[1]:
            messages.append({"role": "assistant", "content": h[1]})
    messages.append({"role": "user", "content": message})

    resp = chat(client, config, messages, TOOLS)
    msg = resp.choices[0].message

    if msg.tool_calls:
        messages.append({"role": "assistant", "content": None, "tool_calls": [
            {"id": tc.id, "type": "function", "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
            for tc in msg.tool_calls
        ]})
        for tc in msg.tool_calls:
            args = json.loads(tc.function.arguments)
            result = execute_tool(tc.function.name, args, conn)
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})
        resp2 = chat(client, config, messages)
        return resp2.choices[0].message.content

    return msg.content or "..."

def create_ui():
    with gr.Blocks(title="Market Data AI", theme=gr.themes.Soft()) as app:
        gr.Markdown("# 📊 Market Data AI — 投资分析助手")
        with gr.Tab("💬 对话"):
            gr.ChatInterface(respond, type="messages")
        with gr.Tab("⭐ 自选池"):
            gr.Markdown("自选池功能将在后续版本实现")
        with gr.Tab("📊 策略"):
            gr.Markdown("策略向导将在后续版本实现")
        with gr.Tab("🎯 信号"):
            gr.Markdown("信号面板将在后续版本实现")
    return app

if __name__ == "__main__":
    app = create_ui()
    app.launch(server_name="0.0.0.0", server_port=7860)
```

### Task 4: Docker + config 更新

**Files:** Create `Dockerfile`, `docker-compose.yml`, modify `config.example.yaml`

### Task 5: 测试 + 文档

---

## Execution

Run each task inline since code is fully specified.
