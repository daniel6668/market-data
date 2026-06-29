"""Gradio UI — 对话表格化 + 自选池 + 全局选中"""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import gradio as gr
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

SYSTEM_PROMPT = """你是 A 股投资分析助手。用工具帮用户筛选股票、分析个股、回测策略。回复简洁，数据让表格说话。"""

# ── 对话处理 ──

def chat_respond(message, history):
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

    # 提取表格
    table_df = pd.DataFrame()
    for tool_name, r in results:
        if tool_name in ("search_stocks", "get_watchlist"):
            try:
                d = json.loads(r)
                if d.get("type") == "table" and d.get("rows"):
                    table_df = pd.DataFrame(d["rows"], columns=d["columns"])
            except Exception:
                pass

    # LLM 总结
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


# ── 自选池操作 ──

def _do_add_watchlist(codes, condition):
    if not codes:
        return "⚠️ 未选中股票"
    args = {"codes": codes, "condition": condition or ""}
    return execute_tool("add_to_watchlist", args, _get_conn())


def _do_remove_watchlist(codes):
    if not codes:
        return "⚠️ 未选中股票"
    args = {"codes": codes}
    return execute_tool("remove_from_watchlist", args, _get_conn())


def load_watchlist_df():
    wl_json = _get_watchlist(_get_conn(), {})
    try:
        wl = json.loads(wl_json)
        return pd.DataFrame(wl["rows"], columns=wl["columns"]) if wl.get("rows") else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def refresh_watchlist():
    return load_watchlist_df()


def add_selected_to_wl(df, condition):
    if df is None or (hasattr(df, 'empty') and df.empty):
        return "⚠️ 无数据可选", load_watchlist_df()
    codes = df.iloc[:, 0].tolist() if hasattr(df, 'iloc') else []
    msg = _do_add_watchlist(codes, condition)
    return msg, load_watchlist_df()


def remove_selected_from_wl(df):
    if df is None or (hasattr(df, 'empty') and df.empty):
        return "⚠️ 无数据可选", load_watchlist_df()
    codes = df.iloc[:, 0].tolist() if hasattr(df, 'iloc') else []
    msg = _do_remove_watchlist(codes)
    return msg, load_watchlist_df()


# ── UI 布局 ──

def create_ui():
    with gr.Blocks(title="Market Data AI") as app:
        provider = config.get("llm", {}).get("provider", "未配置")
        gr.Markdown(f"# 📊 Market Data AI — {provider}")

        with gr.Tab("💬 对话"):
            with gr.Row():
                with gr.Column(scale=3):
                    chatbot = gr.Chatbot(label="对话", height=450)
                    msg = gr.Textbox(label="输入", placeholder="如：帮我找 PE<30 的股票")
                    with gr.Row():
                        send = gr.Button("发送", variant="primary")
                        clear = gr.Button("清空")
                with gr.Column(scale=2):
                    result_table = gr.Dataframe(
                        label="筛选结果 (勾选后可操作)", interactive=True,
                        headers=["代码","名称","PE","PB","涨跌幅%"],
                        datatype=["str","str","number","number","number"],
                        min_width=400)
                    select_info = gr.Textbox(label="操作结果", interactive=False)
                    with gr.Row():
                        add_wl_btn = gr.Button("⭐ 加入自选池")
                    cond_input = gr.Textbox(label="筛选条件记录", placeholder="PE<30")

            def on_send(message, history):
                text, df = chat_respond(message, history or [])
                if history is None:
                    history = []
                new_history = history + [
                    {"role": "user", "content": message},
                    {"role": "assistant", "content": text},
                ]
                return new_history, df if not df.empty else None

            send.click(on_send, [msg, chatbot], [chatbot, result_table]).then(
                lambda: "", None, msg)
            clear.click(lambda: ([], None, ""), None, [chatbot, result_table, select_info])
            add_wl_btn.click(
                lambda df, cond: add_selected_to_wl(df, cond),
                [result_table, cond_input], [select_info]
            )

        with gr.Tab("⭐ 自选池"):
            with gr.Row():
                wl_refresh = gr.Button("🔄 刷新")
            wl_table = gr.Dataframe(
                label="我的自选池", interactive=True,
                headers=["代码","名称","PE","PB","加入日期","筛选条件"],
                datatype=["str","str","number","number","str","str"])
            with gr.Row():
                wl_remove = gr.Button("🗑 移出选中")
            wl_info = gr.Textbox(label="操作结果", interactive=False)

            wl_refresh.click(refresh_watchlist, [], wl_table)
            wl_remove.click(
                lambda df: remove_selected_from_wl(df),
                [wl_table], [wl_info, wl_table]
            )

        with gr.Tab("📊 策略"):
            gr.Markdown("### 策略筛选器\n独立筛选面板开发中...")

        with gr.Tab("🎯 信号"):
            gr.Markdown("### 信号面板\nPhase 4 开发中...")

    return app


if __name__ == "__main__":
    app = create_ui()
    print("=" * 50)
    print("  Market Data AI 启动成功！")
    print("  http://127.0.0.1:7860")
    print("=" * 50)
    app.launch(server_name="0.0.0.0", server_port=7860)
