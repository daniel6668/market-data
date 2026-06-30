"""Gradio UI — 对话表格化 + 自选池"""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import gradio as gr
import pandas as pd
from src.agent.llm import get_client, chat
from src.agent.tools import TOOLS, execute_tool
from src.utils import load_config

config = load_config()
_conn = None

def _get_conn():
    global _conn
    if _conn is None:
        from src.db import get_connection
        _conn = get_connection(config)
    return _conn

SYSTEM_PROMPT = """你是 A 股投资分析助手。用工具帮用户筛选股票。回复简洁。"""


def chat_respond(message, history):
    llm_cfg = config.get("llm", {})
    if not llm_cfg.get("api_key"):
        return "⚠️ 请配置 LLM API Key", pd.DataFrame(), ""

    client = get_client(config)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in (history or []):
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
        return f"❌ {e}", pd.DataFrame(), ""

    msg = resp.choices[0].message
    if not msg.tool_calls:
        return msg.content or "", pd.DataFrame(), ""

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
            results.append((tc.function.name, f"失败:{e}"))

    # 提取表格
    table_df = pd.DataFrame()
    for tool_name, r in results:
        if tool_name == "search_stocks":
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
        text = f"共 {len(table_df)} 条结果"
    return text, table_df, ""


def _read_selected_rows(df, selected_indices):
    """从 Dataframe 和选中行号提取股票代码列表"""
    if df is None or df.empty or not selected_indices:
        return []
    codes = []
    for idx in selected_indices:
        if idx < len(df):
            row = df.iloc[idx]
            codes.append(str(row.iloc[0]))  # 第一列是代码
    return codes


def add_to_watchlist(selected_text, condition):
    """selected_text: 逗号分隔的代码字符串"""
    codes = [c.strip() for c in selected_text.split(",") if c.strip() and c.strip() != "点击表格行选中股票"]
    if not codes:
        return "⚠️ 请先在表格中点击选中股票", pd.DataFrame()
    args = {"codes": codes, "condition": condition or ""}
    msg = execute_tool("add_to_watchlist", args, _get_conn())
    return msg, load_watchlist_df()


def remove_from_watchlist(selected_text):
    codes = [c.strip() for c in selected_text.split(",") if c.strip()]
    if not codes:
        return "⚠️ 请先选中要移除的股票", pd.DataFrame()
    args = {"codes": codes}
    msg = execute_tool("remove_from_watchlist", args, _get_conn())
    return msg, load_watchlist_df()


def load_watchlist_df():
    conn = _get_conn()
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
        return pd.DataFrame()
    data = [[r[0], r[1], round(r[2],2) if r[2] else 0, round(r[3],2) if r[3] else 0,
             str(r[4])[:10] if r[4] else "", r[5] or ""] for r in rows]
    return pd.DataFrame(data, columns=["代码","名称","PE","PB","加入日期","筛选条件"])


def create_ui():
    provider = config.get("llm", {}).get("provider", "未配置")

    with gr.Blocks(title="Market Data AI") as app:
        gr.Markdown(f"# 📊 Market Data AI — {provider}")

        with gr.Tab("💬 对话"):
            with gr.Row():
                with gr.Column(scale=2):
                    chatbot = gr.Chatbot(label="对话", height=400)
                    msg = gr.Textbox(label="输入", placeholder="如：帮我找 PE<10 的股票")
                    with gr.Row():
                        send = gr.Button("发送", variant="primary")
                with gr.Column(scale=3):
                    result_table = gr.Dataframe(
                        label="筛选结果 — 点击行选中，Ctrl+点击多选",
                        interactive=False,
                        wrap=True)
                    with gr.Row():
                        selected_box = gr.Textbox(label="已选中", placeholder="点击表格中的行来选中", interactive=False)
                        clear_sel = gr.Button("清除选择")
                    with gr.Row():
                        cond_input = gr.Textbox(label="筛选条件记录", scale=2)
                        add_wl_btn = gr.Button("⭐ 加入自选池", variant="primary", scale=1)
                    op_info = gr.Textbox(label="操作结果", interactive=False)

            def on_send(message, history):
                text, df, _ = chat_respond(message, history or [])
                if history is None:
                    history = []
                new_history = history + [
                    {"role": "user", "content": message},
                    {"role": "assistant", "content": text},
                ]
                return new_history, df, "点击表格行选中股票"

            def on_select(evt: gr.SelectData, df):
                if df is None or df.empty:
                    return "无数据"
                idx = evt.index[0] if hasattr(evt.index, '__iter__') else evt.index
                if isinstance(idx, list):
                    codes = [str(df.iloc[i].iloc[0]) for i in idx if i < len(df)]
                else:
                    codes = [str(df.iloc[idx].iloc[0])] if idx < len(df) else []
                return ", ".join(codes)

            send.click(on_send, [msg, chatbot], [chatbot, result_table, selected_box]).then(
                lambda: "", None, msg)
            result_table.select(on_select, [result_table], [selected_box])
            clear_sel.click(lambda: ("点击表格行选中股票", ""), None, [selected_box, op_info])
            add_wl_btn.click(
                lambda sel, cond: add_to_watchlist(sel, cond),
                [selected_box, cond_input], [op_info])

        with gr.Tab("⭐ 自选池"):
            wl_refresh = gr.Button("🔄 刷新")
            wl_table = gr.Dataframe(label="我的自选池", interactive=False, wrap=True)
            with gr.Row():
                wl_selected = gr.Textbox(label="已选中", interactive=False, scale=2)
                wl_remove = gr.Button("🗑 移出", scale=1)
            wl_info = gr.Textbox(label="操作结果", interactive=False)

            wl_refresh.click(load_watchlist_df, [], wl_table)
            wl_table.select(on_select, [wl_table], [wl_selected])
            # 简化版：直接用选中代码文本
            wl_remove.click(
                remove_from_watchlist,
                [wl_selected], [wl_info, wl_table]
            )

        with gr.Tab("📊 策略"):
            gr.Markdown("### 策略筛选器\n开发中...")
        with gr.Tab("🎯 信号"):
            gr.Markdown("### 信号面板\nPhase 4...")

    return app


def _parse_selection(sel_text):
    """从选中文本解析代码列表"""
    if not sel_text or sel_text == "点击表格行选中股票":
        return []
    return [c.strip() for c in sel_text.split(",") if c.strip()]


if __name__ == "__main__":
    app = create_ui()
    print("=" * 50)
    print("  Market Data AI")
    print("  http://127.0.0.1:7860")
    print("=" * 50)
    app.launch(server_name="0.0.0.0", server_port=7860)
