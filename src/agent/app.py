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

SYSTEM_PROMPT = """你是 A 股投资分析助手。用 search_stocks 工具筛选股票。
支持因子: pe_ttm(PE), pb(PB), ma5/ma10/ma20/ma60(均线),
rsi6/rsi14(RSI), macd_dif/macd_dea(MACD), vol_ratio(量比),
ret_5d/ret_20d(涨跌幅), turnover_rate(换手率)。
运算符: gt(>), lt(<), gte(>=), lte(<=)。
跨因子比较(如MACD金叉): {"factor":"macd_dif","op":"gt","value":"macd_dea"}
提示: "盈利"=pe_ttm>0, "低估值"=pe_ttm<15, "放量"=vol_ratio>1.5"""


def chat_respond(message, history):
    llm_cfg = config.get("llm", {})
    if not llm_cfg.get("api_key"):
        return "⚠️ 请配置 LLM API Key", pd.DataFrame()

    client = get_client(config)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in (history or []):
        if isinstance(h, dict): messages.append(h)
        elif isinstance(h, (list, tuple)) and len(h) >= 2:
            messages.append({"role": "user", "content": str(h[0])})
            if h[1]: messages.append({"role": "assistant", "content": str(h[1])})
    messages.append({"role": "user", "content": message})

    try: resp = chat(client, config, messages, TOOLS)
    except Exception as e: return f"❌ {e}", pd.DataFrame()

    msg = resp.choices[0].message
    if not msg.tool_calls: return msg.content or "", pd.DataFrame()

    results = []
    for tc in msg.tool_calls:
        try: args = json.loads(tc.function.arguments)
        except json.JSONDecodeError: args = {}
        try: r = execute_tool(tc.function.name, args, _get_conn())
        except Exception as e: r = f"失败:{e}"
        results.append((tc.function.name, r))

    table_df = pd.DataFrame()
    for tn, r in results:
        if tn == "search_stocks":
            try:
                d = json.loads(r)
                if d.get("type") == "table" and d.get("rows"):
                    table_df = pd.DataFrame(d["rows"], columns=d["columns"])
            except: pass

    messages.append({"role": "assistant", "content": None, "tool_calls": [
        {"id": tc.id, "type": "function", "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
        for tc in msg.tool_calls
    ]})
    for tc in msg.tool_calls:
        for tn, rr in results:
            if tn == tc.function.name:
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": rr}); break
    try: text = chat(client, config, messages).choices[0].message.content or ""
    except: text = f"共 {len(table_df)} 条"
    return text, table_df


# ── 自选池 ──

def load_watchlist_df():
    cols = ["代码","名称","PE","PB","加入日期","筛选条件"]
    rows = _get_conn().execute("""
        SELECT w.ts_code, w.name, COALESCE(b.pe,0), COALESCE(b.pb,0),
               w.added_at, w.source_condition
        FROM watchlist w LEFT JOIN (
            SELECT ts_code, pe, pb FROM (
                SELECT ts_code, pe, pb,
                    ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) rn
                FROM a_daily_basic) sub WHERE rn=1
        ) b ON w.ts_code=b.ts_code ORDER BY w.added_at DESC
    """).fetchall()
    if not rows: return pd.DataFrame(columns=cols)
    return pd.DataFrame([[r[0],r[1],round(r[2],2) if r[2] else 0,round(r[3],2) if r[3] else 0,
                          str(r[4])[:10] if r[4] else "", r[5] or ""] for r in rows], columns=cols)


def create_ui():
    provider = config.get("llm", {}).get("provider", "未配置")
    with gr.Blocks(title="Market Data AI") as app:
        gr.Markdown(f"# 📊 Market Data AI — {provider}")

        # ── 对话 Tab ──
        with gr.Tab("💬 对话"):
            with gr.Row():
                with gr.Column(scale=1):
                    chatbot = gr.Chatbot(label="对话", height=400)
                    msg = gr.Textbox(label="输入", placeholder="如：找 MACD 近3日金叉 的股票")
                    send = gr.Button("发送", variant="primary")
                with gr.Column(scale=1):
                    result_table = gr.Dataframe(label="结果表格", interactive=False, wrap=True,
                        headers=["代码","名称","PE","PB","涨跌幅%"])
                    sel_codes = gr.Textbox(label="选中代码(逗号分隔)", interactive=True, placeholder="点击行选中后复制到这里，或直接输入")
                    with gr.Row():
                        cond_input = gr.Textbox(label="筛选条件", scale=2)
                        add_btn = gr.Button("⭐ 加入自选池", variant="primary", scale=1)
                    op_out = gr.Textbox(label="结果", interactive=False)

            def on_send(msg_text, hist):
                text, df = chat_respond(msg_text, hist or [])
                hist = (hist or []) + [{"role":"user","content":msg_text}, {"role":"assistant","content":text}]
                return hist, df if not df.empty else None, ""

            def on_table_select(evt: gr.SelectData, df):
                if df is None or df.empty: return ""
                idx = evt.index
                # Gradio SelectData.index = [row, col] 或单行号
                if isinstance(idx, (list, tuple)):
                    row = idx[0]  # 只取行号
                    codes = [str(df.iloc[row].iloc[0])] if row < len(df) else []
                else:
                    codes = [str(df.iloc[idx].iloc[0])] if idx < len(df) else []
                return ", ".join(codes)

            def do_add(sel_text, cond):
                codes = [c.strip() for c in sel_text.split(",") if c.strip()]
                if not codes: return "⚠️ 请先输入代码", pd.DataFrame()
                args = {"codes": codes, "condition": cond or ""}
                r = execute_tool("add_to_watchlist", args, _get_conn())
                return r

            send.click(on_send, [msg, chatbot], [chatbot, result_table, msg])
            result_table.select(on_table_select, [result_table], [sel_codes])
            add_btn.click(do_add, [sel_codes, cond_input], [op_out])

        # ── 自选池 Tab ──
        with gr.Tab("⭐ 自选池"):
            wl_refresh = gr.Button("🔄 刷新")
            wl_table = gr.Dataframe(label="我的自选池", interactive=False, wrap=True,
                headers=["代码","名称","PE","PB","加入日期","筛选条件"])
            wl_codes = gr.Textbox(label="选中代码", interactive=True)
            wl_remove = gr.Button("🗑 移出")
            wl_info = gr.Textbox(label="结果", interactive=False)

            wl_refresh.click(load_watchlist_df, [], wl_table)
            wl_table.select(on_table_select, [wl_table], [wl_codes])
            wl_remove.click(
                lambda sel: execute_tool("remove_from_watchlist",
                    {"codes": [c.strip() for c in sel.split(",") if c.strip()]}, _get_conn())
                if sel else "⚠️", [wl_codes], [wl_info]).then(load_watchlist_df, [], wl_table)

        with gr.Tab("📊 策略"): gr.Markdown("开发中")
        with gr.Tab("🎯 信号"): gr.Markdown("Phase 4")

    return app


if __name__ == "__main__":
    app = create_ui()
    print("Market Data AI → http://127.0.0.1:7860")
    app.launch(server_name="0.0.0.0", server_port=7860)
