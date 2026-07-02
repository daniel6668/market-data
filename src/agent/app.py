"""Gradio UI — v2 策略研究-监控一体化"""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import gradio as gr
import pandas as pd
from src.agent.llm import get_client, chat
from src.agent.tools import TOOLS, execute_tool
from src.utils import load_config
from src.db import get_pending_actions, confirm_action

config = load_config()
_conn = None
def _get_conn():
    global _conn
    if _conn is None:
        from src.db import get_connection
        _conn = get_connection(config)
    return _conn

SYSTEM_PROMPT = """你是投资研究助手。使用 discover_stocks 工具将用户自然语言翻译为策略并执行。
可用工具:
- discover_stocks: 自然语言选股+回测（主力工具）
- get_monitor_signals: 查看监控信号/操作建议
- confirm_monitor_action: 确认/驳回建议
- get_watchlist: 查看关注列表
- add_to_watchlist: 加入关注列表
- remove_from_watchlist: 移出关注列表
- analyze_stock: 个股分析
- market_overview: 市场概况"""


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
        except Exception as e:
            r = f"失败:{e}"
        results.append((tc.function.name, r))

    table_df = pd.DataFrame()
    plan_json = ""
    for tn, r in results:
        if tn in ("search_stocks", "discover_stocks"):
            try:
                d = json.loads(r)
                if d.get("type") == "table" and d.get("rows"):
                    table_df = pd.DataFrame(d["rows"], columns=d["columns"])
                if d.get("plan"):
                    plan_json = json.dumps(d["plan"], ensure_ascii=False)
            except:
                pass

    # 获取 LLM 汇总
    messages.append({"role": "assistant", "content": None, "tool_calls": [
        {"id": tc.id, "type": "function", "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
        for tc in msg.tool_calls
    ]})
    for tc in msg.tool_calls:
        for tn, rr in results:
            if tn == tc.function.name:
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": rr}); break
    try:
        text = chat(client, config, messages).choices[0].message.content or ""
    except:
        text = f"共 {len(table_df)} 条"

    return text, table_df, plan_json


def load_watchlist_v2():
    """加载增强的关注列表（含收益+信号）"""
    rows = _get_conn().execute("""
        SELECT w.ts_code, w.name, w.market, w.entry_price, w.entry_date,
               p.current_price, p.cumulative_return, p.ret_5d,
               CASE WHEN p.below_ma60 THEN '🔴' WHEN p.below_ma20 THEN '🟡' ELSE '🟢' END as signal,
               p.macd_cross, w.strategy_name
        FROM watchlist w
        LEFT JOIN watchlist_performance p ON w.ts_code=p.ts_code
            AND p.calc_date=(SELECT MAX(calc_date) FROM watchlist_performance WHERE ts_code=w.ts_code)
        WHERE w.status='active'
        ORDER BY w.added_at DESC
    """).fetchall()
    cols = ["代码","名称","市场","加入价","加入日","现价","累计收益%","5日收益%","信号","MACD","策略"]
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([
        [r[0],r[1],r[2],r[3],str(r[4])[:10] if r[4] else "",
         r[5],round(r[6],1) if r[6] else 0,round(r[7],1) if r[7] else 0,
         r[8],r[9] or "-",r[10] or "-"] for r in rows
    ], columns=cols)


def load_pending_actions_df():
    """加载待审核建议"""
    actions = get_pending_actions(_get_conn())
    cols = ["ID","代码","名称","建议","原因","触发日期"]
    if not actions:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([
        [a["id"],a["ts_code"],a["name"],a["action"],a["reason"],a["trigger_date"]]
        for a in actions
    ], columns=cols)


def create_ui():
    provider = config.get("llm", {}).get("provider", "未配置")
    with gr.Blocks(title="Market Data Studio v2") as app:
        gr.Markdown(f"# 📊 Market Data Studio — {provider}")

        # ===== Tab 1: 发现 =====
        with gr.Tab("🔍 发现"):
            with gr.Row():
                with gr.Column(scale=1):
                    chatbot = gr.Chatbot(label="对话", height=350)
                    msg = gr.Textbox(label="输入你的策略想法",
                        placeholder="如：找出A股PE<20、MACD金叉的股票，做组合回测看看效果")
                    with gr.Row():
                        send = gr.Button("🚀 发送", variant="primary")
                        clear_btn = gr.Button("清除")
                with gr.Column(scale=1):
                    result_table = gr.Dataframe(label="筛选结果", interactive=False, wrap=True)
                    sel_codes = gr.Textbox(label="选中代码(逗号分隔)", interactive=True,
                        placeholder="点击表格行选中代码，或直接输入")
                    with gr.Row():
                        cond_input = gr.Textbox(label="策略描述/备注", scale=2)
                        add_btn = gr.Button("⭐ 加入关注列表", variant="primary", scale=1)
                    op_out = gr.Textbox(label="结果", interactive=False)
                    plan_state = gr.State("")  # 存储 plan JSON

            def on_send(msg_text, hist):
                text, df, plan_json = chat_respond(msg_text, hist or [])
                hist = (hist or []) + [
                    {"role":"user","content":msg_text},
                    {"role":"assistant","content":text}
                ]
                return hist, df if not df.empty else None, "", plan_json

            def on_table_select(evt: gr.SelectData, df):
                if df is None or df.empty:
                    return ""
                idx = evt.index
                if isinstance(idx, (list, tuple)):
                    row = idx[0]
                    codes = [str(df.iloc[row].iloc[0])] if row < len(df) else []
                else:
                    codes = [str(df.iloc[idx].iloc[0])] if idx < len(df) else []
                return ", ".join(codes)

            def do_add(sel_text, cond, plan_json):
                codes = [c.strip() for c in sel_text.split(",") if c.strip()]
                if not codes:
                    return "⚠️ 请先输入代码"
                desc = cond
                if plan_json:
                    try:
                        p = json.loads(plan_json)
                        desc = cond or json.dumps(p.get("conditions", []), ensure_ascii=False)
                    except:
                        pass
                args = {"codes": codes, "condition": desc}
                r = execute_tool("add_to_watchlist", args, _get_conn())
                return r

            send.click(on_send, [msg, chatbot],
                      [chatbot, result_table, msg, plan_state])
            clear_btn.click(lambda: ([], None, "", ""), [],
                           [chatbot, result_table, msg, plan_state])
            result_table.select(on_table_select, [result_table], [sel_codes])
            add_btn.click(do_add, [sel_codes, cond_input, plan_state], [op_out])

        # ===== Tab 2: 关注列表 =====
        with gr.Tab("👁 关注列表"):
            with gr.Row():
                wl_refresh = gr.Button("🔄 刷新")
                wl_export = gr.Button("📥 导出CSV")
            wl_table = gr.Dataframe(label="我的关注列表", interactive=False, wrap=True)
            with gr.Row():
                wl_codes = gr.Textbox(label="选中代码", interactive=True)
                wl_remove = gr.Button("🗑 移出")
            wl_info = gr.Textbox(label="结果", interactive=False)

            wl_refresh.click(load_watchlist_v2, [], wl_table)
            wl_table.select(on_table_select, [wl_table], [wl_codes])
            wl_remove.click(
                lambda sel: (execute_tool("remove_from_watchlist",
                    {"codes": [c.strip() for c in sel.split(",") if c.strip()]}, _get_conn())
                    if sel else "⚠️"),
                [wl_codes], [wl_info]).then(load_watchlist_v2, [], wl_table)

        # ===== Tab 3: 策略 =====
        with gr.Tab("📋 策略"):
            strategies_cfg = config.get("strategies", {})
            strategy_text = ""
            for mkt, s in strategies_cfg.items():
                strategy_text += f"## {s.get('label', mkt)} ({mkt})\n"
                strategy_text += f"**卖出规则:**\n"
                sr = s.get("sell_rules", {})
                strategy_text += f"- 止损: {sr.get('stop_loss', '-')}%  止盈: {sr.get('stop_profit', '-')}%\n"
                strategy_text += f"- 均线: {sr.get('ma_break', [])}\n"
                strategy_text += f"- 形态: {sr.get('pattern', [])}\n"
                strategy_text += f"**移除条件:**\n"
                for rc in s.get("remove_conditions", []):
                    strategy_text += f"- {rc.get('type')}: {rc}\n"
                strategy_text += "\n"
            gr.Markdown(strategy_text or "⚠️ 请在 config.yaml 中配置 strategies 段")

        # ===== Tab 4: 信号 =====
        with gr.Tab("⚡ 信号"):
            sig_refresh = gr.Button("🔄 刷新")
            sig_table = gr.Dataframe(label="待处理信号", interactive=False, wrap=True)
            with gr.Row():
                sig_id_input = gr.Number(label="信号ID", precision=0)
                with gr.Row():
                    sig_confirm = gr.Button("✅ 确认", variant="primary")
                    sig_dismiss = gr.Button("❌ 驳回")
            sig_info = gr.Textbox(label="结果", interactive=False)

            sig_refresh.click(load_pending_actions_df, [], sig_table)

            def do_confirm(aid):
                if not aid:
                    return "请输入信号ID", pd.DataFrame()
                r = execute_tool("confirm_monitor_action",
                    {"action_id": int(aid), "decision": "confirmed"}, _get_conn())
                return r, load_pending_actions_df()

            def do_dismiss(aid):
                if not aid:
                    return "请输入信号ID", pd.DataFrame()
                r = execute_tool("confirm_monitor_action",
                    {"action_id": int(aid), "decision": "dismissed"}, _get_conn())
                return r, load_pending_actions_df()

            sig_confirm.click(do_confirm, [sig_id_input], [sig_info, sig_table])
            sig_dismiss.click(do_dismiss, [sig_id_input], [sig_info, sig_table])

        # ===== Tab 5: 回测历史 =====
        with gr.Tab("📊 回测历史"):
            bt_refresh = gr.Button("🔄 刷新")
            bt_table = gr.Dataframe(label="回测记录", interactive=False, wrap=True)

            def load_bt_history():
                rows = _get_conn().execute("""
                    SELECT id, strategy_name, market, start_date, end_date,
                           total_return, annual_return, sharpe_ratio, max_drawdown,
                           n_stocks, created_at
                    FROM backtest_history ORDER BY created_at DESC LIMIT 50
                """).fetchall()
                cols = ["ID","策略","市场","开始","结束","总收益%","年化%","夏普","最大回撤%","股票数","时间"]
                if not rows:
                    return pd.DataFrame(columns=cols)
                return pd.DataFrame([
                    [r[0],r[1] or "-",r[2],str(r[3]),str(r[4]),
                     round(r[5],1) if r[5] else 0, round(r[6],1) if r[6] else 0,
                     round(r[7],2) if r[7] else 0, round(r[8],1) if r[8] else 0,
                     r[9], str(r[10])[:19] if r[10] else ""]
                    for r in rows
                ], columns=cols)

            bt_refresh.click(load_bt_history, [], bt_table)

    return app


if __name__ == "__main__":
    app = create_ui()
    print("Market Data Studio v2 → http://127.0.0.1:7860")
    app.launch(server_name="0.0.0.0", server_port=7860)
