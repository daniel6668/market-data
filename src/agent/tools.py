"""Agent 工具函数 — 灵活因子筛选 + 自选池管理"""
import json
import duckdb
import pandas as pd
from datetime import datetime

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_stocks",
            "description": "多条件筛选股票。支持因子: pe_ttm(PE), pb(PB), "
                           "ma5,ma20,ma60(均线), macd_dif,macd_dea(MACD), "
                           "rsi6,rsi14(RSI), ret_5d,ret_20d(涨跌幅), "
                           "vol_ratio(量比), turnover_rate(换手率). "
                           "运算符: gt(>), lt(<), gte(>=), lte(<=). "
                           "示例: MACD金叉→factor:macd_dif,op:gt,value:macd_dea",
            "parameters": {
                "type": "object",
                "properties": {
                    "conditions": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "factor": {"type": "string"},
                                "op": {"type": "string", "enum": ["gt","lt","gte","lte"]},
                                "value": {"type": "string"},
                            },
                            "required": ["factor","op","value"],
                        },
                        "description": "条件列表，AND关系",
                    },
                },
                "required": ["conditions"],
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
                    "code": {"type": "string"},
                    "start": {"type": "string"},
                    "end": {"type": "string"},
                },
                "required": ["code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "analyze_stock",
            "description": "分析个股基本面和技术面",
            "parameters": {"type": "object", "properties": {"code": {"type": "string"}}, "required": ["code"]},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "market_overview",
            "description": "查看市场概况",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "add_to_watchlist",
            "description": "将股票加入自选池",
            "parameters": {
                "type": "object",
                "properties": {
                    "codes": {"type": "array", "items": {"type": "string"}},
                    "condition": {"type": "string"},
                },
                "required": ["codes"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_watchlist",
            "description": "查看自选池",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "remove_from_watchlist",
            "description": "从自选池移除",
            "parameters": {
                "type": "object",
                "properties": {"codes": {"type": "array", "items": {"type": "string"}}},
                "required": ["codes"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "discover_stocks",
            "description": "用自然语言描述选股策略，系统翻译为筛选条件后执行全市场扫描+组合回测。"
                           "支持描述：低估值、MACD金叉、放量上涨、超卖反弹等。"
                           "示例：'找出A股PE<15、MACD金叉的股票并回测'",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "自然语言策略描述"},
                    "market": {"type": "string", "enum": ["A","ETF","HK","US","all"]},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_monitor_signals",
            "description": "查看最新监控信号：卖出建议、移除建议、新机会",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {"type": "string", "enum": ["SELL","REDUCE","REMOVE","BUY","all"]},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "confirm_monitor_action",
            "description": "确认或驳回监控建议",
            "parameters": {
                "type": "object",
                "properties": {
                    "action_id": {"type": "integer"},
                    "decision": {"type": "string", "enum": ["confirmed","dismissed"]},
                },
                "required": ["action_id", "decision"],
            },
        },
    },
]


def execute_tool(name: str, args: dict, conn: duckdb.DuckDBPyConnection) -> str:
    m = {
        "search_stocks": _search_stocks,
        "run_backtest": _run_backtest,
        "analyze_stock": _analyze_stock,
        "market_overview": _market_overview,
        "add_to_watchlist": _add_to_watchlist,
        "get_watchlist": _get_watchlist,
        "remove_from_watchlist": _remove_from_watchlist,
        "discover_stocks": _discover_stocks,
        "get_monitor_signals": _get_monitor_signals,
        "confirm_monitor_action": _confirm_monitor_action,
    }
    return m.get(name, lambda c, a: "未知工具")(conn, args)


def _search_stocks(conn, args):
    from src.screening.screener import StockScreener
    screener = StockScreener(conn)
    raw = args.get("conditions", [])
    # 转换 value: 可能是数字或因子名(跨因子比较如 macd_dif > macd_dea)
    conditions = []
    for c in raw:
        v = c.get("value")
        try:
            v = float(v)
        except (ValueError, TypeError):
            pass  # 保持为字符串(跨因子比较)
        conditions.append({"factor": c["factor"], "op": c.get("op", "lt"), "value": v})
    if not conditions:
        return json.dumps({"type": "error", "message": "请指定筛选条件"})

    df = screener.search_with_indicators(conditions)
    if df.empty:
        basic_count = conn.execute("SELECT COUNT(*) FROM a_daily_basic").fetchone()[0]
        return json.dumps({"type": "table", "columns": [], "rows": [], "total": 0,
                "hint": f"无匹配。a_daily_basic:{basic_count}行"}, ensure_ascii=False)

    cols = ["ts_code", "name", "pe", "pb", "change_pct"]
    avail = [c for c in cols if c in df.columns]
    rows = df[avail].values.tolist()
    col_labels = {"ts_code": "代码", "name": "名称", "pe": "PE", "pb": "PB", "change_pct": "涨跌幅%"}
    return json.dumps({
        "type": "table",
        "columns": [col_labels.get(c, c) for c in avail],
        "rows": rows, "total": len(df),
    }, ensure_ascii=False, default=str)


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
    return f"**{code} 均线交叉回测**\n{result.summary()}\n同期买入持有: {result.benchmark_return:.2f}%"


def _analyze_stock(conn, args):
    code = args["code"]
    p = f"{code}%"
    info = conn.execute("SELECT name, industry FROM stock_info WHERE ts_code LIKE ?", [p]).fetchone()
    basic = conn.execute("SELECT pe, pb, turnover_rate FROM a_daily_basic WHERE ts_code LIKE ? ORDER BY trade_date DESC LIMIT 1", [p]).fetchone()
    factors = conn.execute("SELECT ma20, ma60, rsi14, ret_20d, vol_ratio FROM stock_factors WHERE ts_code LIKE ? ORDER BY trade_date DESC LIMIT 1", [p]).fetchone()
    lines = [f"**{code}**" + (f" — {info[0]}" if info else "")]
    if info and info[1]: lines.append(f"- 行业: {info[1]}")
    if basic: lines.append(f"- PE: {basic[0]:.1f}  PB: {basic[1]:.2f}  换手率: {basic[2]:.2f}%")
    if factors: lines.append(f"- MA20: {factors[0]:.2f}  MA60: {factors[1]:.2f}\n- RSI14: {factors[2]:.1f}  20日涨幅: {factors[3]:.1f}%  量比: {factors[4]:.2f}")
    return "\n".join(lines) if len(lines) > 1 else f"{code}: 数据不足"


def _market_overview(conn):
    n = conn.execute("SELECT COUNT(*) FROM stock_info WHERE market='A'").fetchone()[0]
    nb = conn.execute("SELECT date, hgt_yi, sgt_yi FROM northbound_flow ORDER BY date DESC LIMIT 1").fetchone()
    lines = [f"- A 股上市: {n} 只"]
    if nb: lines.append(f"- 北向({nb[0]}): 沪 {nb[1]:.1f}亿  深 {nb[2]:.1f}亿")
    return "\n".join(lines)


def _add_to_watchlist(conn, args):
    codes = args.get("codes", [])
    condition = args.get("condition", "")
    if not codes: return "未指定股票"
    added = 0
    for code in codes:
        name_row = conn.execute(
            "SELECT name, market FROM stock_info WHERE ts_code LIKE ?", [f"{code}%"]).fetchone()
        name = name_row[0] if name_row else code
        market = name_row[1] if name_row else "A"

        # 获取当前价格作为 entry_price
        price_row = conn.execute(
            "SELECT close FROM a_daily WHERE ts_code LIKE ? ORDER BY trade_date DESC LIMIT 1",
            [f"{code}%"]).fetchone()
        entry_price = price_row[0] if price_row else None

        from datetime import date
        conn.execute("""
            INSERT OR REPLACE INTO watchlist
            (ts_code, name, source_condition, status, market, entry_price, entry_date)
            VALUES (?, ?, ?, 'active', ?, ?, ?)
        """, [code, name, condition, market, entry_price, date.today()])
        added += 1
    return f"✅ 已添加 {added} 只到自选池"


def _get_watchlist(conn, args):
    rows = conn.execute("""
        SELECT w.ts_code, w.name, COALESCE(b.pe,0), COALESCE(b.pb,0), w.added_at, w.source_condition
        FROM watchlist w
        LEFT JOIN (SELECT ts_code, pe, pb FROM (SELECT ts_code, pe, pb, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) rn FROM a_daily_basic) sub WHERE rn=1) b ON w.ts_code=b.ts_code
        ORDER BY w.added_at DESC
    """).fetchall()
    if not rows: return json.dumps({"type":"table","columns":[],"rows":[],"total":0,"hint":"自选池为空"})
    data = [[r[0],r[1],round(r[2],2) if r[2] else 0,round(r[3],2) if r[3] else 0,str(r[4])[:10] if r[4] else "",r[5] or ""] for r in rows]
    return json.dumps({"type":"table","columns":["代码","名称","PE","PB","加入日期","筛选条件"],"rows":data,"total":len(data)}, ensure_ascii=False)


def _remove_from_watchlist(conn, args):
    codes = args.get("codes", [])
    if not codes: return "未指定股票"
    for code in codes: conn.execute("DELETE FROM watchlist WHERE ts_code=?", [code])
    return f"✅ 已移除 {len(codes)} 只"


def _discover_stocks(conn, args):
    """发现引擎：NL → 条件翻译 → 筛选 → 回测"""
    query = args.get("query", "")
    if not query:
        return "请描述选股策略"

    # 加载配置
    import os, sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from src.utils import load_config
    from src.agent.llm import get_client
    config = load_config()

    if not config.get("llm", {}).get("api_key"):
        return json.dumps({"type": "error", "message": "请配置 LLM API Key"}, ensure_ascii=False)

    try:
        from src.discover.translator import translate_nl_to_conditions
        from src.discover.compiler import validate_plan, compile_and_execute
    except ImportError:
        return json.dumps({"type": "error", "message": "发现引擎未安装"}, ensure_ascii=False)

    client = get_client(config)
    plan = translate_nl_to_conditions(client, config, query)
    ok, err_msg = validate_plan(plan)
    if not ok:
        return json.dumps({"type": "error", "message": err_msg}, ensure_ascii=False)

    result = compile_and_execute(conn, plan)
    df = result["df"]

    if df.empty:
        return json.dumps({
            "type": "table", "columns": [], "rows": [], "total": 0,
            "hint": f"条件: {json.dumps(plan['conditions'], ensure_ascii=False)}，无匹配结果",
        }, ensure_ascii=False)

    # 构建响应
    cols = ["ts_code", "name", "pe", "pb", "change_pct"]
    avail = [c for c in cols if c in df.columns]
    rows = df[avail].values.tolist()
    col_labels = {"ts_code": "代码", "name": "名称", "pe": "PE", "pb": "PB", "change_pct": "涨跌幅%"}

    response_parts = [f"筛选条件: {json.dumps(plan['conditions'], ensure_ascii=False)}"]
    response_parts.append(f"匹配 {len(df)} 只股票")

    bt = result.get("backtest_result")
    if bt:
        response_parts.append(f"\n**组合回测** ({plan.get('backtest',{}).get('start','')} ~ {plan.get('backtest',{}).get('end','')})")
        response_parts.append(bt.summary())

    return json.dumps({
        "type": "table",
        "columns": [col_labels.get(c, c) for c in avail],
        "rows": rows,
        "total": len(df),
        "hint": "\n".join(response_parts),
        "plan": plan,
    }, ensure_ascii=False, default=str)


def _get_monitor_signals(conn, args):
    """查看监控信号"""
    from src.db import get_pending_actions

    action = args.get("action", "all")
    actions = get_pending_actions(conn, action if action != "all" else None)

    if not actions:
        return json.dumps({"type": "text", "text": "当前无待处理信号"}, ensure_ascii=False)

    rows = [[a["ts_code"], a["name"], a["action"], a["reason"], a["trigger_date"]]
            for a in actions]
    return json.dumps({
        "type": "table",
        "columns": ["代码", "名称", "建议", "原因", "触发日期"],
        "rows": rows,
        "total": len(actions),
        "action_ids": [a["id"] for a in actions],
    }, ensure_ascii=False, default=str)


def _confirm_monitor_action(conn, args):
    """确认/驳回监控建议"""
    aid = args.get("action_id")
    decision = args.get("decision", "dismissed")
    from src.db import confirm_action

    confirm_action(conn, aid, decision)
    return f"已{'确认' if decision == 'confirmed' else '驳回'}建议 #{aid}"
