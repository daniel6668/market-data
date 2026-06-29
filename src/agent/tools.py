"""Agent 工具函数 — 返回 JSON 结构化数据"""
import json
import duckdb
import pandas as pd
from datetime import datetime

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_stocks",
            "description": "多条件筛选股票，返回表格数据",
            "parameters": {
                "type": "object",
                "properties": {
                    "pe_max": {"type": "number", "description": "PE 上限"},
                    "pb_max": {"type": "number", "description": "PB 上限"},
                    "rsi_max": {"type": "number", "description": "RSI(14) 上限"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_backtest",
            "description": "回测均线交叉策略 (MA20/MA60)",
            "parameters": {
                "type": "object",
                "properties": {
                    "code": {"type": "string", "description": "股票代码"},
                    "start": {"type": "string", "description": "开始 YYYY-MM-DD"},
                    "end": {"type": "string", "description": "结束 YYYY-MM-DD"},
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
            "parameters": {
                "type": "object",
                "properties": {
                    "code": {"type": "string"},
                },
                "required": ["code"],
            },
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
                    "codes": {"type": "array", "items": {"type": "string"}, "description": "股票代码列表"},
                    "condition": {"type": "string", "description": "筛选条件描述"},
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
                "properties": {
                    "codes": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["codes"],
            },
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
    elif name == "add_to_watchlist":
        return _add_to_watchlist(conn, args)
    elif name == "get_watchlist":
        return _get_watchlist(conn, args)
    elif name == "remove_from_watchlist":
        return _remove_from_watchlist(conn, args)
    return "未知工具"


def _search_stocks(conn, args):
    from src.screening.screener import StockScreener
    screener = StockScreener(conn)
    conditions = []
    cond_desc = []
    if "pe_max" in args:
        conditions.append({"factor": "pe_ttm", "op": "lt", "value": args["pe_max"]})
        cond_desc.append(f"PE<{args['pe_max']}")
    if "pb_max" in args:
        conditions.append({"factor": "pb", "op": "lt", "value": args["pb_max"]})
        cond_desc.append(f"PB<{args['pb_max']}")
    if "rsi_max" in args:
        conditions.append({"factor": "rsi14", "op": "lt", "value": args["rsi_max"]})
        cond_desc.append(f"RSI<{args['rsi_max']}")
    if not conditions:
        return json.dumps({"type": "error", "message": "请指定筛选条件"})

    df = screener.search_with_indicators(conditions)
    if df.empty:
        basic_count = conn.execute("SELECT COUNT(*) FROM a_daily_basic").fetchone()[0]
        return json.dumps({"type": "table", "columns": [], "rows": [], "total": 0,
                "hint": f"无匹配。a_daily_basic:{basic_count}行"}, ensure_ascii=False)

    # 确保列存在
    cols = ["ts_code", "name", "pe", "pb", "change_pct", "main_net_5d"]
    avail = [c for c in cols if c in df.columns]
    rows = df[avail].values.tolist()
    col_labels = {"ts_code": "代码", "name": "名称", "pe": "PE", "pb": "PB",
                  "change_pct": "涨跌幅%", "main_net_5d": "主力净流入5日"}
    return json.dumps({
        "type": "table",
        "columns": [col_labels.get(c, c) for c in avail],
        "rows": rows,
        "total": len(df),
        "condition": ",".join(cond_desc),
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
    return (f"**{code} 均线交叉回测**\n"
            f"{result.summary()}\n"
            f"同期买入持有: {result.benchmark_return:.2f}%")


def _analyze_stock(conn, args):
    code = args["code"]
    pattern = f"{code}%"
    info = conn.execute(
        "SELECT name, industry FROM stock_info WHERE ts_code LIKE ?", [pattern]
    ).fetchone()
    basic = conn.execute(
        "SELECT pe, pb, turnover_rate FROM a_daily_basic "
        "WHERE ts_code LIKE ? ORDER BY trade_date DESC LIMIT 1", [pattern]
    ).fetchone()
    factors = conn.execute(
        "SELECT ma20, ma60, rsi14, ret_20d, vol_ratio FROM stock_factors "
        "WHERE ts_code LIKE ? ORDER BY trade_date DESC LIMIT 1", [pattern]
    ).fetchone()
    lines = [f"**{code}**" + (f" — {info[0]}" if info else "")]
    if info and info[1]:
        lines.append(f"- 行业: {info[1]}")
    if basic:
        lines.append(f"- PE: {basic[0]:.1f}  PB: {basic[1]:.2f}  换手率: {basic[2]:.2f}%")
    if factors:
        lines.append(f"- MA20: {factors[0]:.2f}  MA60: {factors[1]:.2f}")
        lines.append(f"- RSI14: {factors[2]:.1f}  20日涨幅: {factors[3]:.1f}%  量比: {factors[4]:.2f}")
    return "\n".join(lines) if len(lines) > 1 else f"{code}: 数据不足"


def _market_overview(conn):
    n = conn.execute("SELECT COUNT(*) FROM stock_info WHERE market='A'").fetchone()[0]
    nb = conn.execute(
        "SELECT date, hgt_yi, sgt_yi FROM northbound_flow ORDER BY date DESC LIMIT 1"
    ).fetchone()
    lines = [f"- A 股上市家数: {n}"]
    if nb:
        lines.append(f"- 北向资金({nb[0]}): 沪股通 {nb[1]:.1f}亿  深股通 {nb[2]:.1f}亿")
    return "\n".join(lines)


def _add_to_watchlist(conn, args):
    codes = args.get("codes", [])
    condition = args.get("condition", "")
    if not codes:
        return "未指定股票代码"
    added = 0
    for code in codes:
        name_row = conn.execute(
            "SELECT name FROM stock_info WHERE ts_code LIKE ?", [f"{code}%"]
        ).fetchone()
        name = name_row[0] if name_row else code
        conn.execute(
            "INSERT OR REPLACE INTO watchlist (ts_code, name, source_condition) VALUES (?,?,?)",
            [code, name, condition])
        added += 1
    return f"✅ 已添加 {added} 只到自选池（条件: {condition}）"


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
    data = [[r[0], r[1], round(r[2], 2) if r[2] else 0, round(r[3], 2) if r[3] else 0,
             str(r[4])[:10] if r[4] else "", r[5] or ""] for r in rows]
    return json.dumps({
        "type": "table",
        "columns": ["代码", "名称", "PE", "PB", "加入日期", "筛选条件"],
        "rows": data, "total": len(data),
    }, ensure_ascii=False)


def _remove_from_watchlist(conn, args):
    codes = args.get("codes", [])
    if not codes:
        return "未指定股票"
    for code in codes:
        conn.execute("DELETE FROM watchlist WHERE ts_code=?", [code])
    return f"✅ 已移除 {len(codes)} 只"
