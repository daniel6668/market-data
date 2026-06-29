"""Agent 工具函数 — LLM Function Calling 定义 + 执行"""
import json
import duckdb
import pandas as pd
from datetime import datetime

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_stocks",
            "description": "多条件筛选股票。可指定 PE 上限、RSI 上限等",
            "parameters": {
                "type": "object",
                "properties": {
                    "pe_max": {"type": "number", "description": "PE(TTM) 上限"},
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
                    "code": {"type": "string", "description": "股票代码，如 600519"},
                    "start": {"type": "string", "description": "开始日期 YYYY-MM-DD"},
                    "end": {"type": "string", "description": "结束日期 YYYY-MM-DD"},
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
                    "code": {"type": "string", "description": "股票代码，如 600519"},
                },
                "required": ["code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "market_overview",
            "description": "查看今日市场概况（A股数量、北向资金等）",
            "parameters": {"type": "object", "properties": {}},
        },
    },
]


def execute_tool(name: str, args: dict, conn: duckdb.DuckDBPyConnection) -> str:
    """执行工具调用并返回结果字符串"""
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
        return "请指定至少一个筛选条件（如 pe_max=30）"
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
        lines.append(f"- PE: {basic[0]:.1f}  |  PB: {basic[1]:.2f}  |  换手率: {basic[2]:.2f}%")
    if factors:
        lines.append(f"- MA20: {factors[0]:.2f}  |  MA60: {factors[1]:.2f}")
        lines.append(f"- RSI14: {factors[2]:.1f}  |  20日涨幅: {factors[3]:.1f}%  |  量比: {factors[4]:.2f}")
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
