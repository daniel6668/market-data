"""条件编译 + 执行引擎"""
import logging
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)

VALID_OPS = {"gt", "lt", "gte", "lte", "eq", "cross_above", "cross_below"}


def validate_plan(plan: dict) -> tuple[bool, str]:
    """验证翻译结果的有效性。返回 (is_valid, error_message)"""
    if "error" in plan:
        return False, plan.get("hint", plan["error"])
    conditions = plan.get("conditions", [])
    if not conditions:
        return False, "未指定任何筛选条件"
    for c in conditions:
        if "factor" not in c or "op" not in c:
            return False, f"条件格式错误: {c}"
        if c["op"] not in VALID_OPS:
            return False, f"不支持的操作符: {c['op']}，支持: {', '.join(VALID_OPS)}"
    universe = plan.get("universe", "A")
    if universe not in ("A", "ETF", "HK", "US", "all"):
        return False, f"不支持的市场: {universe}"
    return True, ""


def compile_and_execute(conn, plan: dict,
                        progress_callback=None) -> dict:
    """编译条件并执行筛选+（可选）回测

    Returns: {df: DataFrame, result_count: int, backtest_result: PortfolioResult|None, plan: dict}
    """
    from src.screening.screener import StockScreener

    screener = StockScreener(conn)
    conditions = plan.get("conditions", [])
    universe = plan.get("universe", "A")

    target_markets = [universe] if universe != "all" else ["A", "ETF", "HK", "US"]

    all_dfs = []
    for m in target_markets:
        if progress_callback:
            progress_callback(0, len(target_markets),
                            f"筛选 {m} 市场...")
        try:
            df = screener.search_full_market(conditions, market=m)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            logger.error(f"筛选 {m} 失败: {e}")

    result_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

    # 回测（如果需要）
    backtest_result = None
    if plan.get("action") == "backtest" and not result_df.empty:
        if progress_callback:
            progress_callback(1, 2, "运行组合回测...")
        backtest_result = _run_discover_backtest(conn, result_df, plan)

    if progress_callback:
        progress_callback(1, 1,
                        f"完成: {len(result_df)} 只匹配")

    return {
        "df": result_df,
        "result_count": len(result_df),
        "backtest_result": backtest_result,
        "plan": plan,
    }


def _run_discover_backtest(conn, df: pd.DataFrame, plan: dict):
    """基于筛选结果运行组合回测"""
    from src.backtest.runner import BacktestRunner
    from src.backtest.report import PortfolioResult

    codes = df["ts_code"].tolist()[:50]  # 最多50只股票
    if len(codes) < 1:
        return None

    bt_cfg = plan.get("backtest", {})
    start = bt_cfg.get("start", "2025-01-01")
    end = bt_cfg.get("end", datetime.now().strftime("%Y-%m-%d"))
    rebalance = bt_cfg.get("rebalance", "monthly")
    weights = bt_cfg.get("weights", "equal")

    runner = BacktestRunner(conn)

    # 构建条件：对每只股票用同样的策略（MA20 > MA60 买入, MA20 < MA60 卖出）
    buy_cond = {}
    sell_cond = {}
    for code in codes:
        prices = runner._load_prices(code, start, end)
        if prices.empty:
            continue
        ma20 = prices.rolling(20).mean()
        ma60 = prices.rolling(60).mean()
        buy_cond[code] = ma20 > ma60
        sell_cond[code] = ma20 < ma60

    if not buy_cond:
        return None

    try:
        return runner.run_portfolio(
            list(buy_cond.keys()),
            buy_cond, sell_cond,
            start, end, weights=weights, rebalance=rebalance
        )
    except Exception as e:
        logger.error(f"回测失败: {e}")
        return None
