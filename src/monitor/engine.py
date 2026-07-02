"""监控引擎 — 收盘后运行：收益跟踪 + 卖出信号扫描 + 新机会扫描"""
import logging
import pandas as pd
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class MonitorEngine:
    """关注列表监控引擎"""

    def __init__(self, conn, config: dict):
        self.conn = conn
        self.config = config
        self.strategies = config.get("strategies", {})

    def run(self, target_date: str = None) -> dict:
        """执行完整的监控流程

        Returns: {watchlist_returns: int, sell_signals: int, new_buy_signals: int}
        """
        if target_date is None:
            target_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"=== 监控引擎启动: {target_date} ===")

        # 1. 收益刷新
        watchlist_returns = self._refresh_performance(target_date)
        logger.info(f"  收益刷新: {watchlist_returns} 只")

        # 2. 卖出信号扫描
        sell_signals = self._scan_sell_signals(target_date)
        logger.info(f"  卖出信号: {sell_signals} 条")

        # 3. 新机会扫描（可选）
        new_buy_signals = self._scan_new_opportunities(target_date)
        logger.info(f"  新机会: {new_buy_signals} 条")

        logger.info(f"=== 监控引擎完成 ===")
        return {
            "watchlist_returns": watchlist_returns,
            "sell_signals": sell_signals,
            "new_buy_signals": new_buy_signals,
        }

    def _refresh_performance(self, target_date: str) -> int:
        """刷新关注列表所有活跃股票的收益指标"""
        rows = self.conn.execute(
            "SELECT ts_code, market, entry_price, entry_date FROM watchlist WHERE status='active'"
        ).fetchall()
        if not rows:
            return 0

        count = 0
        for ts_code, market, entry_price, entry_date in rows:
            # 获取当前价格
            daily_table = self._daily_table(market)
            price_row = self.conn.execute(
                f"SELECT close FROM {daily_table} WHERE ts_code=? AND trade_date<=? "
                "ORDER BY trade_date DESC LIMIT 1",
                [ts_code, target_date]
            ).fetchone()
            if not price_row:
                continue
            current_price = price_row[0]

            # 计算收益
            entry_px = entry_price if entry_price else current_price
            cumulative = (current_price / entry_px - 1) * 100 if entry_px else 0

            # 近期收益
            ret5 = self._calc_ret(self.conn, ts_code, daily_table, target_date, 5)
            ret10 = self._calc_ret(self.conn, ts_code, daily_table, target_date, 10)
            ret20 = self._calc_ret(self.conn, ts_code, daily_table, target_date, 20)

            # 均线状态（仅 A 股有 stock_factors）
            below_ma20, below_ma60 = False, False
            macd_cross, rsi_val = None, None
            if market == "A":
                f_row = self.conn.execute(
                    "SELECT ma20, ma60, macd_dif, macd_dea, rsi14 FROM stock_factors "
                    "WHERE ts_code=? AND trade_date<=? ORDER BY trade_date DESC LIMIT 1",
                    [ts_code, target_date]
                ).fetchone()
                if f_row:
                    below_ma20 = current_price < f_row[0] if f_row[0] else False
                    below_ma60 = current_price < f_row[1] if f_row[1] else False
                    if f_row[2] and f_row[3]:
                        macd_cross = "golden" if f_row[2] > f_row[3] else "dead"
                    rsi_val = f_row[4] if f_row[4] else None

            self.conn.execute("""
                INSERT OR REPLACE INTO watchlist_performance
                (ts_code, calc_date, entry_price, entry_date, current_price,
                 cumulative_return, ret_5d, ret_10d, ret_20d,
                 below_ma20, below_ma60, macd_cross, rsi)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [ts_code, target_date, entry_price, entry_date, current_price,
                  round(cumulative, 2), round(ret5, 2) if ret5 else 0,
                  round(ret10, 2) if ret10 else 0, round(ret20, 2) if ret20 else 0,
                  below_ma20, below_ma60, macd_cross, rsi_val])
            count += 1

        return count

    def _scan_sell_signals(self, target_date: str) -> int:
        """扫描卖出信号：按市场规则检查止损/止盈/均线/形态"""
        from src.db import upsert_suggested_action

        rows = self.conn.execute(
            "SELECT w.ts_code, w.name, w.market, w.entry_price, "
            "p.current_price, p.cumulative_return, p.below_ma20, p.below_ma60, "
            "p.macd_cross, p.rsi FROM watchlist w "
            "JOIN watchlist_performance p ON w.ts_code=p.ts_code AND p.calc_date=? "
            "WHERE w.status='active'",
            [target_date]
        ).fetchall()
        if not rows:
            return 0

        count = 0
        for (ts_code, name, market, entry_px, cur_px, cum_ret,
             below_ma20, below_ma60, macd_cross, rsi) in rows:
            strategy = self.strategies.get(market, {})
            sell_rules = strategy.get("sell_rules", {})
            if not sell_rules:
                continue

            reasons = []
            action = "HOLD"

            # 止损
            stop_loss = sell_rules.get("stop_loss")
            if stop_loss and cum_ret and cum_ret <= float(stop_loss):
                reasons.append(f"触发止损: 累计收益 {cum_ret:.1f}% ≤ {stop_loss}%")
                action = "SELL"

            # 止盈
            stop_profit = sell_rules.get("stop_profit")
            if stop_profit and cum_ret and cum_ret >= float(stop_profit) and action == "HOLD":
                reasons.append(f"触发止盈提醒: 累计收益 {cum_ret:.1f}% ≥ {stop_profit}%")
                action = "REDUCE"

            # 均线跌破
            ma_break = sell_rules.get("ma_break", [])
            if "ma60" in ma_break and below_ma60:
                reasons.append("跌破 MA60")
                action = "SELL"
            elif "ma20" in ma_break and below_ma20 and action != "SELL":
                reasons.append("跌破 MA20")
                action = "REDUCE"

            # 形态
            pattern = sell_rules.get("pattern", [])
            if "macd_dead_cross" in pattern and macd_cross == "dead":
                reasons.append("MACD 死叉")
                if action == "HOLD":
                    action = "REDUCE"

            if action != "HOLD" and reasons:
                upsert_suggested_action(
                    self.conn, ts_code, name, market,
                    action, "; ".join(reasons), target_date,
                    {"cumulative_return": cum_ret, "current_price": cur_px,
                     "entry_price": entry_px, "below_ma20": below_ma20,
                     "below_ma60": below_ma60, "macd_cross": macd_cross}
                )
                count += 1

        return count

    def _scan_new_opportunities(self, target_date: str) -> int:
        """新机会扫描：用活跃策略条件全市场重新筛选"""
        from src.db import get_active_rules, upsert_suggested_action
        from src.screening.screener import StockScreener

        screener = StockScreener(self.conn)
        count = 0

        for market in ["A", "ETF", "HK", "US"]:
            rules = get_active_rules(self.conn, market, "screen")
            if not rules:
                continue

            for rule in rules:
                try:
                    df = screener.search_full_market(rule["conditions"], market=market)
                    if df.empty:
                        continue
                    # 检查是否已在关注列表
                    existing = {r[0] for r in self.conn.execute(
                        "SELECT ts_code FROM watchlist WHERE status='active'").fetchall()}
                    for _, row in df.iterrows():
                        if row["ts_code"] not in existing:
                            upsert_suggested_action(
                                self.conn, row["ts_code"], row.get("name", ""),
                                market, "BUY",
                                f'匹配策略: {rule["name"]}', target_date,
                                {"pe": float(row.get("pe", 0)), "pb": float(row.get("pb", 0))}
                            )
                            count += 1
                except Exception as e:
                    logger.error(f"新机会扫描失败 {market}/{rule['name']}: {e}")

        return count

    def _daily_table(self, market: str) -> str:
        return {"A": "a_daily", "ETF": "etf_daily",
                "HK": "hk_daily", "US": "us_daily"}.get(market, "a_daily")

    def _calc_ret(self, conn, ts_code, table, target_date, days) -> float:
        try:
            row = conn.execute(
                f"SELECT close FROM {table} WHERE ts_code=? AND trade_date<=? "
                "ORDER BY trade_date DESC LIMIT ?",
                [ts_code, target_date, days + 1]
            ).fetchall()
            if len(row) >= days + 1:
                return (row[0][0] / row[-1][0] - 1) * 100
        except Exception:
            pass
        return 0.0
