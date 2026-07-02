"""维护引擎 — 形态破坏检测 + 移除建议"""
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class MaintainEngine:
    """关注列表维护引擎"""

    def __init__(self, conn, config: dict):
        self.conn = conn
        self.strategies = config.get("strategies", {})

    def run(self, target_date: str = None) -> list[dict]:
        """执行形态破坏检查，返回 remove 建议列表

        Returns: [{"ts_code": str, "name": str, "market": str, "reason": str, "consecutive_days": int}]
        """
        if target_date is None:
            target_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"=== 维护引擎启动: {target_date} ===")
        suggestions = []

        for market, strategy in self.strategies.items():
            remove_conds = strategy.get("remove_conditions", [])
            if not remove_conds:
                continue

            # 获取该市场活跃的关注股票
            rows = self.conn.execute(
                "SELECT ts_code, name FROM watchlist WHERE status='active' AND market=?",
                [market]
            ).fetchall()

            for ts_code, name in rows:
                for cond in remove_conds:
                    result = self._check_condition(ts_code, name, market, cond, target_date)
                    if result:
                        suggestions.append(result)
                        break  # 一个条件满足就够

        # 写入 suggested_actions
        from src.db import upsert_suggested_action
        for s in suggestions:
            upsert_suggested_action(
                self.conn, s["ts_code"], s["name"], s["market"],
                "REMOVE", s["reason"], target_date,
                {"consecutive_days": s.get("consecutive_days", 0)}
            )

        logger.info(f"=== 维护引擎完成: {len(suggestions)} 条移除建议 ===")
        return suggestions

    def _check_condition(self, ts_code: str, name: str, market: str,
                         cond: dict, target_date: str) -> dict | None:
        """检查单只股票是否满足某 remove 条件"""
        cond_type = cond.get("type")
        consecutive = cond.get("consecutive_days", 3)

        if cond_type == "price_below_ma":
            ma = cond.get("ma", "ma60")
            return self._check_price_below_ma(ts_code, name, market, ma,
                                              consecutive, target_date)
        elif cond_type == "macd_dead_cross":
            return self._check_macd_dead_hold(ts_code, name, market,
                                              consecutive, target_date)
        return None

    def _check_price_below_ma(self, ts_code: str, name: str, market: str,
                               ma: str, consecutive: int,
                               target_date: str) -> dict | None:
        """检查是否连续 N 日低于均线"""
        ma_col = "ma60" if ma == "ma60" else "ma20"
        daily_table = {"A": "a_daily", "ETF": "etf_daily",
                       "HK": "hk_daily", "US": "us_daily"}.get(market, "a_daily")

        # 查最近 N 天的收盘价 vs 均线
        if market == "A":
            # 用 stock_factors 的均线值
            rows = self.conn.execute(
                "SELECT a.close, f.{} FROM {} a "
                "JOIN stock_factors f ON a.ts_code=f.ts_code AND a.trade_date=f.trade_date "
                "WHERE a.ts_code=? AND a.trade_date<=? "
                "ORDER BY a.trade_date DESC LIMIT ?".format(ma_col, daily_table),
                [ts_code, target_date, consecutive]
            ).fetchall()
        else:
            # 非A股自己算均线（简化：取最近N+60天数据）
            window = 60 if ma == "ma60" else 20
            rows_raw = self.conn.execute(
                f"SELECT close FROM {daily_table} WHERE ts_code=? AND trade_date<=? "
                "ORDER BY trade_date DESC LIMIT ?",
                [ts_code, target_date, consecutive + window]
            ).fetchall()
            if len(rows_raw) < window:
                return None
            # 计算最近 consecutive 天的均线
            import pandas as pd
            s = pd.Series([r[0] for r in reversed(rows_raw)])
            ma_vals = s.rolling(window).mean().iloc[-consecutive:]
            close_vals = s.iloc[-consecutive:]
            rows = list(zip(close_vals, ma_vals))

        if len(rows) < consecutive:
            return None

        all_below = all(
            close is not None and ma_val is not None and close < ma_val
            for close, ma_val in rows
        )
        if all_below:
            return {
                "ts_code": ts_code, "name": name, "market": market,
                "reason": f"连续 {consecutive} 日低于 {ma.upper()}",
                "consecutive_days": consecutive,
            }
        return None

    def _check_macd_dead_hold(self, ts_code: str, name: str, market: str,
                               consecutive: int, target_date: str) -> dict | None:
        """检查 MACD 死叉是否持续 N 日"""
        rows = self.conn.execute(
            "SELECT macd_dif, macd_dea FROM stock_factors "
            "WHERE ts_code=? AND trade_date<=? "
            "ORDER BY trade_date DESC LIMIT ?",
            [ts_code, target_date, consecutive]
        ).fetchall()
        if len(rows) < consecutive:
            return None
        all_dead = all(
            dif is not None and dea is not None and dif < dea
            for dif, dea in rows
        )
        if all_dead:
            return {
                "ts_code": ts_code, "name": name, "market": market,
                "reason": f"MACD 死叉持续 {consecutive} 日",
                "consecutive_days": consecutive,
            }
        return None
