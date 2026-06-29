"""因子引擎 — 批量计算 + DB 写入"""
import logging
import duckdb
import pandas as pd
from .registry import FACTOR_REGISTRY

logger = logging.getLogger(__name__)


class FactorEngine:
    """日级别因子计算引擎"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def compute_single(self, ts_code: str, start: str, end: str) -> pd.DataFrame:
        """计算单只股票的因子，返回 DataFrame（不写 DB）"""
        df = self.conn.execute("""
            SELECT trade_date, open, high, low, close, vol
            FROM a_daily
            WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date
        """, [ts_code, start, end]).fetchdf()
        if df.empty:
            return pd.DataFrame()
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        result = pd.DataFrame({"ts_code": ts_code, "trade_date": df["trade_date"]})

        for name, (func, kwargs, col) in FACTOR_REGISTRY.items():
            try:
                out = func(df, **kwargs)
                if col:
                    if hasattr(out, 'columns'):
                        result[name] = out[col].values
                    else:
                        result[name] = out
                else:
                    result[name] = out.values if hasattr(out, 'values') else out
            except Exception as e:
                logger.debug(f"因子 {name} 计算失败 for {ts_code}: {e}")
                result[name] = None

        # 附加估值因子（从 a_daily_basic）
        basic = self.conn.execute("""
            SELECT trade_date, pe, pb, turnover_rate
            FROM a_daily_basic
            WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
        """, [ts_code, start, end]).fetchdf()
        if not basic.empty:
            basic["trade_date"] = pd.to_datetime(basic["trade_date"])
            result = result.merge(basic, on="trade_date", how="left", suffixes=("", "_basic"))
            result["pe_ttm"] = result["pe"] if "pe" in result.columns else None
            result["pb"] = result["pb"] if "pb" in result.columns else None
            result["turnover_rate"] = result["turnover_rate"] if "turnover_rate" in result.columns else None

        return result

    def compute_all(self, trade_date: str) -> int:
        """计算全市场指定日期的因子，写入 stock_factors 表。返回成功数。"""
        codes = self.conn.execute(
            "SELECT ts_code FROM stock_info WHERE market = 'A'"
        ).fetchall()
        total = 0
        for (ts_code,) in codes:
            df = self.compute_single(
                ts_code,
                start="2024-01-01",
                end=trade_date,
            )
            if df.empty:
                continue
            day_df = df[df["trade_date"] == pd.to_datetime(trade_date)]
            if day_df.empty:
                continue
            self._upsert_factors(day_df)
            total += 1
        logger.info(f"因子计算完成: {total} 只")
        return total

    def _upsert_factors(self, df: pd.DataFrame):
        """写入因子表（upsert）"""
        df = df.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        self.conn.register("_tmp_factors", df)
        cols = ", ".join(df.columns)
        self.conn.execute(
            f"INSERT OR REPLACE INTO stock_factors ({cols}) "
            f"SELECT * FROM _tmp_factors"
        )
        self.conn.unregister("_tmp_factors")
