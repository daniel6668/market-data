"""选股筛选器 — 支持跨因子比较 (如 macd_dif > macd_dea)"""
import logging
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

FACTOR_SOURCES = {
    "pe_ttm":        ("a_daily_basic", "pe"),
    "pb":            ("a_daily_basic", "pb"),
    "turnover_rate": ("a_daily_basic", "turnover_rate"),
    "ma5":           ("stock_factors", "ma5"),
    "ma10":          ("stock_factors", "ma10"),
    "ma20":          ("stock_factors", "ma20"),
    "ma60":          ("stock_factors", "ma60"),
    "ret_5d":        ("stock_factors", "ret_5d"),
    "ret_20d":       ("stock_factors", "ret_20d"),
    "rsi6":          ("stock_factors", "rsi6"),
    "rsi14":         ("stock_factors", "rsi14"),
    "vol_ratio":     ("stock_factors", "vol_ratio"),
    "macd_dif":      ("stock_factors", "macd_dif"),
    "macd_dea":      ("stock_factors", "macd_dea"),
    "macd_bar":      ("stock_factors", "macd_bar"),
}

TABLE_COLUMNS = {
    "a_daily_basic": ["pe", "pb", "turnover_rate"],
    "stock_factors": ["ma5","ma10","ma20","ma60","ret_5d","ret_20d",
                       "rsi6","rsi14","vol_ratio","macd_dif","macd_dea","macd_bar"],
}

OP_MAP = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<=", "eq": "=", "ne": "!="}


class StockScreener:

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def by_conditions(self, conditions: list[dict], trade_date=None) -> pd.DataFrame:
        if not conditions: return pd.DataFrame()
        tables_needed = sorted(set(FACTOR_SOURCES[c["factor"]][0] for c in conditions if c["factor"] in FACTOR_SOURCES))
        if not tables_needed: return pd.DataFrame()
        joins, where, ta = self._build_query_parts(tables_needed, conditions)
        query = f"SELECT si.ts_code, si.name FROM stock_info si {''.join(joins)} WHERE {' AND '.join(where)} ORDER BY si.ts_code LIMIT 100"
        try: return self.conn.execute(query).fetchdf()
        except Exception as e: logger.error(f"筛选失败: {e}"); return pd.DataFrame()

    def search_with_indicators(self, conditions: list[dict]) -> pd.DataFrame:
        if not conditions: return pd.DataFrame()
        tables_needed = sorted(set(FACTOR_SOURCES[c["factor"]][0] for c in conditions if c["factor"] in FACTOR_SOURCES))
        if not tables_needed: return pd.DataFrame()
        if "a_daily_basic" not in tables_needed: tables_needed.append("a_daily_basic")
        tables_needed = sorted(set(tables_needed))
        joins, where, ta = self._build_query_parts(tables_needed, conditions)
        ab = ta.get("a_daily_basic", "t0")
        query = f"""
            SELECT si.ts_code, si.name,
                   COALESCE({ab}.pe,0) pe,
                   COALESCE({ab}.pb,0) pb,
                   ROUND(COALESCE(chg.ret_5d,0), 2) change_pct
            FROM stock_info si
            {''.join(joins)}
            LEFT JOIN (
                SELECT ts_code,
                    (close - LAG(close,5) OVER w) / NULLIF(LAG(close,5) OVER w,0) * 100 ret_5d
                FROM a_daily
                WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) = 1
            ) chg ON si.ts_code = chg.ts_code
            WHERE {' AND '.join(where)}
            ORDER BY {ab}.pe ASC NULLS LAST
            LIMIT 200
        """
        try: return self.conn.execute(query).fetchdf()
        except Exception as e: logger.error(f"search_with_indicators: {e}"); return pd.DataFrame()

    def _build_query_parts(self, tables_needed, conditions):
        joins, where, ta = [], ["si.market = 'A'"], {}
        for i, table in enumerate(tables_needed):
            alias = f"t{i}"; ta[table] = alias
            cols = ", ".join(TABLE_COLUMNS.get(table, ["*"]))
            joins.append(f"""
                LEFT JOIN (
                    SELECT ts_code, {cols} FROM (
                        SELECT ts_code, {cols}, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) rn
                        FROM {table}
                    ) sub WHERE rn=1
                ) {alias} ON si.ts_code = {alias}.ts_code
            """)
        for cond in conditions:
            factor = cond["factor"]
            if factor not in FACTOR_SOURCES: continue
            table, col = FACTOR_SOURCES[factor]
            op = OP_MAP.get(cond["op"], cond["op"])
            alias = ta[table]
            val = cond["value"]
            if isinstance(val, str) and val in FACTOR_SOURCES:
                t2, c2 = FACTOR_SOURCES[val]; a2 = ta[t2]
                where.append(f"COALESCE({alias}.{col},0) {op} COALESCE({a2}.{c2},0)")
            else:
                where.append(f"COALESCE({alias}.{col},0) {op} {val}")
            if factor in ("pe_ttm", "pb"): where.append(f"{alias}.{col} > 0")
        return joins, where, ta

    def by_template(self, template: str, trade_date=None) -> pd.DataFrame:
        t = {
            "value_low_pe": [{"factor":"pe_ttm","op":"lt","value":20},{"factor":"pb","op":"lt","value":2}],
            "momentum_strong": [{"factor":"ret_20d","op":"gt","value":10}],
            "oversold_bounce": [{"factor":"rsi14","op":"lt","value":35},{"factor":"vol_ratio","op":"gt","value":1.5}],
        }
        return self.by_conditions(t.get(template, []), trade_date)
