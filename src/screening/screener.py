"""选股筛选器 — 多条件组合筛选"""
import logging
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

FACTOR_SOURCES = {
    "ma5": ("stock_factors", "ma5"),
    "ma20": ("stock_factors", "ma20"),
    "ma60": ("stock_factors", "ma60"),
    "ret_5d": ("stock_factors", "ret_5d"),
    "ret_20d": ("stock_factors", "ret_20d"),
    "rsi14": ("stock_factors", "rsi14"),
    "vol_ratio": ("stock_factors", "vol_ratio"),
    "pe_ttm": ("a_daily_basic", "pe"),
    "pb": ("a_daily_basic", "pb"),
    "turnover_rate": ("a_daily_basic", "turnover_rate"),
}

OP_MAP = {
    "gt": ">", "lt": "<", "gte": ">=",
    "lte": "<=", "eq": "=", "ne": "!=",
}


class StockScreener:
    """多条件选股筛选器"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def by_conditions(self, conditions: list[dict],
                       trade_date: str = None) -> pd.DataFrame:
        """多条件 AND 筛选"""
        if trade_date is None:
            trade_date = self._latest_date()

        from_clauses = ["stock_info si"]
        where_clauses = ["si.market = 'A'"]
        joined = {}

        for i, cond in enumerate(conditions):
            factor = cond["factor"]
            op = OP_MAP.get(cond["op"], cond["op"])
            value = cond["value"]

            if factor not in FACTOR_SOURCES:
                logger.warning(f"未知因子: {factor}")
                continue

            table, col = FACTOR_SOURCES[factor]
            alias = f"t{i}"
            if table not in joined:
                from_clauses.append(
                    f"LEFT JOIN {table} {alias} "
                    f"ON si.ts_code = {alias}.ts_code "
                    f"AND {alias}.trade_date = '{trade_date}'"
                )
                joined[table] = alias
            else:
                alias = joined[table]
            where_clauses.append(f"{alias}.{col} {op} {value}")

        query = f"""
            SELECT si.ts_code, si.name
            FROM {' '.join(from_clauses)}
            WHERE {' AND '.join(where_clauses)}
            LIMIT 100
        """
        try:
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            logger.error(f"筛选失败: {e}")
            return pd.DataFrame()

    def by_template(self, template: str,
                     trade_date: str = None) -> pd.DataFrame:
        """预设模板筛选"""
        templates = {
            "value_low_pe": [
                {"factor": "pe_ttm", "op": "lt", "value": 20},
                {"factor": "pb", "op": "lt", "value": 2},
            ],
            "momentum_strong": [
                {"factor": "ret_20d", "op": "gt", "value": 10},
            ],
            "oversold_bounce": [
                {"factor": "rsi14", "op": "lt", "value": 35},
                {"factor": "vol_ratio", "op": "gt", "value": 1.5},
            ],
        }
        if template not in templates:
            logger.warning(f"未知模板: {template}")
            return pd.DataFrame()
        return self.by_conditions(templates[template], trade_date)

    def _latest_date(self) -> str:
        row = self.conn.execute(
            "SELECT MAX(trade_date) FROM a_daily"
        ).fetchone()
        return str(row[0]) if row[0] else "2026-01-01"
