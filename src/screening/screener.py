"""选股筛选器 — 多条件组合筛选

使用窗口函数取每只股票最新数据行，解决不同源日期不对齐问题。
"""
import logging
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

FACTOR_SOURCES = {
    "pe_ttm":        ("a_daily_basic", "pe"),
    "pb":            ("a_daily_basic", "pb"),
    "turnover_rate": ("a_daily_basic", "turnover_rate"),
    "ma5":           ("stock_factors", "ma5"),
    "ma20":          ("stock_factors", "ma20"),
    "ma60":          ("stock_factors", "ma60"),
    "ret_5d":        ("stock_factors", "ret_5d"),
    "ret_20d":       ("stock_factors", "ret_20d"),
    "rsi14":         ("stock_factors", "rsi14"),
    "vol_ratio":     ("stock_factors", "vol_ratio"),
}

# 每个数据表的列清单（用于 ROW_NUMBER 子查询）
TABLE_COLUMNS = {
    "a_daily_basic": ["pe", "pb", "turnover_rate"],
    "stock_factors": ["ma5", "ma20", "ma60", "ret_5d", "ret_20d",
                       "rsi14", "vol_ratio"],
}

OP_MAP = {
    "gt": ">", "lt": "<", "gte": ">=",
    "lte": "<=", "eq": "=", "ne": "!=",
}


class StockScreener:
    """多条件选股筛选器 — 取每只股票各自最新数据"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def by_conditions(self, conditions: list[dict],
                       trade_date: str = None) -> pd.DataFrame:
        """多条件 AND 筛选"""
        if not conditions:
            return pd.DataFrame()

        # 1. 收集涉及的表（排好序保证确定性）
        tables_needed = sorted(set(
            FACTOR_SOURCES[c["factor"]][0]
            for c in conditions
            if c["factor"] in FACTOR_SOURCES
        ))
        if not tables_needed:
            return pd.DataFrame()

        # 2. 构建 JOIN 子句和 WHERE 条件（同一遍循环保证 alias 一致）
        joins = []
        where = ["si.market = 'A'"]
        table_alias = {}

        for i, table in enumerate(tables_needed):
            alias = f"t{i}"
            table_alias[table] = alias
            cols = TABLE_COLUMNS[table]
            cols_str = ", ".join(cols)
            joins.append(f"""
                LEFT JOIN (
                    SELECT ts_code, {cols_str}
                    FROM (
                        SELECT ts_code, {cols_str},
                            ROW_NUMBER() OVER (
                                PARTITION BY ts_code ORDER BY trade_date DESC
                            ) rn
                        FROM {table}
                    ) sub WHERE rn = 1
                ) {alias} ON si.ts_code = {alias}.ts_code
            """)

        for cond in conditions:
            factor = cond["factor"]
            if factor not in FACTOR_SOURCES:
                continue
            table, col = FACTOR_SOURCES[factor]
            op = OP_MAP.get(cond["op"], cond["op"])
            alias = table_alias[table]
            where.append(f"{alias}.{col} {op} {cond['value']}")

        query = f"""
            SELECT si.ts_code, si.name
            FROM stock_info si
            {"".join(joins)}
            WHERE {" AND ".join(where)}
            ORDER BY si.ts_code
            LIMIT 100
        """
        try:
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            logger.error(f"筛选失败: {e}\nSQL: {query[:300]}")
            return pd.DataFrame()

    def by_template(self, template: str,
                     trade_date: str = None) -> pd.DataFrame:
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
            return pd.DataFrame()
        return self.by_conditions(templates[template], trade_date)

    def search_with_indicators(self, conditions: list[dict]) -> pd.DataFrame:
        """多条件筛选 + 返回指标列（PE/PB/涨跌幅/主力资金）"""
        if not conditions:
            return pd.DataFrame()

        tables_needed = sorted(set(
            FACTOR_SOURCES[c["factor"]][0]
            for c in conditions
            if c["factor"] in FACTOR_SOURCES
        ))
        if not tables_needed:
            return pd.DataFrame()

        # 始终 JOIN a_daily_basic 和 stock_factors 获取指标
        if "a_daily_basic" not in tables_needed:
            tables_needed.append("a_daily_basic")
        if "stock_factors" not in tables_needed:
            tables_needed.append("stock_factors")
        tables_needed = sorted(set(tables_needed))

        joins = []
        where = ["si.market = 'A'"]
        table_alias = {}

        for i, table in enumerate(tables_needed):
            alias = f"t{i}"
            table_alias[table] = alias
            cols = TABLE_COLUMNS.get(table, ["*"])
            cols_str = ", ".join(cols)
            joins.append(f"""
                LEFT JOIN (
                    SELECT ts_code, {cols_str}
                    FROM (
                        SELECT ts_code, {cols_str},
                            ROW_NUMBER() OVER (
                                PARTITION BY ts_code ORDER BY trade_date DESC
                            ) rn
                        FROM {table}
                    ) sub WHERE rn = 1
                ) {alias} ON si.ts_code = {alias}.ts_code
            """)

        for cond in conditions:
            factor = cond["factor"]
            if factor not in FACTOR_SOURCES:
                continue
            table, col = FACTOR_SOURCES[factor]
            op = OP_MAP.get(cond["op"], cond["op"])
            alias = table_alias[table]
            where.append(f"COALESCE({alias}.{col},0) {op} {cond['value']}")

        ab = table_alias.get("a_daily_basic", "t0")
        sf = table_alias.get("stock_factors", "t1")

        query = f"""
            SELECT si.ts_code, si.name,
                   COALESCE({ab}.pe,0) pe,
                   COALESCE({ab}.pb,0) pb,
                   COALESCE({sf}.ret_5d,0) change_pct
            FROM stock_info si
            {"".join(joins)}
            WHERE {" AND ".join(where)}
            ORDER BY {ab}.pe ASC NULLS LAST
            LIMIT 200
        """
        try:
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            logger.error(f"search_with_indicators 失败: {e}")
            return pd.DataFrame()
