"""选股筛选器 — 多条件组合筛选

使用每只股票最新的数据行（不限定同一天），解决不同数据源更新频率不一致的问题。
"""
import logging
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# 因子 → (表名, 列名, 是否是日期无关快照)
FACTOR_SOURCES = {
    "pe_ttm":        ("a_daily_basic",    "pe"),
    "pb":            ("a_daily_basic",    "pb"),
    "turnover_rate": ("a_daily_basic",    "turnover_rate"),
    "ma5":           ("stock_factors",    "ma5"),
    "ma20":          ("stock_factors",    "ma20"),
    "ma60":          ("stock_factors",    "ma60"),
    "ret_5d":        ("stock_factors",    "ret_5d"),
    "ret_20d":       ("stock_factors",    "ret_20d"),
    "rsi14":         ("stock_factors",    "rsi14"),
    "vol_ratio":     ("stock_factors",    "vol_ratio"),
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
        """多条件 AND 筛选

        对每个因子表取每只股票的最新一行数据，不要求同一天。
        """
        # 收集涉及的表
        tables_needed = set()
        for cond in conditions:
            factor = cond["factor"]
            if factor in FACTOR_SOURCES:
                tables_needed.add(FACTOR_SOURCES[factor][0])
            else:
                logger.warning(f"未知因子: {factor}")

        if not tables_needed:
            return pd.DataFrame()

        # 对每个表构建"最新一行"子查询
        latest_subs = {}
        table_index = 0
        for table in tables_needed:
            alias = f"t{table_index}"
            table_index += 1
            if table == "a_daily_basic":
                latest_subs[alias] = f"""
                    LEFT JOIN (
                        SELECT ts_code, pe, pb, turnover_rate
                        FROM (
                            SELECT *, ROW_NUMBER() OVER (
                                PARTITION BY ts_code ORDER BY trade_date DESC
                            ) rn
                            FROM a_daily_basic
                        ) WHERE rn = 1
                    ) {alias} ON si.ts_code = {alias}.ts_code
                """
            elif table == "stock_factors":
                latest_subs[alias] = f"""
                    LEFT JOIN (
                        SELECT ts_code, ma5, ma20, ma60,
                               ret_5d, ret_20d, rsi14, vol_ratio
                        FROM (
                            SELECT *, ROW_NUMBER() OVER (
                                PARTITION BY ts_code ORDER BY trade_date DESC
                            ) rn
                            FROM stock_factors
                        ) WHERE rn = 1
                    ) {alias} ON si.ts_code = {alias}.ts_code
                """

        # 构建查询
        where_clauses = ["si.market = 'A'"]
        alias_for_table = {}  # table → alias used in JOIN
        for alias, _ in latest_subs.items():
            pass  # we need table→alias mapping

        # 重新构建：记录每个 table 对应的 alias
        table_alias = {}
        idx = 0
        for table in tables_needed:
            table_alias[table] = f"t{idx}"
            idx += 1

        for cond in conditions:
            factor = cond["factor"]
            op = OP_MAP.get(cond["op"], cond["op"])
            value = cond["value"]
            if factor not in FACTOR_SOURCES:
                continue
            table, col = FACTOR_SOURCES[factor]
            alias = table_alias[table]
            where_clauses.append(f"{alias}.{col} {op} {value}")

        from_clause = "stock_info si\n" + "\n".join(latest_subs.values())
        query = f"""
            SELECT si.ts_code, si.name
            FROM {from_clause}
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
