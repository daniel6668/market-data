"""选股筛选器"""
import logging, duckdb, pandas as pd, numpy as np

logger = logging.getLogger(__name__)

FACTOR_SOURCES = {
    "pe_ttm": ("a_daily_basic", "pe"), "pb": ("a_daily_basic", "pb"),
    "turnover_rate": ("a_daily_basic", "turnover_rate"),
    "ma5": ("stock_factors", "ma5"), "ma10": ("stock_factors", "ma10"),
    "ma20": ("stock_factors", "ma20"), "ma60": ("stock_factors", "ma60"),
    "ret_5d": ("stock_factors", "ret_5d"), "ret_20d": ("stock_factors", "ret_20d"),
    "rsi6": ("stock_factors", "rsi6"), "rsi14": ("stock_factors", "rsi14"),
    "vol_ratio": ("stock_factors", "vol_ratio"),
    "macd_dif": ("stock_factors", "macd_dif"), "macd_dea": ("stock_factors", "macd_dea"),
}

TABLE_COLUMNS = {
    "a_daily_basic": ["pe", "pb", "turnover_rate"],
    "stock_factors": ["ma5","ma10","ma20","ma60","ret_5d","ret_20d",
                       "rsi6","rsi14","vol_ratio","macd_dif","macd_dea"],
}
OP_MAP = {"gt":">","lt":"<","gte":">=","lte":"<="}


class StockScreener:

    def __init__(self, conn): self.conn = conn

    def search_with_indicators(self, conditions):
        """筛选 + 输出PE/PB/涨跌幅。自动检测MACD/MA金叉死叉"""
        if not conditions: return pd.DataFrame()

        # 分离交叉条件
        cross, normal = [], []
        for c in conditions:
            v = c.get("value")
            if isinstance(v, str) and v in FACTOR_SOURCES:
                cross.append(c)
            else:
                normal.append(c)

        # 用 Python 处理交叉条件（比 SQL 简单）
        if cross:
            return self._search_with_cross(cross, normal)

        # 纯 SQL 路径（无交叉条件）
        tables = sorted(set(FACTOR_SOURCES[c["factor"]][0]
            for c in normal if c["factor"] in FACTOR_SOURCES))
        if not tables: return pd.DataFrame()
        if "a_daily_basic" not in tables: tables.append("a_daily_basic")
        tables = sorted(set(tables))
        joins, where, ta = self._build_parts(tables, normal)
        ab = ta.get("a_daily_basic", "t0")
        q = f"""
            SELECT si.ts_code, si.name,
                   COALESCE({ab}.pe,0) pe, COALESCE({ab}.pb,0) pb,
                   ROUND(COALESCE(chg.ret_5d,0),2) change_pct
            FROM stock_info si {''.join(joins)}
            LEFT JOIN (SELECT ts_code,
                (close-LAG(close,5) OVER w)/NULLIF(LAG(close,5) OVER w,0)*100 ret_5d
                FROM a_daily WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC)=1
            ) chg ON si.ts_code=chg.ts_code
            WHERE {' AND '.join(where)}
            ORDER BY {ab}.pe ASC NULLS LAST LIMIT 200
        """
        try: return self.conn.execute(q).fetchdf()
        except Exception as e: logger.error(f"search: {e}"); return pd.DataFrame()

    def _search_with_cross(self, cross_conds, normal_conds):
        """Python 侧计算交叉 + 筛选"""
        from datetime import datetime, timedelta

        # 1. 从 a_daily 拉最近 60 个交易日数据
        codes = [r[0] for r in self.conn.execute(
            "SELECT DISTINCT ts_code FROM a_daily WHERE trade_date >= '2026-04-01'"
        ).fetchall()]
        if not codes: return pd.DataFrame()

        results = []
        for ts_code in codes:
            df = self.conn.execute("""
                SELECT trade_date, close FROM a_daily
                WHERE ts_code=? ORDER BY trade_date
            """, [ts_code]).fetchdf()
            if len(df) < 60: continue

            df['ma5'] = df['close'].rolling(5).mean()
            df['ma10'] = df['close'].rolling(10).mean()
            df['ma20'] = df['close'].rolling(20).mean()
            df['ma60'] = df['close'].rolling(60).mean()
            # MACD
            ema12 = df['close'].ewm(span=12, adjust=False).mean()
            ema26 = df['close'].ewm(span=26, adjust=False).mean()
            df['dif'] = ema12 - ema26
            df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()

            # 检查交叉条件（最近3天）
            ok = True
            for c in cross_conds:
                f1, f2 = c['factor'], c["value"]
                col1 = f1.replace("macd_dif","dif").replace("ma","ma")
                col2 = f2.replace("macd_dea","dea").replace("ma","ma")
                # 近3日内是否有过交叉
                last3 = df.tail(4)  # 需要4行来做LAG
                if len(last3) < 4: ok = False; break
                crossed = False
                for i in range(1, len(last3)):
                    prev1 = last3.iloc[i-1][col1]; prev2 = last3.iloc[i-1][col2]
                    cur1 = last3.iloc[i][col1]; cur2 = last3.iloc[i][col2]
                    if pd.notna(prev1) and pd.notna(cur1):
                        if prev1 < prev2 and cur1 > cur2:
                            crossed = True; break
                if not crossed: ok = False; break

            if not ok: continue

            # 普通条件检查（最新一行）
            latest = df.iloc[-1]
            for c in normal_conds:
                f, op, v = c['factor'], c['op'], c['value']
                col = f.replace("macd_dif","dif").replace("macd_dea","dea").replace("ma","ma")
                val = latest.get(col)
                if pd.isna(val): ok = False; break
                if op == 'lt' and val >= v: ok = False; break
                if op == 'gt' and val <= v: ok = False; break
                if op == 'lte' and val > v: ok = False; break
                if op == 'gte' and val < v: ok = False; break

            if ok:
                pe_row = self.conn.execute(
                    "SELECT pe, pb FROM a_daily_basic WHERE ts_code=? ORDER BY trade_date DESC LIMIT 1",
                    [ts_code]).fetchone()
                name_row = self.conn.execute(
                    "SELECT name FROM stock_info WHERE ts_code=?", [ts_code]).fetchone()
                chg = (df['close'].iloc[-1] / df['close'].iloc[-6] - 1) * 100 if len(df) > 5 else 0
                results.append({
                    "ts_code": ts_code,
                    "name": name_row[0] if name_row else "",
                    "pe": round(pe_row[0], 2) if pe_row and pe_row[0] else 0,
                    "pb": round(pe_row[1], 2) if pe_row and pe_row[1] else 0,
                    "change_pct": round(chg, 2),
                })

        return pd.DataFrame(results).head(200) if results else pd.DataFrame()

    def _build_parts(self, tables, conditions):
        joins, where, ta = [], ["si.market='A'"], {}
        for i, table in enumerate(tables):
            alias = f"t{i}"; ta[table] = alias
            cols = ", ".join(TABLE_COLUMNS.get(table, ["*"]))
            joins.append(f"""
                LEFT JOIN (SELECT ts_code, {cols} FROM (
                    SELECT ts_code, {cols},
                        ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) rn
                    FROM {table}) sub WHERE rn=1
                ) {alias} ON si.ts_code={alias}.ts_code""")
        for c in conditions:
            f, op, v = c["factor"], c.get("op","lt"), c["value"]
            if f not in FACTOR_SOURCES: continue
            table, col = FACTOR_SOURCES[f]; alias = ta[table]
            where.append(f"COALESCE({alias}.{col},0) {OP_MAP.get(op,op)} {v}")
            if f in ("pe_ttm","pb"): where.append(f"{alias}.{col}>0")
        return joins, where, ta

    def by_conditions(self, conditions, trade_date=None):
        tables = sorted(set(FACTOR_SOURCES[c["factor"]][0] for c in conditions if c["factor"] in FACTOR_SOURCES))
        if not tables: return pd.DataFrame()
        joins, where, _ = self._build_parts(tables, conditions)
        q = f"SELECT si.ts_code, si.name FROM stock_info si {''.join(joins)} WHERE {' AND '.join(where)} LIMIT 100"
        try: return self.conn.execute(q).fetchdf()
        except Exception as e: logger.error(f"by_conditions: {e}"); return pd.DataFrame()

    def by_template(self, template, trade_date=None):
        t = {
            "value_low_pe": [{"factor":"pe_ttm","op":"lt","value":20},{"factor":"pb","op":"lt","value":2}],
            "momentum_strong": [{"factor":"ret_20d","op":"gt","value":10}],
            "oversold_bounce": [{"factor":"rsi14","op":"lt","value":35},{"factor":"vol_ratio","op":"gt","value":1.5}],
        }
        return self.by_conditions(t.get(template, []), trade_date)
