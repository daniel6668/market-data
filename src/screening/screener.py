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
        """Python 侧计算交叉 + 筛选（限制500只以防超时）"""
        # 取最活跃的500只（按成交量排序）
        codes = [r[0] for r in self.conn.execute("""
            SELECT ts_code FROM a_daily
            WHERE trade_date >= (SELECT MAX(trade_date) FROM a_daily)
            ORDER BY vol DESC LIMIT 500
        """).fetchall()]
        if not codes: return pd.DataFrame()

        results = []
        for ts_code in codes:
            df = self.conn.execute("""
                SELECT trade_date, close FROM a_daily
                WHERE ts_code=? AND trade_date >= '2026-03-01'
                ORDER BY trade_date
            """, [ts_code]).fetchdf()
            if len(df) < 60: continue

            close = df['close']
            # 均线
            ma5 = close.rolling(5).mean()
            ma10 = close.rolling(10).mean()
            ma20 = close.rolling(20).mean()
            ma60 = close.rolling(60).mean()
            # MACD
            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            dif = ema12 - ema26
            dea = dif.ewm(span=9, adjust=False).mean()

            # 检查每个交叉条件
            series_map = {"ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60,
                          "dif": dif, "dea": dea}
            ok = True
            for c in cross_conds:
                f1, f2 = c['factor'], c["value"]
                col1 = f1.replace("macd_dif","dif").replace("ma","ma")
                col2 = f2.replace("macd_dea","dea").replace("ma","ma")
                s1 = series_map.get(col1); s2 = series_map.get(col2)
                if s1 is None or s2 is None: ok = False; break
                crossed = False
                # 取最近3个交易日（数据最后3行）
                n_last = min(3, len(df))
                for i in range(len(df)-1, len(df)-n_last, -1):
                    p1, p2 = s1.iloc[i-1], s2.iloc[i-1]
                    c1, c2 = s1.iloc[i], s2.iloc[i]
                    if pd.notna(p1) and pd.notna(c1) and p1 < p2 and c1 > c2:
                        crossed = True; break
                # 确保当前状态仍是金叉
                if crossed:
                    cur1, cur2 = s1.iloc[-1], s2.iloc[-1]
                    if pd.isna(cur1) or pd.isna(cur2) or cur1 <= cur2:
                        crossed = False
                if not crossed: ok = False; break

            if not ok: continue

            if ok:
                pe_row = self.conn.execute(
                    "SELECT pe, pb FROM a_daily_basic WHERE ts_code=? ORDER BY trade_date DESC LIMIT 1",
                    [ts_code]).fetchone()
                name = self.conn.execute(
                    "SELECT name FROM stock_info WHERE ts_code=?", [ts_code]).fetchone()
                chg = (close.iloc[-1]/close.iloc[-6]-1)*100 if len(df)>5 else 0
                results.append({
                    "ts_code": ts_code, "name": name[0] if name else "",
                    "pe": round(pe_row[0],2) if pe_row and pe_row[0] else 0,
                    "pb": round(pe_row[1],2) if pe_row and pe_row[1] else 0,
                    "change_pct": round(chg,2),
                })

        logger.info(f"cross search: {len(results)} from {len(codes)} stocks, filtering normal conds...")
        if not results: return pd.DataFrame()

        # 普通条件用 SQL 二次过滤（PE/PB等非技术因子）
        if normal_conds:
            codes_found = [r["ts_code"] for r in results]
            tables = sorted(set(FACTOR_SOURCES[c["factor"]][0]
                for c in normal_conds if c["factor"] in FACTOR_SOURCES))
            if tables:
                if "a_daily_basic" not in tables: tables.append("a_daily_basic")
                tables = sorted(set(tables))
                # 用这些 codes 构建临时过滤
                code_list = "','".join(codes_found)
                joins2, where2, _ = self._build_parts(tables, normal_conds)
                where2.append(f"si.ts_code IN ('{code_list}')")
                q = f"SELECT si.ts_code FROM stock_info si {''.join(joins2)} WHERE {' AND '.join(where2)}"
                try:
                    valid = {r[0] for r in self.conn.execute(q).fetchall()}
                    results = [r for r in results if r["ts_code"] in valid]
                except Exception as e:
                    logger.error(f"secondary filter: {e}")

        logger.info(f"cross search final: {len(results)} results")
        return pd.DataFrame(results) if results else pd.DataFrame()

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
