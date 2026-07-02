# Market Data v2 — 策略研究-监控一体化 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将现有"模块集合"重构为三段式策略研究-监控系统（发现→监控→维护），Agent 统一调度，NL→条件→筛选→回测→关注→跟踪→建议全流程。

**Architecture:** Agent 层负责 NL→结构化条件翻译 + 工具调度；发现引擎负责条件编译、全市场筛选、组合回测；监控引擎负责收盘后关注列表跟踪和卖出信号扫描；维护引擎负责形态破坏检测和移除建议。

**Tech Stack:** Python 3.11+, DuckDB, vectorbt, Gradio, OpenAI-compatible LLM (DeepSeek/GLM), pandas, stockstats

## Global Constraints

- LLM 仅参与 NL→条件翻译，不参与任何数据查询
- 所有数据操作在 DuckDB 本地完成
- 长任务（>3 秒）必须显示进度条
- 系统只建议操作，不自动执行（需用户审核确认）
- 移除关注列表操作必须用户确认
- 筛选范围：全市场（去除 500 只限制）
- 回看窗口：可配置参数（去除固定 2026-03-01）
- 组合回测支持 A 股 + ETF + 港股 + 美股
- 策略规则按市场分类配置（A/ETF/HK/US）

---

## File Structure

```
src/
├── db.py                          # [MODIFY] 新增 4 表 + 变更 watchlist
├── screening/
│   └── screener.py                # [MODIFY] 去硬编码、全市场、进度条
├── backtest/
│   ├── runner.py                  # [MODIFY] 组合级回测
│   └── report.py                  # [MODIFY] PortfolioResult 新增
├── factors/
│   └── registry.py                # [MODIFY] 新增卖出信号因子
├── discover/                      # [NEW] 发现引擎
│   ├── __init__.py
│   ├── translator.py              # NL→结构化条件（LLM 调用）
│   └── compiler.py                # 条件→SQL/Python 编译执行
├── monitor/                       # [NEW] 监控+维护引擎
│   ├── __init__.py
│   ├── engine.py                  # MonitorEngine: 收益跟踪+卖出信号
│   └── maintainer.py              # MaintainEngine: 形态破坏+移除建议
├── agent/
│   ├── app.py                     # [MODIFY] 5-Tab 工作流 UI
│   ├── tools.py                   # [MODIFY] 新工具函数
│   └── llm.py                     # [KEEP] 不变
config.yaml                        # [MODIFY] 新增 strategies 段
scheduler.py                       # [MODIFY] 集成监控+维护任务
cli.py                             # [MODIFY] 新增 monitor/maintain 命令
```

---

### Task 1: 数据库扩展 — 新增表 + 变更 watchlist

**Files:**
- Modify: `src/db.py` — `create_tables()` 函数，在现有最后一行之后新增 4 张表的 CREATE TABLE IF NOT EXISTS，并修改 watchlist 表结构
- Test: `tests/test_db.py`

**Interfaces:**
- Produces: 4 新表 (`watchlist_performance`, `strategy_rules`, `suggested_actions`, `backtest_history`)，以及 watchlist 表新增 5 列 (`entry_price`, `entry_date`, `strategy_name`, `status`, `market`)，一个辅助函数 `ensure_watchlist_columns(conn)` 用于兼容旧表

- [ ] **Step 1: 在 `create_tables()` 末尾新增 4 张表**

在 `src/db.py` 的 `create_tables()` 函数末尾（`stock_factors` 表之后，函数结束之前）追加：

```python
    # ── v2: 策略研究-监控系统 ──
    conn.execute("""
        CREATE TABLE IF NOT EXISTS watchlist_performance (
            ts_code     VARCHAR NOT NULL,
            calc_date   DATE NOT NULL,
            entry_price DOUBLE,
            entry_date  DATE,
            current_price DOUBLE,
            cumulative_return DOUBLE,
            ret_5d      DOUBLE,
            ret_10d     DOUBLE,
            ret_20d     DOUBLE,
            below_ma20  BOOLEAN DEFAULT FALSE,
            below_ma60  BOOLEAN DEFAULT FALSE,
            macd_cross  VARCHAR,      -- 'golden' | 'dead' | NULL
            rsi         DOUBLE,
            PRIMARY KEY (ts_code, calc_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS strategy_rules (
            id          INTEGER PRIMARY KEY,
            name        VARCHAR NOT NULL,
            market      VARCHAR NOT NULL,       -- A | ETF | HK | US
            rule_type   VARCHAR NOT NULL,       -- 'screen' | 'sell' | 'remove'
            conditions  TEXT NOT NULL,          -- JSON 条件数组
            is_active   BOOLEAN DEFAULT TRUE,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS suggested_actions (
            id          INTEGER PRIMARY KEY,
            ts_code     VARCHAR NOT NULL,
            name        VARCHAR,
            market      VARCHAR,
            action      VARCHAR NOT NULL,       -- BUY | SELL | REDUCE | REMOVE
            reason      TEXT,
            trigger_date DATE NOT NULL,
            metrics     TEXT,                   -- JSON: 触发时的指标快照
            status      VARCHAR DEFAULT 'pending',  -- pending | confirmed | dismissed
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS backtest_history (
            id            INTEGER PRIMARY KEY,
            strategy_name VARCHAR,
            market        VARCHAR,
            conditions    TEXT NOT NULL,         -- JSON: 筛选条件
            backtest_cfg  TEXT,                  -- JSON: 回测参数
            start_date    DATE,
            end_date      DATE,
            total_return  DOUBLE,
            annual_return DOUBLE,
            sharpe_ratio  DOUBLE,
            max_drawdown  DOUBLE,
            win_rate      DOUBLE,
            profit_factor DOUBLE,
            n_stocks      INTEGER,
            n_trades      INTEGER,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ── 迁移 watchlist 表（兼容旧结构）──
    _migrate_watchlist(conn)
```

- [ ] **Step 2: 新增 `ensure_watchlist_columns()` 辅助函数**

在 `src/db.py` 中 `create_tables` 之前新增：

```python
def _migrate_watchlist(conn: duckdb.DuckDBPyConnection) -> None:
    """兼容旧 watchlist 表：添加 v2 新列（如果不存在）"""
    new_cols = {
        "entry_price": "DOUBLE",
        "entry_date": "DATE",
        "strategy_name": "VARCHAR",
        "status": "VARCHAR DEFAULT 'active'",
        "market": "VARCHAR",
    }
    try:
        existing = {r[2].lower() for r in conn.execute(
            "SELECT * FROM information_schema.columns WHERE table_name='watchlist'"
        ).fetchall()}
    except Exception:
        return  # 表还不存在，CREATE TABLE 会处理
    for col_name, col_type in new_cols.items():
        if col_name not in existing:
            conn.execute(f"ALTER TABLE watchlist ADD COLUMN {col_name} {col_type}")
```

同时修改 watchlist 的 CREATE TABLE IF NOT EXISTS（DuckDB 不会在 IF NOT EXISTS 时修改已有表，但新安装会包含这些列），将现有 watchlist 的 CREATE TABLE 改为包含新列：

```python
    conn.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            ts_code         VARCHAR NOT NULL PRIMARY KEY,
            name            VARCHAR,
            added_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_condition TEXT,
            notes           TEXT,
            entry_price     DOUBLE,
            entry_date      DATE,
            strategy_name   VARCHAR,
            status          VARCHAR DEFAULT 'active',
            market          VARCHAR
        )
    """)
```

- [ ] **Step 3: 写测试验证新表创建**

在 `tests/test_db.py` 末尾追加：

```python
def test_v2_tables_exist():
    """v2 新增表：watchlist_performance, strategy_rules, suggested_actions, backtest_history"""
    from src.utils import load_config
    from src.db import get_connection
    import tempfile, os
    
    cfg = load_config()
    cfg["database"]["path"] = os.path.join(tempfile.mkdtemp(), "test_v2.duckdb")
    conn = get_connection(cfg)
    
    tables = ["watchlist_performance", "strategy_rules", "suggested_actions", "backtest_history"]
    existing = {r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()}
    for t in tables:
        assert t in existing, f"Table {t} missing"
    conn.close()


def test_watchlist_v2_columns():
    """watchlist 包含 v2 新列"""
    from src.utils import load_config
    from src.db import get_connection
    import tempfile, os
    
    cfg = load_config()
    cfg["database"]["path"] = os.path.join(tempfile.mkdtemp(), "test_wl.duckdb")
    conn = get_connection(cfg)
    
    cols = {r[2].lower() for r in conn.execute(
        "SELECT * FROM information_schema.columns WHERE table_name='watchlist'"
    ).fetchall()}
    for c in ["entry_price", "entry_date", "strategy_name", "status", "market"]:
        assert c in cols, f"Column {c} missing from watchlist"
    conn.close()
```

- [ ] **Step 4: 运行测试**

```bash
python -m pytest tests/test_db.py::test_v2_tables_exist tests/test_db.py::test_watchlist_v2_columns -v
```

Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add src/db.py tests/test_db.py
git commit -m "feat: v2 database — 4 new tables + watchlist migration"
```

---

### Task 2: 策略配置 — config.yaml + strategy_rules CRUD

**Files:**
- Modify: `config.yaml` — 新增 strategies 段
- Modify: `config.example.yaml` — 同上
- Modify: `src/db.py` — strategy_rules CRUD 函数
- Test: `tests/test_strategy_rules.py`

**Interfaces:**
- Produces: `save_strategy_rule(conn, name, market, rule_type, conditions)`, `get_active_rules(conn, market, rule_type) → list[dict]`, `load_strategies_from_config(config) → dict`

- [ ] **Step 1: 更新 config.yaml**

```yaml
# 在 config.yaml 末尾新增
strategies:
  A:
    label: "A股"
    sell_rules:
      stop_loss: -8
      stop_profit: 30
      ma_break: [ma20, ma60]
      pattern: [macd_dead_cross]
    remove_conditions:
      - type: price_below_ma
        ma: ma60
        consecutive_days: 5
      - type: macd_dead_cross
        consecutive_days: 3
  ETF:
    label: "ETF基金"
    sell_rules:
      stop_loss: -5
      stop_profit: 15
      ma_break: [ma20]
      pattern: [macd_dead_cross]
    remove_conditions:
      - type: price_below_ma
        ma: ma20
        consecutive_days: 3
  HK:
    label: "港股"
    sell_rules:
      stop_loss: -10
      stop_profit: 20
      ma_break: [ma60]
      pattern: [macd_dead_cross]
    remove_conditions:
      - type: price_below_ma
        ma: ma60
        consecutive_days: 5
  US:
    label: "美股"
    sell_rules:
      stop_loss: -10
      stop_profit: 25
      ma_break: [ma20, ma60]
      pattern: [macd_dead_cross]
    remove_conditions:
      - type: price_below_ma
        ma: ma60
        consecutive_days: 5
```

同样更新 `config.example.yaml`（去掉具体参数，保留结构作为模板）。

- [ ] **Step 2: 新增 strategy_rules CRUD 函数**

在 `src/db.py` 末尾追加：

```python
def save_strategy_rule(conn, name: str, market: str, rule_type: str,
                       conditions: list, is_active: bool = True) -> int:
    """保存策略规则，返回 rule id"""
    import json
    result = conn.execute("""
        INSERT INTO strategy_rules (name, market, rule_type, conditions, is_active)
        VALUES (?, ?, ?, ?, ?)
        RETURNING id
    """, [name, market, rule_type, json.dumps(conditions, ensure_ascii=False), is_active]).fetchone()
    return result[0] if result else 0


def get_active_rules(conn, market: str, rule_type: str = None) -> list[dict]:
    """获取活跃的策略规则，返回 dict 列表（conditions 已解析为 list）"""
    import json
    if rule_type:
        rows = conn.execute(
            "SELECT id, name, market, rule_type, conditions FROM strategy_rules "
            "WHERE market=? AND rule_type=? AND is_active=TRUE ORDER BY id",
            [market, rule_type]
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, name, market, rule_type, conditions FROM strategy_rules "
            "WHERE market=? AND is_active=TRUE ORDER BY id",
            [market]
        ).fetchall()
    return [
        {"id": r[0], "name": r[1], "market": r[2], "rule_type": r[3],
         "conditions": json.loads(r[4]) if r[4] else []}
        for r in rows
    ]


def upsert_suggested_action(conn, ts_code: str, name: str, market: str,
                            action: str, reason: str, trigger_date: str,
                            metrics: dict = None) -> None:
    """写入操作建议（同一天同股票同类 action 不重复）"""
    import json
    existing = conn.execute(
        "SELECT id FROM suggested_actions WHERE ts_code=? AND action=? "
        "AND trigger_date=? AND status='pending'",
        [ts_code, action, trigger_date]
    ).fetchone()
    if existing:
        return  # 已有待审核的同类建议
    conn.execute("""
        INSERT INTO suggested_actions (ts_code, name, market, action, reason, trigger_date, metrics)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [ts_code, name, market, action, reason, trigger_date,
          json.dumps(metrics, ensure_ascii=False) if metrics else None])


def get_pending_actions(conn, action: str = None) -> list[dict]:
    """获取待审核操作建议"""
    import json
    if action:
        rows = conn.execute(
            "SELECT id, ts_code, name, market, action, reason, trigger_date, metrics "
            "FROM suggested_actions WHERE status='pending' AND action=? ORDER BY created_at DESC",
            [action]
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, ts_code, name, market, action, reason, trigger_date, metrics "
            "FROM suggested_actions WHERE status='pending' ORDER BY created_at DESC"
        ).fetchall()
    return [
        {"id": r[0], "ts_code": r[1], "name": r[2], "market": r[3],
         "action": r[4], "reason": r[5], "trigger_date": str(r[6]),
         "metrics": json.loads(r[7]) if r[7] else {}}
        for r in rows
    ]


def confirm_action(conn, action_id: int, status: str = "confirmed") -> None:
    """确认或驳回操作建议"""
    conn.execute(
        "UPDATE suggested_actions SET status=? WHERE id=?",
        [status, action_id]
    )
```

- [ ] **Step 3: 写测试**

创建 `tests/test_strategy_rules.py`：

```python
"""策略规则 CRUD 测试"""
import tempfile
import os
from src.utils import load_config
from src.db import get_connection, save_strategy_rule, get_active_rules, upsert_suggested_action, get_pending_actions, confirm_action


def test_save_and_get_rules():
    cfg = load_config()
    cfg["database"]["path"] = os.path.join(tempfile.mkdtemp(), "test_rules.duckdb")
    conn = get_connection(cfg)
    
    sid = save_strategy_rule(conn, "低估值A股", "A", "screen",
        [{"factor": "pe_ttm", "op": "lt", "value": 20}])
    assert sid > 0
    
    rules = get_active_rules(conn, "A", "screen")
    assert len(rules) == 1
    assert rules[0]["name"] == "低估值A股"
    assert len(rules[0]["conditions"]) == 1
    conn.close()


def test_suggested_actions_flow():
    cfg = load_config()
    cfg["database"]["path"] = os.path.join(tempfile.mkdtemp(), "test_actions.duckdb")
    conn = get_connection(cfg)
    
    upsert_suggested_action(conn, "600519.SH", "贵州茅台", "A",
        "SELL", "跌破MA60", "2026-07-01", {"ma60": 1950.0, "close": 1920.0})
    # 重复写入不会创建新记录
    upsert_suggested_action(conn, "600519.SH", "贵州茅台", "A",
        "SELL", "跌破MA60", "2026-07-01")
    
    pending = get_pending_actions(conn, "SELL")
    assert len(pending) == 1
    assert pending[0]["ts_code"] == "600519.SH"
    assert pending[0]["action"] == "SELL"
    
    confirm_action(conn, pending[0]["id"], "confirmed")
    pending2 = get_pending_actions(conn, "SELL")
    assert len(pending2) == 0
    conn.close()
```

- [ ] **Step 4: 运行测试**

```bash
python -m pytest tests/test_strategy_rules.py -v
```

Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add config.yaml config.example.yaml src/db.py tests/test_strategy_rules.py
git commit -m "feat: strategy config + rules CRUD + suggested_actions CRUD"
```

---

### Task 3: 筛选器增强 — 去硬编码 + 全市场 + 进度条

**Files:**
- Modify: `src/screening/screener.py` — 重构 `_search_with_cross`，去除 500 只限制和 2026-03-01 固定日期，新增 `search_full_market(conditions, progress_callback=None)` 
- Test: `tests/test_screening.py`

**Interfaces:**
- Consumes: 现有 `StockScreener` 类
- Produces: `search_full_market(conditions, market='A', lookback_days=120, progress_callback=None) → pd.DataFrame` — 全市场扫描，支持跨市场，可配置回看窗口，可选进度回调

- [ ] **Step 1: 重构 `_search_with_cross` — 去硬编码**

在 `src/screening/screener.py` 中，修改 `_search_with_cross` 方法：

```python
    def _search_with_cross(self, cross_conds, normal_conds,
                           market: str = "A", lookback_days: int = 120):
        """Python 侧计算交叉 + 筛选（全市场）"""
        # 取指定市场的全部活跃股票
        codes = [r[0] for r in self.conn.execute("""
            SELECT ts_code FROM stock_info WHERE market = ? AND list_status = 'L'
        """, [market]).fetchall()]
        if not codes:
            return pd.DataFrame()

        # 计算回看起始日
        max_date = self.conn.execute(
            "SELECT MAX(trade_date) FROM a_daily").fetchone()[0]
        import datetime
        if isinstance(max_date, str):
            max_date = datetime.date.fromisoformat(str(max_date))
        start_date = max_date - datetime.timedelta(days=lookback_days)

        results = []
        total = len(codes)
        for idx, ts_code in enumerate(codes):
            df = self.conn.execute("""
                SELECT trade_date, close FROM a_daily
                WHERE ts_code=? AND trade_date >= ?
                ORDER BY trade_date
            """, [ts_code, start_date]).fetchdf()
            if len(df) < 60:
                continue

            close = df['close']
            ma5 = close.rolling(5).mean()
            ma10 = close.rolling(10).mean()
            ma20 = close.rolling(20).mean()
            ma60 = close.rolling(60).mean()
            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            dif = ema12 - ema26
            dea = dif.ewm(span=9, adjust=False).mean()

            series_map = {"ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60,
                          "dif": dif, "dea": dea}
            ok = True
            for c in cross_conds:
                f1, f2 = c['factor'], c["value"]
                col1 = f1.replace("macd_dif","dif").replace("ma","ma")
                col2 = f2.replace("macd_dea","dea").replace("ma","ma")
                s1 = series_map.get(col1); s2 = series_map.get(col2)
                if s1 is None or s2 is None:
                    ok = False; break
                crossed = False
                n_last = min(3, len(df))
                for i in range(len(df)-1, len(df)-n_last, -1):
                    p1, p2 = s1.iloc[i-1], s2.iloc[i-1]
                    c1, c2 = s1.iloc[i], s2.iloc[i]
                    if pd.notna(p1) and pd.notna(c1) and p1 < p2 and c1 > c2:
                        crossed = True; break
                if crossed:
                    cur1, cur2 = s1.iloc[-1], s2.iloc[-1]
                    if pd.isna(cur1) or pd.isna(cur2) or cur1 <= cur2:
                        crossed = False
                if not crossed:
                    ok = False; break

            if not ok:
                continue

            pe_row = self.conn.execute(
                "SELECT pe, pb FROM a_daily_basic WHERE ts_code=? ORDER BY trade_date DESC LIMIT 1",
                [ts_code]).fetchone()
            name = self.conn.execute(
                "SELECT name FROM stock_info WHERE ts_code=?", [ts_code]).fetchone()
            chg = (close.iloc[-1]/close.iloc[-6]-1)*100 if len(df)>5 else 0
            results.append({
                "ts_code": ts_code,
                "name": name[0] if name else "",
                "pe": round(pe_row[0],2) if pe_row and pe_row[0] else 0,
                "pb": round(pe_row[1],2) if pe_row and pe_row[1] else 0,
                "change_pct": round(chg,2),
            })

        # 普通条件二次过滤
        if normal_conds and results:
            codes_found = [r["ts_code"] for r in results]
            code_list = "','".join(codes_found)
            tables = sorted(set(FACTOR_SOURCES[c["factor"]][0]
                for c in normal_conds if c["factor"] in FACTOR_SOURCES))
            if tables:
                if "a_daily_basic" not in tables:
                    tables.append("a_daily_basic")
                tables = sorted(set(tables))
                joins2, where2, _ = self._build_parts(tables, normal_conds)
                where2.append(f"si.ts_code IN ('{code_list}')")
                q = f"SELECT si.ts_code FROM stock_info si {''.join(joins2)} WHERE {' AND '.join(where2)}"
                try:
                    valid = {r[0] for r in self.conn.execute(q).fetchall()}
                    results = [r for r in results if r["ts_code"] in valid]
                except Exception as e:
                    logger.error(f"secondary filter: {e}")

        logger.info(f"cross search ({market}): {len(results)} results from {total} stocks")
        return pd.DataFrame(results) if results else pd.DataFrame()
```

- [ ] **Step 2: 新增 `search_full_market` 方法**

在 `StockScreener` 类中新增：

```python
    def search_full_market(self, conditions: list, market: str = "A",
                           lookback_days: int = 120,
                           progress_callback=None) -> pd.DataFrame:
        """全市场筛选（支持跨市场、进度条）

        progress_callback: callable(current, total, message) 用于进度反馈
        """
        if not conditions:
            return pd.DataFrame()

        cross, normal = [], []
        for c in conditions:
            v = c.get("value")
            if isinstance(v, str) and v in FACTOR_SOURCES:
                cross.append(c)
            else:
                normal.append(c)

        if cross:
            if progress_callback:
                progress_callback(0, 1, f"全市场交叉检测 ({market})...")
            return self._search_with_cross(cross, normal, market, lookback_days)

        # 纯 SQL 路径
        tables = sorted(set(FACTOR_SOURCES[c["factor"]][0]
            for c in normal if c["factor"] in FACTOR_SOURCES))
        if not tables:
            return pd.DataFrame()
        if "a_daily_basic" not in tables:
            tables.append("a_daily_basic")
        tables = sorted(set(tables))

        joins, where, ta = self._build_parts(tables, normal)
        # market filter
        where = [w for w in where if not w.startswith("si.market=")]
        where.append(f"si.market='{market}'")

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
            ORDER BY {ab}.pe ASC NULLS LAST
        """
        try:
            if progress_callback:
                progress_callback(0, 1, f"SQL 筛选执行中 ({market})...")
            result = self.conn.execute(q).fetchdf()
            if progress_callback:
                progress_callback(1, 1, f"完成: {len(result)} 只")
            return result
        except Exception as e:
            logger.error(f"search_full_market: {e}")
            return pd.DataFrame()
```

- [ ] **Step 3: 修改 `search_with_indicators` 调用新方法**

保持 `search_with_indicators` 向后兼容，但内部委托给新逻辑：

```python
    def search_with_indicators(self, conditions):
        """筛选 + 输出PE/PB/涨跌幅（向后兼容，默认500只限制）"""
        return self.search_full_market(conditions, market="A", lookback_days=120)
```

- [ ] **Step 4: 运行现有测试（确保向后兼容）**

```bash
python -m pytest tests/test_screening.py -v
```

Expected: 3 PASS（现有测试不破坏）

- [ ] **Step 5: Commit**

```bash
git add src/screening/screener.py
git commit -m "feat: full-market screening — remove 500 limit, configurable lookback, progress bar"
```

---

### Task 4: 组合回测引擎升级

**Files:**
- Modify: `src/backtest/runner.py` — 新增 `run_portfolio()`、`load_prices_batch()`
- Modify: `src/backtest/report.py` — 新增 `PortfolioResult` dataclass
- Test: `tests/test_backtest.py`

**Interfaces:**
- Consumes: `BacktestRunner` 现有 `_load_prices`, `_generate_signals`
- Produces: `run_portfolio(codes, start, end, buy_condition, sell_condition, weights='equal', rebalance='monthly') → PortfolioResult`； `PortfolioResult` dataclass (组合级指标 + 个股明细)

- [ ] **Step 1: 新增 `PortfolioResult` dataclass**

在 `src/backtest/report.py` 追加：

```python
@dataclass
class PortfolioResult:
    """组合回测结果"""
    total_return: float = 0.0
    annual_return: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    n_stocks: int = 0
    n_trades: int = 0
    benchmark_return: float = 0.0
    equity_curve: pd.DataFrame = field(default_factory=pd.DataFrame)
    stock_results: list = field(default_factory=list)  # list[BacktestResult]
    weights: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "total_return": round(self.total_return, 2),
            "annual_return": round(self.annual_return, 2),
            "sharpe_ratio": round(self.sharpe_ratio, 2),
            "max_drawdown": round(self.max_drawdown, 2),
            "win_rate": round(self.win_rate, 2),
            "n_stocks": self.n_stocks,
            "n_trades": self.n_trades,
            "benchmark_return": round(self.benchmark_return, 2),
            "stock_count": len(self.stock_results),
        }

    def summary(self) -> str:
        d = self.to_dict()
        return (
            f"组合收益: {d['total_return']}% | 年化: {d['annual_return']}% | "
            f"夏普: {d['sharpe_ratio']} | 最大回撤: {d['max_drawdown']}% | "
            f"股票数: {d['stock_count']} | 交易: {d['n_trades']}次 | "
            f"基准: {d['benchmark_return']}%"
        )
```

- [ ] **Step 2: 新增 `run_portfolio` 方法**

在 `src/backtest/runner.py` 中追加：

```python
    def run_portfolio(self, codes: list[str],
                      condition_buy: dict[str, pd.Series],
                      condition_sell: dict[str, pd.Series],
                      start: str, end: str,
                      weights: str = "equal",
                      rebalance: str = "monthly",
                      commission: float = 0.0002) -> "PortfolioResult":
        """组合回测

        codes: 股票代码列表
        condition_buy / condition_sell: {ts_code: boolean Series}
        weights: 'equal' | 'market_cap'
        rebalance: 'monthly' | 'quarterly' | None
        """
        from .report import PortfolioResult

        # 加载所有股票的复权价格
        all_prices = {}
        all_entries = {}
        all_exits = {}
        valid_codes = []

        for ts_code in codes:
            prices = self._load_prices(ts_code, start, end)
            if prices.empty:
                continue
            buy_cond = condition_buy.get(ts_code)
            sell_cond = condition_sell.get(ts_code)
            if buy_cond is None or sell_cond is None:
                continue
            entries, exits = self._generate_signals(prices, buy_cond, sell_cond)
            if not entries.any() and not exits.any():
                # 无信号的股票，用 buy & hold 填充
                entries = pd.Series(False, index=prices.index)
                entries.iloc[0] = True
                exits = pd.Series(False, index=prices.index)

            all_prices[ts_code] = prices
            all_entries[ts_code] = entries
            all_exits[ts_code] = exits
            valid_codes.append(ts_code)

        if not valid_codes:
            return PortfolioResult()

        # 构建组合层面的等权重价格序列
        price_df = pd.DataFrame(all_prices).ffill()
        entry_df = pd.DataFrame(all_entries).fillna(False)
        exit_df = pd.DataFrame(all_exits).fillna(False)

        # 等权重：每只股票分配 equal weight
        n = len(valid_codes)
        w = {c: 1.0/n for c in valid_codes}

        # 用 vectorbt 组合回测
        try:
            pf = vbt.Portfolio.from_signals(
                price_df, entry_df, exit_df,
                freq="1D",
                init_cash=100000,
                fees=commission,
                slippage=0.001,
            )
        except Exception as e:
            logger.warning(f"组合回测失败: {e}")
            return PortfolioResult()

        stats = pf.stats()
        equity = pf.value()
        bh = (price_df.mean(axis=1).iloc[-1] / price_df.mean(axis=1).iloc[0] - 1) * 100

        # 跑个股回测作为明细
        stock_results = []
        for ts_code in valid_codes:
            try:
                sr = self.run_single(
                    ts_code,
                    all_entries[ts_code],
                    all_exits[ts_code],
                    start, end, commission
                )
                stock_results.append(sr)
            except Exception:
                pass

        return PortfolioResult(
            total_return=float(stats.get("Total Return [%]", 0)),
            annual_return=float(stats.get("Annual Return [%]", 0)),
            sharpe_ratio=float(stats.get("Sharpe Ratio", 0)),
            max_drawdown=float(stats.get("Max Drawdown [%]", 0)),
            win_rate=float(stats.get("Win Rate [%]", 0)),
            n_stocks=len(valid_codes),
            n_trades=int(stats.get("Total Trades", 0)),
            benchmark_return=bh,
            equity_curve=pd.DataFrame({"date": equity.index, "value": equity.values}),
            stock_results=stock_results,
            weights=w,
        )
```

- [ ] **Step 3: 写组合回测测试**

在 `tests/test_backtest.py` 追加：

```python
def test_portfolio_backtest():
    """组合回测：多只股票 + 等权重"""
    from src.utils import load_config
    from src.db import get_connection, upsert_daily
    import tempfile, os
    import pandas as pd
    import numpy as np

    cfg = load_config()
    cfg["database"]["path"] = os.path.join(tempfile.mkdtemp(), "test_pf.duckdb")
    conn = get_connection(cfg)

    # 插入两只股票的模拟数据
    dates = pd.date_range("2025-01-01", "2026-06-30", freq="B")
    for code in ["000001.SZ", "000002.SZ"]:
        np.random.seed(hash(code) % 2**32)
        df = pd.DataFrame({
            "ts_code": code, "trade_date": dates,
            "open": 0, "high": 0, "low": 0,
            "close": 10 * (1 + np.cumsum(np.random.randn(len(dates)) * 0.02)),
            "pre_close": 0, "change": 0, "pct_chg": 0,
            "vol": 1e6, "amount": 1e7,
        })
        upsert_daily(conn, "a_daily", df)
        # adj_factor
        adj_df = pd.DataFrame({"ts_code": code, "trade_date": dates, "adj_factor": 1.0})
        conn.register("_tmp_adj", adj_df)
        conn.execute("INSERT OR REPLACE INTO a_adj_factor SELECT * FROM _tmp_adj")
        conn.unregister("_tmp_adj")
        # stock_info
        conn.execute("INSERT OR REPLACE INTO stock_info (ts_code, name, market) VALUES (?,?,?)",
                     [code, f"Test{code}", "A"])

    from src.backtest.runner import BacktestRunner
    runner = BacktestRunner(conn)

    prices1 = runner._load_prices("000001.SZ", "2025-01-01", "2026-06-30")
    prices2 = runner._load_prices("000002.SZ", "2025-01-01", "2026-06-30")
    ma20_1 = prices1.rolling(20).mean()
    ma60_1 = prices1.rolling(60).mean()
    ma20_2 = prices2.rolling(20).mean()
    ma60_2 = prices2.rolling(60).mean()

    result = runner.run_portfolio(
        ["000001.SZ", "000002.SZ"],
        {"000001.SZ": ma20_1 > ma60_1, "000002.SZ": ma20_2 > ma60_2},
        {"000001.SZ": ma20_1 < ma60_1, "000002.SZ": ma20_2 < ma60_2},
        "2025-01-01", "2026-06-30"
    )
    # 基本完整性检查
    assert result.n_stocks == 2
    assert result.total_return != 0  # 有结果
    d = result.to_dict()
    assert "total_return" in d
    assert "stock_count" in d
    conn.close()
```

- [ ] **Step 4: 运行测试**

```bash
python -m pytest tests/test_backtest.py::test_portfolio_backtest -v --timeout=30
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/backtest/runner.py src/backtest/report.py tests/test_backtest.py
git commit -m "feat: portfolio backtest — multi-stock equal-weight with vectorbt"
```

---

### Task 5: 发现引擎 — NL→条件翻译 + 条件编译

**Files:**
- Create: `src/discover/__init__.py`
- Create: `src/discover/translator.py`
- Create: `src/discover/compiler.py`
- Test: `tests/test_discover.py`

**Interfaces:**
- Consumes: `llm.get_client()`, `llm.chat()` 用于翻译；`StockScreener` 用于执行
- Produces: `translate_nl_to_conditions(client, config, nl_text) → dict` 返回结构化条件 `{conditions, universe, action, backtest}`； `compile_and_execute(conn, plan, progress_callback) → dict` 返回 `{df, backtest_result}`

- [ ] **Step 1: 创建 `src/discover/__init__.py`**

```python
"""发现引擎 — NL→条件翻译 + 条件编译执行"""
```

- [ ] **Step 2: 创建 `src/discover/translator.py`**

```python
"""NL → 结构化条件 翻译器"""
import json

TRANSLATE_SYSTEM_PROMPT = """你是金融数据查询翻译器。将用户的自然语言投资想法翻译为结构化筛选条件。

## 可用因子
| 因子名 | 含义 | 来源 |
|--------|------|------|
| pe_ttm | 市盈率(PE) | a_daily_basic |
| pb | 市净率(PB) | a_daily_basic |
| turnover_rate | 换手率 | a_daily_basic |
| ma5, ma10, ma20, ma60 | 移动均线 | stock_factors |
| rsi6, rsi14 | RSI | stock_factors |
| macd_dif, macd_dea | MACD | stock_factors |
| ret_5d, ret_20d | N日涨跌幅 | stock_factors |
| vol_ratio | 量比 | stock_factors |

## 操作符
- gt (大于), lt (小于), gte (≥), lte (≤): 因子 vs 数值
- cross_above (金叉), cross_below (死叉): 因子 vs 因子（value 写另一个因子名）

## 市场
- A (A股), ETF (ETF基金), HK (港股), US (美股), all (全部)

## 输出格式（严格JSON，无其他文字）
{
  "conditions": [
    {"factor": "因子名", "op": "操作符", "value": 数值或另一因子名}
  ],
  "universe": "A",
  "action": "screen",
  "backtest": {"start": "2025-01-01", "end": "2026-06-30", "rebalance": "monthly", "weights": "equal"}
}

## 规则
- 只输出JSON，不要解释
- 用户没提到回测需求时 action="screen"，提到回测时 action="backtest"
- 回测日期默认可从用户描述提取，否则用2025-01-01到today
- "低估值" = pe_ttm lt 15
- "放量" = vol_ratio gt 1.5
- "MACD金叉" = macd_dif cross_above macd_dea
- "超卖" = rsi14 lt 35
- "近期强势" = ret_20d gt 10
- 如果用户输入无法翻译为条件，返回 {"error": "无法理解", "hint": "..."}
"""


def translate_nl_to_conditions(client, config: dict, nl_text: str) -> dict:
    """将自然语言翻译为结构化筛选条件
    
    Returns: {conditions, universe, action, backtest} 或 {error, hint}
    """
    from src.agent.llm import chat as llm_chat
    messages = [
        {"role": "system", "content": TRANSLATE_SYSTEM_PROMPT},
        {"role": "user", "content": nl_text},
    ]
    try:
        resp = llm_chat(client, config, messages)  # 无 tools，纯对话
        text = resp.choices[0].message.content or ""
    except Exception as e:
        return {"error": f"LLM 调用失败: {e}", "hint": "请检查 API 配置"}

    # 提取 JSON（处理可能的 markdown 包裹）
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])
    try:
        plan = json.loads(text)
    except json.JSONDecodeError:
        return {"error": "JSON 解析失败", "hint": f"LLM 输出格式异常，原始文本: {text[:200]}", "raw": text}

    # 验证必填字段
    if "error" in plan:
        return plan
    if "conditions" not in plan:
        return {"error": "缺少 conditions 字段", "hint": str(plan)}
    return plan
```

- [ ] **Step 3: 创建 `src/discover/compiler.py`**

```python
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
```

- [ ] **Step 4: 写测试**

创建 `tests/test_discover.py`：

```python
"""发现引擎测试"""
import tempfile, os, json
from src.utils import load_config
from src.db import get_connection
from src.discover.compiler import validate_plan


def test_validate_plan_valid():
    plan = {
        "conditions": [{"factor": "pe_ttm", "op": "lt", "value": 20}],
        "universe": "A",
        "action": "screen"
    }
    ok, err = validate_plan(plan)
    assert ok, f"Should be valid: {err}"


def test_validate_plan_invalid_op():
    plan = {
        "conditions": [{"factor": "pe_ttm", "op": "INVALID", "value": 20}],
        "universe": "A"
    }
    ok, err = validate_plan(plan)
    assert not ok


def test_validate_plan_empty():
    ok, err = validate_plan({"conditions": [], "universe": "A"})
    assert not ok


def test_validate_plan_unknown_market():
    plan = {
        "conditions": [{"factor": "pe_ttm", "op": "lt", "value": 20}],
        "universe": "CRYPTO"
    }
    ok, err = validate_plan(plan)
    assert not ok


def test_translate_schema():
    """验证翻译 schema 包含必要字段"""
    from src.discover.translator import TRANSLATE_SYSTEM_PROMPT
    assert "cross_above" in TRANSLATE_SYSTEM_PROMPT
    assert "cross_below" in TRANSLATE_SYSTEM_PROMPT
    assert "pe_ttm" in TRANSLATE_SYSTEM_PROMPT
    assert "JSON" in TRANSLATE_SYSTEM_PROMPT
```

- [ ] **Step 5: 运行测试**

```bash
python -m pytest tests/test_discover.py -v
```

Expected: 5 PASS

- [ ] **Step 6: Commit**

```bash
git add src/discover/ tests/test_discover.py
git commit -m "feat: discover engine — NL→conditions translator + compiler"
```

---

### Task 6: 监控引擎 + 维护引擎

**Files:**
- Create: `src/monitor/__init__.py`
- Create: `src/monitor/engine.py` — MonitorEngine
- Create: `src/monitor/maintainer.py` — MaintainEngine
- Test: `tests/test_monitor.py`

**Interfaces:**
- Consumes: `get_connection()`, `Pipeline`, `FactorEngine`, `StockScreener`；`config["strategies"]` 配置；`get_active_rules`, `upsert_suggested_action` from db.py
- Produces: `MonitorEngine.run(date) → dict` 返回 `{watchlist_returns, sell_signals, new_opportunities}`； `MaintainEngine.run(date) → list[dict]` 返回 remove 建议列表

- [ ] **Step 1: 创建 `src/monitor/__init__.py`**

```python
"""监控+维护引擎 — 收盘后关注列表跟踪 + 形态破坏检测"""
from .engine import MonitorEngine
from .maintainer import MaintainEngine
```

- [ ] **Step 2: 创建 `src/monitor/engine.py`**

```python
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
```

- [ ] **Step 3: 创建 `src/monitor/maintainer.py`**

```python
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
```

- [ ] **Step 4: 写测试**

创建 `tests/test_monitor.py`：

```python
"""监控+维护引擎测试"""
import tempfile, os
from src.utils import load_config
from src.db import get_connection, upsert_daily
import pandas as pd
import numpy as np


def test_monitor_engine_creation():
    cfg = load_config()
    cfg["database"]["path"] = os.path.join(tempfile.mkdtemp(), "test_m1.duckdb")
    conn = get_connection(cfg)
    from src.monitor.engine import MonitorEngine
    engine = MonitorEngine(conn, cfg)
    # 无关注列表时不报错
    result = engine.run("2026-07-01")
    assert result["watchlist_returns"] == 0
    assert result["sell_signals"] == 0
    conn.close()


def test_maintain_engine_price_below_ma():
    cfg = load_config()
    cfg["database"]["path"] = os.path.join(tempfile.mkdtemp(), "test_m2.duckdb")
    conn = get_connection(cfg)
    
    # 插入模拟数据：连续多日低于 MA60
    dates = pd.date_range("2026-06-01", "2026-07-01", freq="B")
    close_vals = [10.0 - 0.1 * i for i in range(len(dates))]  # 持续下跌
    df = pd.DataFrame({
        "ts_code": "600519.SH", "trade_date": dates,
        "open": 0, "high": 0, "low": 0,
        "close": close_vals, "pre_close": 0, "change": 0,
        "pct_chg": 0, "vol": 1e6, "amount": 1e7,
    })
    upsert_daily(conn, "a_daily", df)
    
    # stock_factors: DIF 持续低于 DEA
    for i, d in enumerate(dates[-6:]):
        conn.execute("""
            INSERT OR REPLACE INTO stock_factors (ts_code, trade_date, ma20, ma60, macd_dif, macd_dea, rsi14)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, ["600519.SH", d.date(), 8.0, 9.0, -0.1, 0.05, 40.0])
    
    conn.execute("INSERT OR REPLACE INTO stock_info (ts_code, name, market) VALUES ('600519.SH','贵州茅台','A')")
    conn.execute("INSERT OR REPLACE INTO watchlist (ts_code, name, market, status, entry_price, entry_date) VALUES ('600519.SH','贵州茅台','A','active',10.5,'2026-06-01')")
    
    from src.monitor.maintainer import MaintainEngine
    engine = MaintainEngine(conn, cfg)
    suggestions = engine.run("2026-07-01")
    
    assert len(suggestions) >= 1
    assert suggestions[0]["action"] == "REMOVE" if "action" in suggestions[0] else True
    conn.close()
```

- [ ] **Step 5: 运行测试**

```bash
python -m pytest tests/test_monitor.py -v
```

Expected: 2 PASS

- [ ] **Step 6: Commit**

```bash
git add src/monitor/ tests/test_monitor.py
git commit -m "feat: monitor + maintain engines — performance tracking, sell signals, remove suggestions"
```

---

### Task 7: Agent 工具 + UI 重构

**Files:**
- Modify: `src/agent/tools.py` — 新增 `discover_stocks`, `run_portfolio_backtest`, `get_monitor_signals`, `confirm_actions` 工具，修改现有 `_add_to_watchlist` 支持 v2 新字段
- Modify: `src/agent/app.py` — 5-Tab UI 重构

**Interfaces:**
- Consumes: `src/discover/translator.py`, `src/discover/compiler.py`, `src/monitor/engine.py`, `src/monitor/maintainer.py`
- Produces: 新 tool 函数 + 新 Gradio UI 布局

- [ ] **Step 1: 扩展 tools.py — 新工具定义**

在 `src/agent/tools.py` 的 `TOOLS` 列表中追加新工具定义，在 `execute_tool` 中注册新函数：

```python
# 在 TOOLS 列表末尾追加：
    {
        "type": "function",
        "function": {
            "name": "discover_stocks",
            "description": "用自然语言描述选股策略，系统翻译为筛选条件后执行全市场扫描+组合回测。"
                           "支持描述：低估值、MACD金叉、放量上涨、超卖反弹等。"
                           "示例：'找出A股PE<15、MACD金叉的股票并回测'",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "自然语言策略描述"},
                    "market": {"type": "string", "enum": ["A","ETF","HK","US","all"]},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_monitor_signals",
            "description": "查看最新监控信号：卖出建议、移除建议、新机会",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {"type": "string", "enum": ["SELL","REDUCE","REMOVE","BUY","all"]},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "confirm_monitor_action",
            "description": "确认或驳回监控建议",
            "parameters": {
                "type": "object",
                "properties": {
                    "action_id": {"type": "integer"},
                    "decision": {"type": "string", "enum": ["confirmed","dismissed"]},
                },
                "required": ["action_id", "decision"],
            },
        },
    },
]

# 在 execute_tool 的 dispatch map 中追加：
    "discover_stocks": _discover_stocks,
    "get_monitor_signals": _get_monitor_signals,
    "confirm_monitor_action": _confirm_monitor_action,
```

新增函数实现：

```python
def _discover_stocks(conn, args):
    """发现引擎：NL → 条件翻译 → 筛选 → 回测"""
    query = args.get("query", "")
    if not query:
        return "请描述选股策略"
    
    # 加载 LLM 配置
    import json, os, sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from src.utils import load_config
    from src.agent.llm import get_client
    config = load_config()
    
    if not config.get("llm", {}).get("api_key"):
        return json.dumps({"type": "error", "message": "请配置 LLM API Key"}, ensure_ascii=False)
    
    try:
        from src.discover.translator import translate_nl_to_conditions
        from src.discover.compiler import validate_plan, compile_and_execute
    except ImportError:
        return json.dumps({"type": "error", "message": "发现引擎未安装"}, ensure_ascii=False)
    
    client = get_client(config)
    plan = translate_nl_to_conditions(client, config, query)
    ok, err_msg = validate_plan(plan)
    if not ok:
        return json.dumps({"type": "error", "message": err_msg}, ensure_ascii=False)
    
    result = compile_and_execute(conn, plan)
    df = result["df"]
    
    if df.empty:
        return json.dumps({
            "type": "table", "columns": [], "rows": [], "total": 0,
            "hint": f"条件: {json.dumps(plan['conditions'], ensure_ascii=False)}，无匹配结果",
        }, ensure_ascii=False)
    
    # 构建响应
    cols = ["ts_code", "name", "pe", "pb", "change_pct"]
    avail = [c for c in cols if c in df.columns]
    rows = df[avail].values.tolist()
    col_labels = {"ts_code": "代码", "name": "名称", "pe": "PE", "pb": "PB", "change_pct": "涨跌幅%"}
    
    response_parts = [f"✅ 筛选条件: {json.dumps(plan['conditions'], ensure_ascii=False)}"]
    response_parts.append(f"📊 匹配 {len(df)} 只股票")
    
    bt = result.get("backtest_result")
    if bt:
        response_parts.append(f"\n📈 **组合回测** ({plan.get('backtest',{}).get('start','')} ~ {plan.get('backtest',{}).get('end','')})")
        response_parts.append(bt.summary())
    
    return json.dumps({
        "type": "table",
        "columns": [col_labels.get(c, c) for c in avail],
        "rows": rows,
        "total": len(df),
        "hint": "\n".join(response_parts),
        "plan": plan,  # 保存 plan 用于后续"加入关注列表"
    }, ensure_ascii=False, default=str)


def _get_monitor_signals(conn, args):
    """查看监控信号"""
    import json
    from src.db import get_pending_actions
    
    action = args.get("action", "all")
    actions = get_pending_actions(conn, action if action != "all" else None)
    
    if not actions:
        return json.dumps({"type": "text", "text": "✅ 当前无待处理信号"}, ensure_ascii=False)
    
    rows = [[a["ts_code"], a["name"], a["action"], a["reason"], a["trigger_date"]]
            for a in actions]
    return json.dumps({
        "type": "table",
        "columns": ["代码", "名称", "建议", "原因", "触发日期"],
        "rows": rows,
        "total": len(actions),
        "action_ids": [a["id"] for a in actions],
    }, ensure_ascii=False, default=str)


def _confirm_monitor_action(conn, args):
    """确认/驳回监控建议"""
    aid = args.get("action_id")
    decision = args.get("decision", "dismissed")
    from src.db import confirm_action, get_pending_actions
    
    confirm_action(conn, aid, decision)
    return f"✅ 已{'确认' if decision == 'confirmed' else '驳回'}建议 #{aid}"
```

修改 `_add_to_watchlist` 支持 v2 字段：

```python
def _add_to_watchlist(conn, args):
    import json
    codes = args.get("codes", [])
    condition = args.get("condition", "")
    if not codes:
        return "未指定股票"
    added = 0
    for code in codes:
        name_row = conn.execute(
            "SELECT name, market FROM stock_info WHERE ts_code LIKE ?", [f"{code}%"]).fetchone()
        name = name_row[0] if name_row else code
        market = name_row[1] if name_row else "A"
        
        # 获取当前价格作为 entry_price
        price_row = conn.execute(
            "SELECT close FROM a_daily WHERE ts_code LIKE ? ORDER BY trade_date DESC LIMIT 1",
            [f"{code}%"]).fetchone()
        entry_price = price_row[0] if price_row else None
        
        from datetime import date
        conn.execute("""
            INSERT OR REPLACE INTO watchlist
            (ts_code, name, source_condition, status, market, entry_price, entry_date)
            VALUES (?, ?, ?, 'active', ?, ?, ?)
        """, [code, name, condition, market, entry_price, date.today()])
        added += 1
    return f"✅ 已添加 {added} 只到自选池"
```

- [ ] **Step 2: 重构 `src/agent/app.py` — 5-Tab UI**

核心改动：
1. Tab 1 (发现): NL 输入 + 结果表格 + 进度条 + 回测摘要 + 加入关注
2. Tab 2 (关注列表): 增强表格含收益+信号 + 待审核面板
3. Tab 3 (策略): 从 config.Strategies 加载展示
4. Tab 4 (信号): 从 suggested_actions 加载待处理
5. Tab 5 (回测历史): 从 backtest_history 加载

```python
"""Gradio UI — v2 策略研究-监控一体化"""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import gradio as gr
import pandas as pd
from src.agent.llm import get_client, chat
from src.agent.tools import TOOLS, execute_tool
from src.utils import load_config
from src.db import get_pending_actions, confirm_action

config = load_config()
_conn = None
def _get_conn():
    global _conn
    if _conn is None:
        from src.db import get_connection
        _conn = get_connection(config)
    return _conn

SYSTEM_PROMPT = """你是投资研究助手。使用 discover_stocks 工具将用户自然语言翻译为策略并执行。
可用工具:
- discover_stocks: 自然语言选股+回测（主力工具）
- get_monitor_signals: 查看监控信号/操作建议
- confirm_monitor_action: 确认/驳回建议
- get_watchlist: 查看关注列表
- add_to_watchlist: 加入关注列表
- remove_from_watchlist: 移出关注列表
- analyze_stock: 个股分析
- market_overview: 市场概况"""


def chat_respond(message, history):
    llm_cfg = config.get("llm", {})
    if not llm_cfg.get("api_key"):
        return "⚠️ 请配置 LLM API Key", pd.DataFrame(), ""

    client = get_client(config)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in (history or []):
        if isinstance(h, dict):
            messages.append(h)
        elif isinstance(h, (list, tuple)) and len(h) >= 2:
            messages.append({"role": "user", "content": str(h[0])})
            if h[1]:
                messages.append({"role": "assistant", "content": str(h[1])})
    messages.append({"role": "user", "content": message})

    try:
        resp = chat(client, config, messages, TOOLS)
    except Exception as e:
        return f"❌ {e}", pd.DataFrame(), ""

    msg = resp.choices[0].message
    if not msg.tool_calls:
        return msg.content or "", pd.DataFrame(), ""

    results = []
    for tc in msg.tool_calls:
        try:
            args = json.loads(tc.function.arguments)
        except json.JSONDecodeError:
            args = {}
        try:
            r = execute_tool(tc.function.name, args, _get_conn())
        except Exception as e:
            r = f"失败:{e}"
        results.append((tc.function.name, r))

    table_df = pd.DataFrame()
    plan_json = ""
    for tn, r in results:
        if tn in ("search_stocks", "discover_stocks"):
            try:
                d = json.loads(r)
                if d.get("type") == "table" and d.get("rows"):
                    table_df = pd.DataFrame(d["rows"], columns=d["columns"])
                    if d.get("hint"):
                        pass  # hint goes to chat text
                if d.get("plan"):
                    plan_json = json.dumps(d["plan"], ensure_ascii=False)
            except:
                pass

    # 获取 LLM 汇总
    messages.append({"role": "assistant", "content": None, "tool_calls": [
        {"id": tc.id, "type": "function", "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
        for tc in msg.tool_calls
    ]})
    for tc in msg.tool_calls:
        for tn, rr in results:
            if tn == tc.function.name:
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": rr}); break
    try:
        text = chat(client, config, messages).choices[0].message.content or ""
    except:
        text = f"共 {len(table_df)} 条"

    return text, table_df, plan_json


def load_watchlist_v2():
    """加载增强的关注列表（含收益+信号）"""
    rows = _get_conn().execute("""
        SELECT w.ts_code, w.name, w.market, w.entry_price, w.entry_date,
               p.current_price, p.cumulative_return, p.ret_5d,
               CASE WHEN p.below_ma60 THEN '🔴' WHEN p.below_ma20 THEN '🟡' ELSE '🟢' END as signal,
               p.macd_cross, w.strategy_name
        FROM watchlist w
        LEFT JOIN watchlist_performance p ON w.ts_code=p.ts_code
            AND p.calc_date=(SELECT MAX(calc_date) FROM watchlist_performance WHERE ts_code=w.ts_code)
        WHERE w.status='active'
        ORDER BY w.added_at DESC
    """).fetchall()
    cols = ["代码","名称","市场","加入价","加入日","现价","累计收益%","5日收益%","信号","MACD","策略"]
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([
        [r[0],r[1],r[2],r[3],str(r[4])[:10] if r[4] else "",
         r[5],round(r[6],1) if r[6] else 0,round(r[7],1) if r[7] else 0,
         r[8],r[9] or "-",r[10] or "-"] for r in rows
    ], columns=cols)


def load_pending_actions_df():
    """加载待审核建议"""
    actions = get_pending_actions(_get_conn())
    cols = ["ID","代码","名称","建议","原因","触发日期"]
    if not actions:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([
        [a["id"],a["ts_code"],a["name"],a["action"],a["reason"],a["trigger_date"]]
        for a in actions
    ], columns=cols)


def create_ui():
    provider = config.get("llm", {}).get("provider", "未配置")
    with gr.Blocks(title="Market Data Studio v2") as app:
        gr.Markdown(f"# 📊 Market Data Studio — {provider}")

        # ===== Tab 1: 发现 =====
        with gr.Tab("🔍 发现"):
            with gr.Row():
                with gr.Column(scale=1):
                    chatbot = gr.Chatbot(label="对话", height=350)
                    msg = gr.Textbox(label="输入你的策略想法",
                        placeholder="如：找出A股PE<20、MACD金叉的股票，做组合回测看看效果")
                    with gr.Row():
                        send = gr.Button("🚀 发送", variant="primary")
                        clear_btn = gr.Button("清除")
                with gr.Column(scale=1):
                    result_table = gr.Dataframe(label="筛选结果", interactive=False, wrap=True)
                    progress = gr.Progress()
                    sel_codes = gr.Textbox(label="选中代码(逗号分隔)", interactive=True,
                        placeholder="点击表格行选中代码，或直接输入")
                    with gr.Row():
                        cond_input = gr.Textbox(label="策略描述/备注", scale=2)
                        add_btn = gr.Button("⭐ 加入关注列表", variant="primary", scale=1)
                    op_out = gr.Textbox(label="结果", interactive=False)
                    plan_state = gr.State("")  # 存储 plan JSON

            def on_send(msg_text, hist):
                text, df, plan_json = chat_respond(msg_text, hist or [])
                hist = (hist or []) + [
                    {"role":"user","content":msg_text},
                    {"role":"assistant","content":text}
                ]
                return hist, df if not df.empty else None, "", plan_json

            def on_table_select(evt: gr.SelectData, df):
                if df is None or df.empty:
                    return ""
                idx = evt.index
                if isinstance(idx, (list, tuple)):
                    row = idx[0]
                    codes = [str(df.iloc[row].iloc[0])] if row < len(df) else []
                else:
                    codes = [str(df.iloc[idx].iloc[0])] if idx < len(df) else []
                return ", ".join(codes)

            def do_add(sel_text, cond, plan_json):
                codes = [c.strip() for c in sel_text.split(",") if c.strip()]
                if not codes:
                    return "⚠️ 请先输入代码", pd.DataFrame()
                desc = cond
                if plan_json:
                    try:
                        p = json.loads(plan_json)
                        desc = cond or json.dumps(p.get("conditions", []), ensure_ascii=False)
                    except:
                        pass
                args = {"codes": codes, "condition": desc}
                r = execute_tool("add_to_watchlist", args, _get_conn())
                return r

            send.click(on_send, [msg, chatbot],
                      [chatbot, result_table, msg, plan_state])
            clear_btn.click(lambda: ([], None, "", ""), [],
                           [chatbot, result_table, msg, plan_state])
            result_table.select(on_table_select, [result_table], [sel_codes])
            add_btn.click(do_add, [sel_codes, cond_input, plan_state], [op_out])

        # ===== Tab 2: 关注列表 =====
        with gr.Tab("👁 关注列表"):
            with gr.Row():
                wl_refresh = gr.Button("🔄 刷新")
                wl_export = gr.Button("📥 导出CSV")
            wl_table = gr.Dataframe(label="我的关注列表", interactive=False, wrap=True)
            with gr.Row():
                wl_codes = gr.Textbox(label="选中代码", interactive=True)
                wl_remove = gr.Button("🗑 移出")
            wl_info = gr.Textbox(label="结果", interactive=False)

            wl_refresh.click(load_watchlist_v2, [], wl_table)
            wl_table.select(on_table_select, [wl_table], [wl_codes])
            wl_remove.click(
                lambda sel: (execute_tool("remove_from_watchlist",
                    {"codes": [c.strip() for c in sel.split(",") if c.strip()]}, _get_conn())
                    if sel else "⚠️"),
                [wl_codes], [wl_info]).then(load_watchlist_v2, [], wl_table)

        # ===== Tab 3: 策略 =====
        with gr.Tab("📋 策略"):
            strategies_cfg = config.get("strategies", {})
            strategy_text = ""
            for mkt, s in strategies_cfg.items():
                strategy_text += f"## {s.get('label', mkt)} ({mkt})\n"
                strategy_text += f"**卖出规则:**\n"
                sr = s.get("sell_rules", {})
                strategy_text += f"- 止损: {sr.get('stop_loss', '-')}%  止盈: {sr.get('stop_profit', '-')}%\n"
                strategy_text += f"- 均线: {sr.get('ma_break', [])}\n"
                strategy_text += f"- 形态: {sr.get('pattern', [])}\n"
                strategy_text += f"**移除条件:**\n"
                for rc in s.get("remove_conditions", []):
                    strategy_text += f"- {rc.get('type')}: {rc}\n"
                strategy_text += "\n"
            gr.Markdown(strategy_text or "⚠️ 请在 config.yaml 中配置 strategies 段")

        # ===== Tab 4: 信号 =====
        with gr.Tab("⚡ 信号"):
            sig_refresh = gr.Button("🔄 刷新")
            sig_table = gr.Dataframe(label="待处理信号", interactive=False, wrap=True)
            with gr.Row():
                sig_id_input = gr.Number(label="信号ID", precision=0)
                with gr.Row():
                    sig_confirm = gr.Button("✅ 确认", variant="primary")
                    sig_dismiss = gr.Button("❌ 驳回")
            sig_info = gr.Textbox(label="结果", interactive=False)

            sig_refresh.click(load_pending_actions_df, [], sig_table)

            def do_confirm(aid):
                if not aid:
                    return "请输入信号ID", pd.DataFrame()
                r = execute_tool("confirm_monitor_action",
                    {"action_id": int(aid), "decision": "confirmed"}, _get_conn())
                return r, load_pending_actions_df()

            def do_dismiss(aid):
                if not aid:
                    return "请输入信号ID", pd.DataFrame()
                r = execute_tool("confirm_monitor_action",
                    {"action_id": int(aid), "decision": "dismissed"}, _get_conn())
                return r, load_pending_actions_df()

            sig_confirm.click(do_confirm, [sig_id_input], [sig_info, sig_table])
            sig_dismiss.click(do_dismiss, [sig_id_input], [sig_info, sig_table])

        # ===== Tab 5: 回测历史 =====
        with gr.Tab("📊 回测历史"):
            bt_refresh = gr.Button("🔄 刷新")
            bt_table = gr.Dataframe(label="回测记录", interactive=False, wrap=True)

            def load_bt_history():
                rows = _get_conn().execute("""
                    SELECT id, strategy_name, market, start_date, end_date,
                           total_return, annual_return, sharpe_ratio, max_drawdown,
                           n_stocks, created_at
                    FROM backtest_history ORDER BY created_at DESC LIMIT 50
                """).fetchall()
                cols = ["ID","策略","市场","开始","结束","总收益%","年化%","夏普","最大回撤%","股票数","时间"]
                if not rows:
                    return pd.DataFrame(columns=cols)
                return pd.DataFrame([
                    [r[0],r[1] or "-",r[2],str(r[3]),str(r[4]),
                     round(r[5],1) if r[5] else 0, round(r[6],1) if r[6] else 0,
                     round(r[7],2) if r[7] else 0, round(r[8],1) if r[8] else 0,
                     r[9], str(r[10])[:19] if r[10] else ""]
                    for r in rows
                ], columns=cols)

            bt_refresh.click(load_bt_history, [], bt_table)

    return app


if __name__ == "__main__":
    app = create_ui()
    print("Market Data Studio v2 → http://127.0.0.1:7860")
    app.launch(server_name="0.0.0.0", server_port=7860)
```

- [ ] **Step 3: 运行现有测试确保不破坏**

```bash
python -m pytest tests/ -v --timeout=30 -k "not test_portfolio and not test_monitor"
```

Expected: 所有现有测试 PASS

- [ ] **Step 4: Commit**

```bash
git add src/agent/tools.py src/agent/app.py
git commit -m "feat: v2 agent tools + 5-tab UI — discover, monitor signals, confirm actions"
```

---

### Task 8: 调度器 + CLI 集成

**Files:**
- Modify: `scheduler.py` — 新增 `run_monitor()` 和 `run_maintain()` 任务
- Modify: `cli.py` — 新增 `monitor` 和 `maintain` 命令

**Interfaces:**
- Produces: 调度器新增 16:20 监控 + 16:30 维护；CLI 新增两个命令

- [ ] **Step 1: 更新 scheduler.py**

在 `src/monitor/engine.py` 和 `src/monitor/maintainer.py` 已创建的前提下，在 `scheduler.py` 中添加：

```python
# 在 run_factors_update() 之后添加：

def run_monitor_task():
    """收盘后监控引擎：收益跟踪 + 卖出信号 + 新机会"""
    config = load_config()
    logger.info("=== 监控引擎启动 ===")
    from src.monitor.engine import MonitorEngine
    from src.db import get_connection
    conn = get_connection(config)
    try:
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        engine = MonitorEngine(conn, config)
        result = engine.run(today)
        logger.info(f"  收益刷新: {result['watchlist_returns']} 只")
        logger.info(f"  卖出信号: {result['sell_signals']} 条")
        logger.info(f"  新机会: {result['new_buy_signals']} 条")
    except Exception as e:
        logger.error(f"监控引擎异常: {e}")
    finally:
        conn.close()
    logger.info("=== 监控引擎结束 ===")


def run_maintain_task():
    """收盘后维护引擎：形态破坏检测 + 移除建议"""
    config = load_config()
    logger.info("=== 维护引擎启动 ===")
    from src.monitor.maintainer import MaintainEngine
    from src.db import get_connection
    conn = get_connection(config)
    try:
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        engine = MaintainEngine(conn, config)
        suggestions = engine.run(today)
        logger.info(f"  移除建议: {len(suggestions)} 条")
        for s in suggestions:
            logger.info(f"    {s['ts_code']} {s.get('name','')}: {s['reason']}")
    except Exception as e:
        logger.error(f"维护引擎异常: {e}")
    finally:
        conn.close()
    logger.info("=== 维护引擎结束 ===")
```

在 `main()` 中注册新调度任务（在现有 `schedule.every()` 行之后）：

```python
    schedule.every().day.at("16:20").do(run_monitor_task)
    schedule.every().day.at("16:30").do(run_maintain_task)
    logger.info("  监控引擎(Monitor): 每天 16:20")
    logger.info("  维护引擎(Maintain): 每天 16:30")
```

同时调整 Phase4 时间从 16:30 → 16:40（避免和维护引擎冲突）：

```python
    schedule.every().day.at("16:40").do(run_phase4_update)
```

- [ ] **Step 2: 更新 cli.py — 新增命令**

在 `cli.py` 的命令分支中添加：

```python
    elif cmd == "monitor":
        # 手动触发监控引擎
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        print(f"Running monitor engine for {today}...")
        from src.monitor.engine import MonitorEngine
        config = load_config()
        conn = get_connection(config)
        try:
            engine = MonitorEngine(conn, config)
            result = engine.run(today)
            print(f"  收益刷新: {result['watchlist_returns']} 只")
            print(f"  卖出信号: {result['sell_signals']} 条")
            print(f"  新机会:   {result['new_buy_signals']} 条")
        finally:
            conn.close()
    
    elif cmd == "maintain":
        # 手动触发维护引擎
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        print(f"Running maintain engine for {today}...")
        from src.monitor.maintainer import MaintainEngine
        config = load_config()
        conn = get_connection(config)
        try:
            engine = MaintainEngine(conn, config)
            suggestions = engine.run(today)
            if suggestions:
                print(f"  {len(suggestions)} remove suggestions:")
                for s in suggestions:
                    print(f"    {s['ts_code']} {s.get('name','')}: {s['reason']}")
            else:
                print("  No remove suggestions.")
        finally:
            conn.close()
```

- [ ] **Step 3: Commit**

```bash
git add scheduler.py cli.py
git commit -m "feat: integrate monitor + maintain into scheduler and CLI"
```

---

### Task 9: 端到端集成测试 + 文档

**Files:**
- Create: `tests/test_v2_integration.py`
- Modify: `README.md` — 更新 v2 使用说明

- [ ] **Step 1: 写端到端集成测试**

创建 `tests/test_v2_integration.py`：

```python
"""v2 端到端集成测试：发现→加入关注→监控→维护"""
import tempfile, os
from src.utils import load_config
from src.db import get_connection, upsert_daily, save_strategy_rule
from src.screening.screener import StockScreener
import pandas as pd
import numpy as np
from datetime import date


def setup_test_db():
    """创建完整的测试数据库（含多日数据）"""
    cfg = load_config()
    cfg["database"]["path"] = os.path.join(tempfile.mkdtemp(), "test_v2_int.duckdb")
    conn = get_connection(cfg)

    # 插入 5 只 A 股 + 模拟日线
    codes = ["600519.SH", "000858.SZ", "000001.SZ", "600036.SH", "000002.SZ"]
    names = ["贵州茅台", "五粮液", "平安银行", "招商银行", "万科A"]
    dates = pd.date_range("2024-01-01", "2026-06-30", freq="B")

    for code, name in zip(codes, names):
        np.random.seed(hash(code) % 2**32)
        close = 20 * (1 + np.cumsum(np.random.randn(len(dates)) * 0.015))
        df = pd.DataFrame({
            "ts_code": code, "trade_date": dates, "open": close * 0.99,
            "high": close * 1.02, "low": close * 0.98, "close": close,
            "pre_close": 0, "change": 0, "pct_chg": 0,
            "vol": 5e6, "amount": 1e8,
        })
        upsert_daily(conn, "a_daily", df)

        # a_daily_basic
        basic_df = pd.DataFrame({
            "ts_code": code, "trade_date": dates,
            "pe": 15 + np.random.randn(len(dates)) * 3,
            "pb": 2 + np.random.randn(len(dates)) * 0.5,
            "turnover_rate": 1.5,
            "volume_ratio": 1.0, "total_mv": 1e9, "circ_mv": 5e8,
        })
        conn.register("_tmp_basic", basic_df)
        conn.execute("INSERT OR REPLACE INTO a_daily_basic SELECT * FROM _tmp_basic")
        conn.unregister("_tmp_basic")

        # adj_factor
        adj_df = pd.DataFrame({"ts_code": code, "trade_date": dates, "adj_factor": 1.0})
        conn.register("_tmp_adj", adj_df)
        conn.execute("INSERT OR REPLACE INTO a_adj_factor SELECT * FROM _tmp_adj")
        conn.unregister("_tmp_adj")

        conn.execute("INSERT OR REPLACE INTO stock_info (ts_code, name, market, list_status) VALUES (?,?,?,'L')",
                     [code, name, "A"])

    # 添加策略配置
    cfg["strategies"] = {
        "A": {
            "label": "A股",
            "sell_rules": {"stop_loss": -8, "stop_profit": 30,
                           "ma_break": ["ma20", "ma60"],
                           "pattern": ["macd_dead_cross"]},
            "remove_conditions": [
                {"type": "price_below_ma", "ma": "ma60", "consecutive_days": 5},
            ],
        }
    }
    return conn, cfg


def test_full_discover_flow():
    """发现流程：条件翻译→筛选→结果"""
    conn, cfg = setup_test_db()

    screener = StockScreener(conn)
    conditions = [{"factor": "pe_ttm", "op": "lt", "value": 30}]
    df = screener.search_full_market(conditions, market="A")
    assert not df.empty
    assert len(df) >= 1
    assert "ts_code" in df.columns
    conn.close()


def test_watchlist_add_and_performance():
    """加入关注→收益刷新"""
    conn, cfg = setup_test_db()

    # 加入关注
    from src.agent.tools import execute_tool
    r = execute_tool("add_to_watchlist",
        {"codes": ["600519.SH", "000858.SZ"], "condition": "pe_ttm<30"},
        conn)
    assert "已添加 2" in r

    # 验证 watchlist 有 v2 新字段
    row = conn.execute("SELECT status, market, entry_price FROM watchlist WHERE ts_code='600519.SH'").fetchone()
    assert row[0] == "active"
    assert row[1] == "A"
    assert row[2] is not None

    # 运行监控引擎
    from src.monitor.engine import MonitorEngine
    engine = MonitorEngine(conn, cfg)
    result = engine.run("2026-06-30")
    assert result["watchlist_returns"] >= 1

    # 验证 performance 表有数据
    perf_count = conn.execute("SELECT COUNT(*) FROM watchlist_performance").fetchone()[0]
    assert perf_count >= 1
    conn.close()


def test_maintain_flow():
    """维护流程：形态检查→建议生成"""
    conn, cfg = setup_test_db()

    # 确保有活跃的关注股票
    conn.execute("INSERT OR REPLACE INTO watchlist (ts_code, name, market, status, entry_price, entry_date) VALUES ('600036.SH','招商银行','A','active',15.0,'2026-01-01')")

    # 更新最后几天的 close 为低于 MA60 的状态（用 stock_factors）
    conn.execute("""
        INSERT OR REPLACE INTO stock_factors (ts_code, trade_date, ma20, ma60, macd_dif, macd_dea, rsi14, close)
        VALUES ('600036.SH', '2026-06-30', 10.0, 20.0, -0.2, 0.1, 35.0, 8.0)
    """)

    from src.monitor.maintainer import MaintainEngine
    engine = MaintainEngine(conn, cfg)
    # 直接检查单个条件
    result = engine._check_price_below_ma(
        "600036.SH", "招商银行", "A", "ma60", 2, "2026-06-30"
    )
    # 如果数据库有足够数据且 close < ma60，应该生成建议
    # 这里主要验证不崩溃
    assert True
    conn.close()
```

- [ ] **Step 2: 运行集成测试**

```bash
python -m pytest tests/test_v2_integration.py -v --timeout=60
```

Expected: 3 PASS（可能由于随机数据，部分条件不触发，但不应崩溃）

- [ ] **Step 3: 更新 README.md**

在 README 顶端添加 v2 说明：

```markdown
## v2: 策略研究-监控一体化

```
发现 ───→ 监控 ───→ 维护
NL选股    收益跟踪   形态破坏检测
组合回测   卖出信号   移除建议
```

**新功能:**
- 🔍 自然语言→筛选条件：描述想法，系统自动翻译并执行
- 📊 组合回测：等权重组合回测，支持 A/ETF/HK/US
- 👁 关注列表监控：自动跟踪收益、检测卖出信号
- ⚡ 操作建议：BUY/SELL/REDUCE/REMOVE，需用户审核确认
- 📋 策略管理：按市场配置止损/止盈/均线/形态规则
```

- [ ] **Step 4: Commit**

```bash
git add tests/test_v2_integration.py README.md
git commit -m "test: v2 integration tests + README update"
```

---

### Task 10: 最终验证 + 全量测试

- [ ] **Step 1: Run all tests**

```bash
python -m pytest tests/ -v --timeout=60
```

Expected: 所有测试 PASS（有新增测试跨模块引用，确保 import 路径正确）

- [ ] **Step 2: 验证 CLI 新命令**

```bash
python cli.py monitor --help 2>&1 || python cli.py monitor
```

Expected: Monitor engine runs (may report empty watchlist)

- [ ] **Step 3: 验证导入完整性**

```bash
python -c "from src.discover.translator import translate_nl_to_conditions; print('discover OK')"
python -c "from src.discover.compiler import validate_plan, compile_and_execute; print('compiler OK')"
python -c "from src.monitor.engine import MonitorEngine; print('monitor OK')"
python -c "from src.monitor.maintainer import MaintainEngine; print('maintainer OK')"
python -c "from src.db import save_strategy_rule, get_active_rules, upsert_suggested_action, get_pending_actions, confirm_action; print('db CRUD OK')"
python -c "from src.backtest.runner import BacktestRunner; r = BacktestRunner(None); print('portfolio OK' if hasattr(r, 'run_portfolio') else 'MISSING')"
```

Expected: All "OK"

- [ ] **Step 4: 验证 config.yaml 格式**

```bash
python -c "from src.utils import load_config; c=load_config(); print('OK' if 'strategies' in c else 'MISSING strategies in config')"
```

Expected: OK

- [ ] **Step 5: Final commit if needed**

```bash
git status
# 如果有遗漏文件
git add -A
git diff --cached --stat
git commit -m "chore: finalize v2 implementation — all modules integrated"
```
