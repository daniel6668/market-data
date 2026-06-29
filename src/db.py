"""DuckDB 数据库模块 — 连接、建表、读写操作"""
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime


def get_connection(config: dict) -> duckdb.DuckDBPyConnection:
    """获取 DuckDB 连接，自动创建数据库文件和表"""
    db_path = config["database"]["path"]
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    create_tables(conn)
    return conn


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """创建所有表（IF NOT EXISTS 保证幂等）"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_info (
            ts_code     VARCHAR NOT NULL,
            name        VARCHAR,
            market      VARCHAR NOT NULL,
            list_date   DATE,
            delist_date DATE,
            industry    VARCHAR,
            area        VARCHAR,
            exchange    VARCHAR,
            is_hs       VARCHAR,
            list_status VARCHAR,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, market)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS a_daily (
            ts_code    VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            open       DOUBLE,
            high       DOUBLE,
            low        DOUBLE,
            close      DOUBLE,
            pre_close  DOUBLE,
            change     DOUBLE,
            pct_chg    DOUBLE,
            vol        DOUBLE,
            amount     DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS a_daily_basic (
            ts_code       VARCHAR NOT NULL,
            trade_date    DATE NOT NULL,
            turnover_rate DOUBLE,
            volume_ratio  DOUBLE,
            pe            DOUBLE,
            pb            DOUBLE,
            total_mv      DOUBLE,
            circ_mv       DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS a_adj_factor (
            ts_code    VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            adj_factor DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS etf_daily (
            ts_code       VARCHAR NOT NULL,
            trade_date    DATE NOT NULL,
            open          DOUBLE,
            high          DOUBLE,
            low           DOUBLE,
            close         DOUBLE,
            vol           DOUBLE,
            amount        DOUBLE,
            iopv          DOUBLE,
            discount_rate DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hk_daily (
            ts_code    VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            open       DOUBLE,
            high       DOUBLE,
            low        DOUBLE,
            close      DOUBLE,
            vol        DOUBLE,
            amount     DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS us_daily (
            ts_code    VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            open       DOUBLE,
            high       DOUBLE,
            low        DOUBLE,
            close      DOUBLE,
            adj_close  DOUBLE,
            volume     BIGINT,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_status (
            ts_code     VARCHAR NOT NULL,
            market      VARCHAR NOT NULL,
            last_sync   DATE,
            first_date  DATE,
            row_count   INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            last_error  TEXT,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, market)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_calendar (
            cal_date  DATE NOT NULL,
            is_open   BOOLEAN NOT NULL,
            exchange  VARCHAR DEFAULT 'SSE',
            PRIMARY KEY (cal_date, exchange)
        )
    """)
    # Phase 4 新增表
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_fund_flow (
            ts_code    VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            main_net   DOUBLE,
            small_net  DOUBLE,
            mid_net    DOUBLE,
            large_net  DOUBLE,
            super_net  DOUBLE,
            main_pct   DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS research_reports (
            info_code   VARCHAR PRIMARY KEY,
            ts_code     VARCHAR NOT NULL,
            publish_date DATE,
            org_name    VARCHAR,
            title       TEXT,
            eps_2026    DOUBLE,
            eps_2027    DOUBLE,
            eps_2028    DOUBLE,
            rating      VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS financial_reports (
            ts_code  VARCHAR NOT NULL,
            period   VARCHAR NOT NULL,
            rpt_type VARCHAR NOT NULL,
            data     TEXT,         -- JSON 存储各行项
            PRIMARY KEY (ts_code, period, rpt_type)
        )
    """)
    # ── Phase 1: 数据补全 8 张新表 ──
    conn.execute("""
        CREATE TABLE IF NOT EXISTS northbound_flow (
            date    DATE PRIMARY KEY,
            hgt_yi  DOUBLE,
            sgt_yi  DOUBLE
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS margin_trading (
            ts_code    VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            rzye       DOUBLE,
            rzmre      DOUBLE,
            rzche      DOUBLE,
            rqye       DOUBLE,
            rqmcl      DOUBLE,
            rqchl      DOUBLE,
            rzrqye     DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dragon_tiger (
            ts_code      VARCHAR NOT NULL,
            trade_date   DATE NOT NULL,
            reason       VARCHAR,
            net_buy_wan  DOUBLE,
            turnover_pct DOUBLE,
            close        DOUBLE,
            change_pct   DOUBLE,
            buy_seats    TEXT,
            sell_seats   TEXT,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS block_trade (
            ts_code     VARCHAR NOT NULL,
            trade_date  DATE NOT NULL,
            deal_price  DOUBLE,
            close       DOUBLE,
            premium_pct DOUBLE,
            deal_vol    DOUBLE,
            deal_amt    DOUBLE,
            buyer       VARCHAR,
            seller      VARCHAR,
            PRIMARY KEY (ts_code, trade_date, deal_price)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS holder_num (
            ts_code          VARCHAR NOT NULL,
            end_date         DATE NOT NULL,
            holder_num       INTEGER,
            change_num       INTEGER,
            change_ratio_pct DOUBLE,
            avg_free_shares  DOUBLE,
            PRIMARY KEY (ts_code, end_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dividend (
            ts_code        VARCHAR NOT NULL,
            ex_date        DATE NOT NULL,
            bonus_rmb      DOUBLE,
            transfer_ratio DOUBLE,
            bonus_ratio    DOUBLE,
            plan           VARCHAR,
            PRIMARY KEY (ts_code, ex_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lockup_expiry (
            ts_code     VARCHAR NOT NULL,
            free_date   DATE NOT NULL,
            stock_type  VARCHAR,
            free_shares DOUBLE,
            free_ratio  DOUBLE,
            PRIMARY KEY (ts_code, free_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_boards (
            ts_code     VARCHAR NOT NULL,
            board_name  VARCHAR NOT NULL,
            board_code  VARCHAR,
            change_pct  DOUBLE,
            lead_stock  VARCHAR,
            PRIMARY KEY (ts_code, board_name)
        )
    """)


def upsert_daily(conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> int:
    """批量插/更新日线数据。返回实际写入行数。"""
    if df.empty:
        return 0
    # 不修改传入的 DataFrame，避免副作用
    df = df.copy()
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    conn.register("_tmp_upsert", df)
    cols = ", ".join(df.columns)
    # DuckDB supports INSERT OR REPLACE
    try:
        conn.execute(f"INSERT OR REPLACE INTO {table} ({cols}) SELECT * FROM _tmp_upsert")
    except Exception:
        # fallback: DELETE + INSERT，用临时表避免 SQL 拼接
        pk_cols = ["ts_code", "trade_date"]
        if all(c in df.columns for c in pk_cols):
            conn.register("_tmp_del_keys", df[pk_cols].drop_duplicates())
            conn.execute(
                f"DELETE FROM {table} WHERE ts_code IN "
                f"(SELECT ts_code FROM _tmp_del_keys) AND trade_date IN "
                f"(SELECT trade_date FROM _tmp_del_keys)"
            )
            conn.unregister("_tmp_del_keys")
        conn.execute(f"INSERT INTO {table} ({cols}) SELECT * FROM _tmp_upsert")
    conn.unregister("_tmp_upsert")
    return len(df)


def update_stock_info(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """批量更新股票基本信息。"""
    if df.empty:
        return 0
    conn.register("_tmp_info", df)
    cols = ", ".join(df.columns)
    conn.execute(f"INSERT OR REPLACE INTO stock_info ({cols}) SELECT * FROM _tmp_info")
    conn.unregister("_tmp_info")
    return len(df)


def get_sync_status(conn: duckdb.DuckDBPyConnection, market: str) -> pd.DataFrame:
    """获取指定市场的同步状态"""
    return conn.execute(
        "SELECT ts_code, last_sync, first_date, row_count, error_count "
        "FROM sync_status WHERE market = ?", [market]
    ).fetchdf()


def update_sync_status(conn: duckdb.DuckDBPyConnection,
                       ts_code: str, market: str,
                       last_date: str, rows: int) -> None:
    """更新单只股票的同步状态

    首次同步时设置 first_date = last_date；后续只更新 last_sync/row_count，
    不影响 first_date/error_count/last_error。
    """
    conn.execute("""
        INSERT INTO sync_status (ts_code, market, last_sync, first_date, row_count, updated_at)
        VALUES (?, ?, ?, ?, ?, now())
        ON CONFLICT (ts_code, market) DO UPDATE SET
            last_sync = excluded.last_sync,
            row_count = excluded.row_count,
            updated_at = now()
    """, [ts_code, market, last_date, last_date, rows])


def record_sync_error(conn: duckdb.DuckDBPyConnection,
                      ts_code: str, market: str, error_msg: str) -> None:
    """记录同步错误"""
    conn.execute("""
        INSERT INTO sync_status (ts_code, market, error_count, last_error, updated_at)
        VALUES (?, ?, 1, ?, now())
        ON CONFLICT (ts_code, market) DO UPDATE SET
            error_count = sync_status.error_count + 1,
            last_error = ?,
            updated_at = now()
    """, [ts_code, market, error_msg, error_msg])


def get_stocks_needing_update(conn: duckdb.DuckDBPyConnection,
                               market: str, start_date: str,
                               max_errors: int = 10) -> pd.DataFrame:
    """获取需要更新的股票列表"""
    return conn.execute("""
        SELECT ts_code, last_sync FROM sync_status
        WHERE market = ?
          AND (last_sync IS NULL OR last_sync < ?)
          AND error_count < ?
        ORDER BY last_sync NULLS FIRST
    """, [market, start_date, max_errors]).fetchdf()


def save_trade_calendar(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """批量写入交易日历数据"""
    if df.empty:
        return 0
    df = df.copy()
    if "cal_date" in df.columns:
        df["cal_date"] = pd.to_datetime(df["cal_date"]).dt.date
    conn.register("_tmp_cal", df)
    conn.execute("INSERT OR REPLACE INTO trade_calendar SELECT * FROM _tmp_cal")
    conn.unregister("_tmp_cal")
    return len(df)


def get_trading_days(conn: duckdb.DuckDBPyConnection,
                     start_date: str, end_date: str,
                     exchange: str = "SSE") -> list:
    """查询交易日列表"""
    return [str(r[0]) for r in conn.execute(
        "SELECT cal_date FROM trade_calendar "
        "WHERE exchange = ? AND is_open = True AND cal_date >= ? AND cal_date <= ? "
        "ORDER BY cal_date",
        [exchange, start_date, end_date]
    ).fetchall()]


def has_trade_calendar(conn: duckdb.DuckDBPyConnection, exchange: str = "SSE") -> bool:
    """检查是否已有交易日历数据"""
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM trade_calendar WHERE exchange = ?", [exchange]
        ).fetchone()[0] > 0
    except Exception:
        return False


# ── Phase 4: 资金流 / 研报 / 财报 CRUD ──

def upsert_fund_flow(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """批量写入资金流数据"""
    if df.empty:
        return 0
    df = df.copy()
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    conn.register("_tmp_ff", df)
    cols = ", ".join(df.columns)
    conn.execute(f"INSERT OR REPLACE INTO stock_fund_flow ({cols}) SELECT * FROM _tmp_ff")
    conn.unregister("_tmp_ff")
    return len(df)


def upsert_research_reports(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """批量写入研报数据（info_code 为 key，自动去重）"""
    if df.empty:
        return 0
    df = df.copy()
    conn.register("_tmp_rpt", df)
    cols = ", ".join(df.columns)
    conn.execute(f"INSERT OR REPLACE INTO research_reports ({cols}) SELECT * FROM _tmp_rpt")
    conn.unregister("_tmp_rpt")
    return len(df)


def upsert_financial_reports(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """写入财报数据（JSON 序列化）"""
    if df.empty:
        return 0
    import json
    df = df.copy()
    # 除 ts_code/period/rpt_type 外，其余列序列化为 JSON
    meta_cols = ["ts_code", "period", "rpt_type"]
    data_cols = [c for c in df.columns if c not in meta_cols]
    if data_cols:
        df["data"] = df[data_cols].apply(lambda r: json.dumps(r.to_dict(), ensure_ascii=False), axis=1)
    keep = [c for c in meta_cols + ["data"] if c in df.columns]
    conn.register("_tmp_fr", df[keep])
    cols = ", ".join(keep)
    conn.execute(f"INSERT OR REPLACE INTO financial_reports ({cols}) SELECT * FROM _tmp_fr")
    conn.unregister("_tmp_fr")
    return len(df)
