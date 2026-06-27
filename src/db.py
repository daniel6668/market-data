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


def upsert_daily(conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> int:
    """批量插/更新日线数据。返回实际写入行数。"""
    if df.empty:
        return 0
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    conn.register("_tmp_upsert", df)
    cols = ", ".join(df.columns)
    # DuckDB supports INSERT OR REPLACE
    try:
        conn.execute(f"INSERT OR REPLACE INTO {table} ({cols}) SELECT * FROM _tmp_upsert")
    except Exception:
        pk_cols = ["ts_code", "trade_date"]
        if all(c in df.columns for c in pk_cols):
            codes = df["ts_code"].unique().tolist()
            dates = df["trade_date"].unique().tolist()
            codes_str = ", ".join([f"'{c}'" for c in codes])
            dates_str = ", ".join([f"'{d}'" for d in dates])
            conn.execute(f"DELETE FROM {table} WHERE ts_code IN ({codes_str}) AND trade_date IN ({dates_str})")
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
    """更新单只股票的同步状态"""
    conn.execute("""
        INSERT OR REPLACE INTO sync_status (ts_code, market, last_sync, row_count, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, [ts_code, market, last_date, rows])


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
