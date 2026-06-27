"""ETL Pipeline — 编排数据采集全流程"""
import logging
import time
from datetime import datetime, timedelta

import pandas as pd
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn

from .db import (
    get_connection, update_stock_info, upsert_daily,
    update_sync_status, record_sync_error,
    get_stocks_needing_update
)
from .sources.tushare import TushareSource
from .sources.akshare import AKShareSource
from .sources.yfinance import YFinanceSource
from .utils import load_config, setup_logger


logger = logging.getLogger("market_data")


class Pipeline:
    """ETL 主流程"""

    def __init__(self, config: dict = None):
        self.config = config or load_config()
        self.logger = setup_logger(self.config)
        self.conn = get_connection(self.config)
        self.start_date = self.config["data"]["start_date"]
        self.max_retries = self.config["retry"]["max_attempts"]
        self.backoff = self.config["retry"]["backoff_base"]

        # 懒加载数据源
        self._ts = None
        self._ak = None
        self._yf = None

    @property
    def ts(self) -> TushareSource:
        if self._ts is None:
            self._ts = TushareSource(self.config)
        return self._ts

    @property
    def ak(self) -> AKShareSource:
        if self._ak is None:
            self._ak = AKShareSource(self.config)
        return self._ak

    @property
    def yf(self) -> YFinanceSource:
        if self._yf is None:
            self._yf = YFinanceSource(self.config)
        return self._yf

    def init_market(self, market: str) -> dict:
        """初始化一个市场：拉股票列表 + 全部历史数据

        Returns: {"total": N, "success": N, "failed": N, "errors": [...]}
        """
        self.logger.info(f"=== 初始化市场: {market} ===")

        # Step 1: 获取股票列表
        self.logger.info(f"获取 {market} 股票列表...")
        stock_list = self._get_stock_list(market)
        self.logger.info(f"获取到 {len(stock_list)} 只")

        # 写入 stock_info
        if not stock_list.empty:
            update_stock_info(self.conn, stock_list)

        # Step 2: 逐只拉取历史数据
        codes = stock_list["ts_code"].tolist()
        total = len(codes)
        success = 0
        failed = 0
        errors = []

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task(f"[cyan]拉取 {market} 日线...", total=total)

            for ts_code in codes:
                try:
                    rows = self._fetch_daily_with_retry(market, ts_code,
                                                        self.start_date,
                                                        datetime.now().strftime("%Y-%m-%d"))
                    progress.update(task, advance=1,
                                   description=f"[cyan]{ts_code} ({success+failed+1}/{total})")
                    success += 1
                except Exception as e:
                    self.logger.error(f"失败 {ts_code}: {e}")
                    record_sync_error(self.conn, ts_code, market, str(e))
                    failed += 1
                    errors.append({"ts_code": ts_code, "error": str(e)})
                    progress.update(task, advance=1)

        self.logger.info(f"完成: {market} — 成功 {success}/{total}，失败 {failed}")
        return {"total": total, "success": success, "failed": failed, "errors": errors}

    def update_market(self, market: str) -> dict:
        """增量更新市场"""
        self.logger.info(f"=== 增量更新市场: {market} ===")

        today = datetime.now().strftime("%Y-%m-%d")
        stocks = get_stocks_needing_update(self.conn, market, today)

        if stocks.empty:
            self.logger.info(f"没有需要更新的 {market} 股票")
            return {"total": 0, "success": 0, "failed": 0, "errors": []}

        total = len(stocks)
        success = 0
        failed = 0
        errors = []

        with Progress() as progress:
            task = progress.add_task(f"[green]更新 {market}...", total=total)

            for _, row in stocks.iterrows():
                ts_code = row["ts_code"]
                last_sync = row.get("last_sync")
                start = (last_sync + timedelta(days=1)).strftime("%Y-%m-%d") if last_sync else self.start_date

                try:
                    rows = self._fetch_daily_with_retry(market, ts_code, start, today)
                    success += 1
                except Exception as e:
                    self.logger.error(f"更新失败 {ts_code}: {e}")
                    record_sync_error(self.conn, ts_code, market, str(e))
                    failed += 1
                    errors.append({"ts_code": ts_code, "error": str(e)})

                progress.update(task, advance=1)

        return {"total": total, "success": success, "failed": failed, "errors": errors}

    def backfill_market(self, market: str, start: str, end: str) -> dict:
        """回补指定日期范围数据"""
        self.logger.info(f"=== 回补 {market}: {start} ~ {end} ===")

        stock_list = self._get_stock_list(market)
        codes = stock_list["ts_code"].tolist()
        total = len(codes)
        success = 0
        failed = 0
        errors = []

        with Progress() as progress:
            task = progress.add_task(f"[yellow]回补 {market}...", total=total)

            for ts_code in codes:
                try:
                    rows = self._fetch_daily_with_retry(market, ts_code, start, end)
                    success += 1
                except Exception as e:
                    failed += 1
                    errors.append({"ts_code": ts_code, "error": str(e)})
                progress.update(task, advance=1)

        return {"total": total, "success": success, "failed": failed, "errors": errors}

    # ---- 内部方法 ----

    def _get_stock_list(self, market: str) -> pd.DataFrame:
        """根据市场获取股票列表"""
        if market == "A":
            df = self.ts.get_stock_list("A")
        elif market == "HK":
            df = self.ts.get_stock_list("HK")
        elif market == "ETF":
            df = self.ak.get_etf_list()
        elif market == "US":
            df = self.yf.get_us_stock_list()
        else:
            raise ValueError(f"Unknown market: {market}")
        return df

    def _fetch_daily_with_retry(self, market: str, ts_code: str,
                                 start: str, end: str) -> int:
        """拉取单只股票日线，带重试

        Returns: 写入行数
        """
        import time
        last_error = None

        for attempt in range(self.max_retries):
            try:
                df = self._fetch_daily(market, ts_code, start, end)
                if df.empty:
                    return 0

                table = self._daily_table(market)
                rows = upsert_daily(self.conn, table, df)

                # 更新 sync_status
                max_date = df["trade_date"].max()
                update_sync_status(self.conn, ts_code, market, str(max_date), len(df))
                return rows

            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    wait = self.backoff * (2 ** attempt)
                    self.logger.warning(f"重试 {attempt+1}/{self.max_retries} for {ts_code}. 等待 {wait}s...")
                    time.sleep(wait)

        raise last_error or Exception(f"Max retries exceeded for {ts_code}")

    def _fetch_daily(self, market: str, ts_code: str, start: str, end: str) -> pd.DataFrame:
        """根据市场拉取日线"""
        if market == "A":
            df = self.ts.get_daily(ts_code, start, end)
            if not df.empty:
                df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
            # 同时拉基本面和复权因子
            try:
                basic = self.ts.get_daily_basic(ts_code, start, end)
                if not basic.empty:
                    basic["trade_date"] = pd.to_datetime(basic["trade_date"]).dt.date
                    upsert_daily(self.conn, "a_daily_basic", basic)
            except Exception as e:
                self.logger.debug(f"daily_basic 拉取失败 {ts_code}: {e}")
            try:
                adj = self.ts.get_adj_factor(ts_code, start, end)
                if not adj.empty:
                    adj["trade_date"] = pd.to_datetime(adj["trade_date"]).dt.date
                    upsert_daily(self.conn, "a_adj_factor", adj)
            except Exception as e:
                self.logger.debug(f"adj_factor 拉取失败 {ts_code}: {e}")
            return df

        elif market == "ETF":
            return self.ak.get_etf_daily(ts_code, start, end)

        elif market == "HK":
            df = self.ts.get_hk_daily(ts_code, start, end)
            if not df.empty:
                df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
            return df

        elif market == "US":
            return self.yf.get_us_daily(ts_code, start, end)

        else:
            raise ValueError(f"Unknown market: {market}")

    def _daily_table(self, market: str) -> str:
        """市场 → 表名映射"""
        return {
            "A": "a_daily",
            "ETF": "etf_daily",
            "HK": "hk_daily",
            "US": "us_daily",
        }[market]

    def close(self):
        """关闭数据库连接"""
        self.conn.close()
