"""ETL Pipeline — 编排数据采集全流程"""
import logging
import socket
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
from .utils import load_config, setup_logger, TradingCalendar
from .validator import validate_daily


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

        # 全局网络超时：防止 AKShare/yfinance 请求无限等待
        socket.setdefaulttimeout(60)

        # 懒加载数据源
        self._ts = None
        self._ak = None
        self._yf = None
        self._mdx = None      # mootdx (A 股日线首选)
        self._tc = None       # Tencent (PE/PB/市值)
        self._em = None       # EastMoney (资金流/研报/行业)
        self._sina = None     # Sina (财报三表)

        # 加载交易日历（如果数据库有数据，否则 fallback 到周末检查）
        TradingCalendar.load_from_db(self.conn)

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

    @property
    def mdx(self):
        """mootdx 数据源 — A 股日线首选（不限频、零 Token）"""
        if self._mdx is None:
            from .sources.mootdx_source import MootdxSource
            try:
                self._mdx = MootdxSource()
            except RuntimeError as e:
                self.logger.warning(f"mootdx 不可用: {e}, 将使用 Tushare")
                self._mdx = None
        return self._mdx

    @property
    def tc(self):
        """腾讯财经数据源 — PE/PB/市值（不限频、零 Token）"""
        if self._tc is None:
            from .sources.tencent_source import TencentSource
            self._tc = TencentSource()
        return self._tc

    @property
    def em(self):
        """东财数据源 — 资金流/研报/行业排名（push2 系列，已内置限流）"""
        if self._em is None:
            from .sources.eastmoney_source import EastMoneySource
            self._em = EastMoneySource()
        return self._em

    @property
    def sina(self):
        """新浪财经数据源 — 财报三表（利润表/资产负债表/现金流）"""
        if self._sina is None:
            from .sources.sina_source import SinaSource
            self._sina = SinaSource()
        return self._sina

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
                    if rows > 0:
                        success += 1
                    else:
                        failed += 1
                    progress.update(task, advance=1,
                                   description=f"[cyan]{ts_code} ({success+failed+1}/{total})")
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
                if last_sync is None or pd.isna(last_sync):
                    start = self.start_date
                else:
                    # 兜底：如果 DuckDB 返回字符串，先转 date
                    if isinstance(last_sync, str):
                        last_sync = pd.to_datetime(last_sync).date()
                    start = (last_sync + timedelta(days=1)).strftime("%Y-%m-%d")

                try:
                    rows = self._fetch_daily_with_retry(market, ts_code, start, today)
                    if rows > 0:
                        success += 1
                    else:
                        failed += 1
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
                    if rows > 0:
                        success += 1
                    else:
                        failed += 1
                except Exception as e:
                    failed += 1
                    errors.append({"ts_code": ts_code, "error": str(e)})
                progress.update(task, advance=1)

        return {"total": total, "success": success, "failed": failed, "errors": errors}

    # ---- 内部方法 ----

    def _get_stock_list(self, market: str) -> pd.DataFrame:
        """根据市场获取股票列表"""
        if market == "A":
            try:
                df = self.ts.get_stock_list("A")
                if not df.empty:
                    return df
            except Exception as e:
                self.logger.warning(f"Tushare stock_basic 失败, 用 AKShare 替代: {e}")
            # AKShare fallback — 添加交易所后缀
            df = self.ak.get_a_stock_list()
            if df.empty:
                return df
            # 补全 ts_code 后缀
            def add_suffix(code):
                code = str(code).zfill(6)
                if code.startswith('688'): return code + '.SH'  # 科创板
                if code.startswith('8'): return code + '.BJ'    # 北交所 8xxx
                if code.startswith('4'): return code + '.BJ'    # 北交所 4xxx
                if code.startswith('6'): return code + '.SH'    # 上证主板
                return code + '.SZ'  # 0/2/3 → 深交所
            df["ts_code"] = df["ts_code"].apply(add_suffix)
            df["exchange"] = df["ts_code"].str.split(".").str[1]
            df["list_status"] = "L"
            df["is_hs"] = None
            return df
        elif market == "HK":
            try:
                df = self.ts.get_stock_list("HK")
                if not df.empty:
                    return df
            except Exception as e:
                self.logger.warning(f"Tushare HK 列表失败, 用 AKShare 替代: {e}")
            df = self.ak.get_hk_stock_list()
        elif market == "ETF":
            try:
                df = self.ak.get_etf_list()
                if not df.empty:
                    return df
            except Exception as e:
                self.logger.warning(f"AKShare ETF 列表失败: {e}")
            return pd.DataFrame()
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
        last_error = None

        for attempt in range(self.max_retries):
            try:
                data = self._fetch_daily(market, ts_code, start, end)
                daily_df = data["daily"]
                if daily_df.empty:
                    return 0

                # 数据校验：写入前检查行数/空值/日期连续性
                validation = validate_daily(daily_df, ts_code)
                if not validation["valid"]:
                    self.logger.warning(f"数据校验失败 {ts_code}: {validation['issues']}")
                    return 0

                table = self._daily_table(market)
                # 事务写入：daily + extra 表原子性
                self.conn.execute("BEGIN TRANSACTION")
                try:
                    rows = upsert_daily(self.conn, table, daily_df)
                    for extra_table, extra_df in data.get("extra", []):
                        if not extra_df.empty:
                            upsert_daily(self.conn, extra_table, extra_df)
                    self.conn.execute("COMMIT")
                except Exception:
                    self.conn.execute("ROLLBACK")
                    raise

                # 更新 sync_status（事务外，独立于数据写入）
                max_date = daily_df["trade_date"].max()
                update_sync_status(self.conn, ts_code, market, str(max_date), len(daily_df))
                return rows

            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    msg = str(e)
                    is_rate_limit = any(kw in msg for kw in
                                        ["频率超限", "Rate", "Too Many", "limit"])
                    if is_rate_limit:
                        wait = 60  # 限流错误等待一个完整周期
                    else:
                        wait = self.backoff * (2 ** attempt)
                    self.logger.warning(f"重试 {attempt+1}/{self.max_retries} for {ts_code}. "
                                        f"等待 {wait}s... ({'限流' if is_rate_limit else '错误'})")
                    time.sleep(wait)

        raise last_error or Exception(f"Max retries exceeded for {ts_code}")

    def _fetch_daily(self, market: str, ts_code: str,
                     start: str, end: str) -> dict:
        """根据市场拉取日线

        Returns: {"daily": DataFrame, "extra": [(table_name, DataFrame), ...]}
        extra 中的 DataFrame 由调用方在同一事务中写入。
        """
        if market == "A":
            # A 股日线：mootdx 优先（不限频），失败时降级到 Tushare
            df = pd.DataFrame()
            mdx = self.mdx
            if mdx is not None:
                df = mdx.get_daily(ts_code, start, end)
            if df.empty:
                df = self.ts.get_daily(ts_code, start, end)
            if not df.empty:
                df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
            extra = []
            # 基本面：尝试腾讯财经（实时数据），fallback 到 Tushare
            try:
                basic = self.tc.get_daily_basic(ts_code)
                if basic.empty:
                    basic = self.ts.get_daily_basic(ts_code, start, end)
                if not basic.empty:
                    basic["trade_date"] = pd.to_datetime(basic["trade_date"]).dt.date
                    extra.append(("a_daily_basic", basic))
            except Exception as e:
                try:
                    basic = self.ts.get_daily_basic(ts_code, start, end)
                    if not basic.empty:
                        basic["trade_date"] = pd.to_datetime(basic["trade_date"]).dt.date
                        extra.append(("a_daily_basic", basic))
                except Exception:
                    self.logger.debug(f"daily_basic 拉取失败 {ts_code}: {e}")
            try:
                adj = self.ts.get_adj_factor(ts_code, start, end)
                if not adj.empty:
                    adj["trade_date"] = pd.to_datetime(adj["trade_date"]).dt.date
                    extra.append(("a_adj_factor", adj))
            except Exception as e:
                self.logger.debug(f"adj_factor 拉取失败 {ts_code}: {e}")
            return {"daily": df, "extra": extra}

        elif market == "ETF":
            return {"daily": self.ak.get_etf_daily(ts_code, start, end), "extra": []}

        elif market == "HK":
            df = self.ts.get_hk_daily(ts_code, start, end)
            if not df.empty:
                df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
            return {"daily": df, "extra": []}

        elif market == "US":
            return {"daily": self.yf.get_us_daily(ts_code, start, end), "extra": []}

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
