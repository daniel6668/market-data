"""Tushare Pro 数据源 — A股 + 港股"""
import os
import pandas as pd
import tushare as ts
from ..utils import RateLimiter
from .base import DataSource


class TushareSource(DataSource):
    """封装 Tushare Pro API，内置接口级限速

    Tushare 不同接口有不同的频率限制：
    - stock_basic / hk_basic: 1 次/小时
    - daily / daily_basic / adj_factor / hk_daily: 50 次/分钟（配置 45 留余量）
    """

    # 接口名 → 配置 key 映射
    _API_GROUPS = {
        "stock_basic": "stock_basic",
        "hk_basic": "stock_basic",
        "daily": "daily",
        "daily_basic": "daily",
        "adj_factor": "daily",
        "hk_daily": "daily",
        "trade_cal": "stock_basic",
    }

    def __init__(self, config: dict):
        token = config["tushare"].get("token") or os.environ.get("TUSHARE_TOKEN", "")
        ts.set_token(token)
        self.pro = ts.pro_api()
        rl_cfg = config["rate_limit"]["tushare"]

        # 默认限速器（向后兼容，无接口级配置时使用）
        self._default_limiter = RateLimiter(
            max_calls=rl_cfg["max_calls"], period=rl_cfg["period"]
        )

        # 按配置 key 建立限速器（stock_basic / daily / daily_basic / adj_factor）
        self._group_limiters = {}
        for group in set(self._API_GROUPS.values()):
            if group in rl_cfg and isinstance(rl_cfg[group], dict):
                self._group_limiters[group] = RateLimiter(
                    max_calls=rl_cfg[group]["max_calls"],
                    period=rl_cfg[group]["period"],
                )

        self._stock_list_cache = None

    def _call(self, func, api_name: str = "default", **kwargs) -> pd.DataFrame:
        """带接口级限速的 API 调用"""
        group = self._API_GROUPS.get(api_name)
        if group and group in self._group_limiters:
            limiter = self._group_limiters[group]
        else:
            limiter = self._default_limiter
        limiter.wait()
        result = func(**kwargs)
        return result if not result.empty else pd.DataFrame()

    @property
    def supports_extra(self) -> bool:
        """Tushare 支持 daily_basic 和 adj_factor 额外数据"""
        return True

    def get_stock_list(self, market: str = "A") -> pd.DataFrame:
        """获取股票列表

        market: 'A' 获取A股，'HK' 获取港股
        返回 DataFrame，列对齐 stock_info 表结构
        """
        if market == "A":
            df = self._call(self.pro.stock_basic,
                           api_name="stock_basic",
                           exchange='',
                           list_status='L',
                           fields='ts_code,symbol,name,area,industry,list_date,delist_date,is_hs')
            if not df.empty:
                df["market"] = "A"
                df["exchange"] = df["ts_code"].str.split(".").str[1]
                if "symbol" in df.columns:
                    df = df.drop(columns=["symbol"])
        elif market == "HK":
            df = self._call(self.pro.hk_basic,
                           api_name="hk_basic",
                           fields='ts_code,name,list_date,delist_date')
            if not df.empty:
                df["market"] = "HK"
                df["industry"] = None
                df["area"] = "HK"
                df["exchange"] = "HKEX"
                df["is_hs"] = None
                df["list_status"] = "L"
        else:
            raise ValueError(f"Unknown market: {market}")
        return df

    def get_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取 A 股日线数据 (OHLCV)

        返回字段: ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
        """
        return self._call(
            self.pro.daily,
            api_name="daily",
            ts_code=ts_code,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", "")
        )

    def get_daily_basic(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取 A 股日线基本面（PE/PB/市值/换手率等）

        返回字段: ts_code, trade_date, turnover_rate, volume_ratio, pe, pb, total_mv, circ_mv
        """
        return self._call(
            self.pro.daily_basic,
            api_name="daily_basic",
            ts_code=ts_code,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", "")
        )

    def get_adj_factor(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取复权因子"""
        return self._call(
            self.pro.adj_factor,
            api_name="adj_factor",
            ts_code=ts_code,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", "")
        )

    def get_hk_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取港股日线"""
        return self._call(
            self.pro.hk_daily,
            api_name="hk_daily",
            ts_code=ts_code,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", "")
        )

    def get_trade_calendar(self, exchange: str = "SSE",
                           start_date: str = "20150101",
                           end_date: str = "20991231") -> pd.DataFrame:
        """获取交易日历"""
        return self._call(
            self.pro.trade_cal,
            api_name="trade_cal",
            exchange=exchange,
            start_date=start_date,
            end_date=end_date
        )
