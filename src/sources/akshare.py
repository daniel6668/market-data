"""AKShare 数据源 — ETF + A 股补充"""
import akshare as ak
import pandas as pd
from ..utils import RateLimiter
from .base import DataSource


class AKShareSource(DataSource):
    """封装 AKShare API，内置限速"""
    
    def __init__(self, config: dict):
        rl_cfg = config["rate_limit"]["akshare"]
        self.limiter = RateLimiter(max_calls=rl_cfg["max_calls"], period=rl_cfg["period"])

    def _wait(self):
        self.limiter.wait()

    def get_etf_list(self) -> pd.DataFrame:
        """获取 ETF 列表"""
        self._wait()
        df = ak.fund_etf_spot_em()
        # 重命名列对齐 stock_info
        result = pd.DataFrame()
        if not df.empty:
            result["ts_code"] = df["代码"]
            result["name"] = df["名称"]
            result["market"] = "ETF"
            result["list_date"] = pd.to_datetime(df.get("数据日期", pd.NaT), errors="coerce").dt.date
            result["industry"] = "ETF"
            result["area"] = "CN"
            result["exchange"] = "SH/SZ"
            result["is_hs"] = None
            result["list_status"] = "L"
            result["delist_date"] = None
        return result

    def get_etf_daily(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取 ETF 日线数据
        
        返回字段: ts_code, trade_date, open, high, low, close, vol, amount
        """
        self._wait()
        df = ak.fund_etf_hist_em(symbol=code, period="daily",
                                 start_date=start_date.replace("-", ""),
                                 end_date=end_date.replace("-", ""))
        if df.empty:
            return pd.DataFrame()
        return pd.DataFrame({
            "ts_code": code,
            "trade_date": pd.to_datetime(df["日期"]).dt.date,
            "open": pd.to_numeric(df["开盘"], errors="coerce"),
            "high": pd.to_numeric(df["最高"], errors="coerce"),
            "low": pd.to_numeric(df["最低"], errors="coerce"),
            "close": pd.to_numeric(df["收盘"], errors="coerce"),
            "vol": pd.to_numeric(df["成交量"], errors="coerce"),
            "amount": pd.to_numeric(df["成交额"], errors="coerce"),
        })

    def get_a_stock_list(self) -> pd.DataFrame:
        """获取 A 股列表（作为 Tushare 的补充/备用）"""
        self._wait()
        df = ak.stock_info_a_code_name()
        result = pd.DataFrame()
        if not df.empty:
            result["ts_code"] = df["code"]
            result["name"] = df["name"]
            result["market"] = "A"
        return result

    def get_hk_stock_list(self) -> pd.DataFrame:
        """获取港股列表（作为 Tushare 的备选）"""
        self._wait()
        df = ak.stock_hk_spot_em()
        result = pd.DataFrame()
        if not df.empty:
            result["ts_code"] = df["代码"].astype(str) + ".HK"
            result["name"] = df["名称"]
            result["market"] = "HK"
            result["list_date"] = None
            result["delist_date"] = None
            result["industry"] = None
            result["area"] = "HK"
            result["exchange"] = "HKEX"
            result["is_hs"] = None
            result["list_status"] = "L"
        return result
