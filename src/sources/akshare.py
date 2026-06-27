"""AKShare 数据源 — ETF + A 股补充"""
import akshare as ak
import pandas as pd
from ..utils import RateLimiter


class AKShareSource:
    """封装 AKShare API，内置限速"""
    
    def __init__(self, config: dict):
        rl_cfg = config["rate_limit"]["akshare"]
        self.limiter = RateLimiter(max_calls=rl_cfg["max_calls"], period=rl_cfg["period"])

    def _wait(self):
        self.limiter.wait()

    def get_etf_list(self) -> pd.DataFrame:
        """获取 ETF 列表"""
        self._wait()
        df = ak.fund_etf_fund_info_em()
        # 重命名列对齐 stock_info
        result = pd.DataFrame()
        if not df.empty:
            result["ts_code"] = df["基金代码"]
            result["name"] = df["基金简称"]
            result["market"] = "ETF"
            result["list_date"] = pd.to_datetime(df.get("上市日期", pd.NaT), errors="coerce").dt.date
            result["industry"] = "ETF"
            result["area"] = "CN"
            result["exchange"] = df.get("上市地", "SH/SZ")
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
        result = pd.DataFrame()
        if not df.empty:
            result["ts_code"] = code
            result["trade_date"] = pd.to_datetime(df["日期"]).dt.date
            result["open"] = pd.to_numeric(df["开盘"], errors="coerce")
            result["high"] = pd.to_numeric(df["最高"], errors="coerce")
            result["low"] = pd.to_numeric(df["最低"], errors="coerce")
            result["close"] = pd.to_numeric(df["收盘"], errors="coerce")
            result["vol"] = pd.to_numeric(df["成交量"], errors="coerce")
            result["amount"] = pd.to_numeric(df["成交额"], errors="coerce")
        return result

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
