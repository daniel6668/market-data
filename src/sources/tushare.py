"""Tushare Pro 数据源 — A股 + 港股"""
import os
import pandas as pd
import tushare as ts
from ..utils import RateLimiter


class TushareSource:
    """封装 Tushare Pro API，内置限速"""
    
    def __init__(self, config: dict):
        token = config["tushare"].get("token") or os.environ.get("TUSHARE_TOKEN", "")
        ts.set_token(token)
        self.pro = ts.pro_api()
        rl_cfg = config["rate_limit"]["tushare"]
        self.limiter = RateLimiter(max_calls=rl_cfg["max_calls"], period=rl_cfg["period"])
        self._stock_list_cache = None

    def _call(self, func, **kwargs) -> pd.DataFrame:
        """带限速的 API 调用"""
        self.limiter.wait()
        result = func(**kwargs)
        return result if not result.empty else pd.DataFrame()

    def get_stock_list(self, market: str = "A") -> pd.DataFrame:
        """获取股票列表
        
        market: 'A' 获取A股，'HK' 获取港股
        返回 DataFrame，列对齐 stock_info 表结构
        """
        if market == "A":
            df = self._call(self.pro.stock_basic, 
                           exchange='', 
                           list_status='L',
                           fields='ts_code,symbol,name,area,industry,list_date,delist_date,is_hs')
            if not df.empty:
                df["market"] = "A"
                df["exchange"] = df["ts_code"].str.split(".").str[1]
                # 删除 symbol 列 — stock_info 表没有此列
                if "symbol" in df.columns:
                    df = df.drop(columns=["symbol"])
        elif market == "HK":
            df = self._call(self.pro.hk_basic,
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
            ts_code=ts_code,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", "")
        )

    def get_adj_factor(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取复权因子"""
        return self._call(
            self.pro.adj_factor,
            ts_code=ts_code,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", "")
        )

    def get_hk_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取港股日线"""
        return self._call(
            self.pro.hk_daily,
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
            exchange=exchange,
            start_date=start_date,
            end_date=end_date
        )
