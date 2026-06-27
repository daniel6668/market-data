"""yfinance 数据源 — 美股"""
import yfinance as yf
import pandas as pd
from ..utils import RateLimiter


# 常见美股列表（SP500 主要成分作为种子，可扩展）
DEFAULT_US_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B",
    "JPM", "V", "JNJ", "WMT", "PG", "MA", "UNH", "HD", "BAC", "DIS",
    "NFLX", "ADBE", "CRM", "AMD", "INTC", "QCOM", "TXN", "PYPL",
    "VZ", "T", "PFE", "MRK", "ABBV", "KO", "PEP", "COST", "AVGO",
    "CSCO", "ACN", "ABT", "DHR", "NKE", "LLY", "CVX", "XOM", "WFC",
    "ORCL", "IBM", "INTU", "AMGN", "GE", "CAT", "BA", "MMM", "GS",
    "MS", "SPY", "QQQ", "IWM", "DIA", "EEM", "XLF", "XLE", "XLK",
    "TLT", "GLD", "SLV", "USO", "VXX", "ARKK", "SMH", "SOXX",
]


class YFinanceSource:
    """封装 yfinance API，内置限速"""
    
    def __init__(self, config: dict):
        rl_cfg = config["rate_limit"]["yfinance"]
        self.limiter = RateLimiter(max_calls=rl_cfg["max_calls"], period=rl_cfg["period"])

    def get_us_stock_list(self) -> pd.DataFrame:
        """获取美股列表（默认符号列表）"""
        rows = []
        for sym in DEFAULT_US_SYMBOLS:
            rows.append({
                "ts_code": sym,
                "name": sym,
                "market": "US",
                "list_date": None,
                "delist_date": None,
                "industry": None,
                "area": "US",
                "exchange": "NYSE/NASDAQ",
                "is_hs": None,
                "list_status": "L",
            })
        return pd.DataFrame(rows)

    def get_us_daily(self, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取美股日线数据"""
        self.limiter.wait()
        try:
            t = yf.Ticker(ticker)
            df = t.history(start=start_date, end=end_date)
            if df.empty:
                return pd.DataFrame()
            df = df.reset_index()
            result = pd.DataFrame()
            result["ts_code"] = ticker
            result["trade_date"] = pd.to_datetime(df["Date"]).dt.date
            result["open"] = df["Open"].astype(float)
            result["high"] = df["High"].astype(float)
            result["low"] = df["Low"].astype(float)
            result["close"] = df["Close"].astype(float)
            result["adj_close"] = df.get("Adj Close", df["Close"]).astype(float) if "Adj Close" in df.columns else df["Close"].astype(float)
            result["volume"] = df["Volume"].astype("int64")
            return result
        except Exception:
            return pd.DataFrame()
