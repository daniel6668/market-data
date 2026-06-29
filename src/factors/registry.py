"""因子注册表 — 每个因子一个计算函数，统一签名"""
import pandas as pd
import numpy as np
from stockstats import StockDataFrame


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """保证 stockstats 需要的列名"""
    df = df.copy()
    if "vol" in df.columns and "volume" not in df.columns:
        df["volume"] = df["vol"]
    return df


def compute_ma(df: pd.DataFrame, window: int) -> pd.Series:
    """移动均线"""
    return df["close"].rolling(window).mean()


def compute_ema(df: pd.DataFrame, window: int) -> pd.Series:
    """指数移动均线"""
    return df["close"].ewm(span=window, adjust=False).mean()


def compute_macd(df: pd.DataFrame) -> pd.DataFrame:
    """MACD 三值"""
    s = StockDataFrame.retype(_ensure_columns(df))
    return pd.DataFrame({
        "macd_dif": s["macd"],
        "macd_dea": s["macds"],
        "macd_bar": s["macdh"],
    })


def compute_ret(df: pd.DataFrame, window: int) -> pd.Series:
    """N日涨跌幅 (%)"""
    return df["close"].pct_change(window) * 100


def compute_rsi(df: pd.DataFrame, window: int) -> pd.Series:
    """RSI"""
    s = StockDataFrame.retype(_ensure_columns(df))
    return s[f"rsi_{window}"]


def compute_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """ATR"""
    s = StockDataFrame.retype(_ensure_columns(df))
    return s["atr"]


def compute_boll(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """布林带 (upper, mid, lower)"""
    s = StockDataFrame.retype(_ensure_columns(df))
    return pd.DataFrame({
        "boll_upper": s["boll_ub"],
        "boll_mid": s["boll"],
        "boll_lower": s["boll_lb"],
    })


def compute_vol_ratio(df: pd.DataFrame) -> pd.Series:
    """量比 = 当日成交量 / 5日均量"""
    avg5 = df["vol"].rolling(5).mean()
    return df["vol"] / avg5


def compute_avg_vol(df: pd.DataFrame, window: int) -> pd.Series:
    """N日均量"""
    return df["vol"].rolling(window).mean()


# 因子注册表: name -> (func, kwargs, column_name)
# column_name: 如果 func 返回 DataFrame，指定取哪个列
FACTOR_REGISTRY = {
    "ma5":         (compute_ma,       {"window": 5},  None),
    "ma10":        (compute_ma,       {"window": 10}, None),
    "ma20":        (compute_ma,       {"window": 20}, None),
    "ma60":        (compute_ma,       {"window": 60}, None),
    "ema12":       (compute_ema,      {"window": 12}, None),
    "ema26":       (compute_ema,      {"window": 26}, None),
    "macd_dif":    (compute_macd,     {},             "macd_dif"),
    "macd_dea":    (compute_macd,     {},             "macd_dea"),
    "macd_bar":    (compute_macd,     {},             "macd_bar"),
    "ret_5d":      (compute_ret,      {"window": 5},  None),
    "ret_10d":     (compute_ret,      {"window": 10}, None),
    "ret_20d":     (compute_ret,      {"window": 20}, None),
    "rsi6":        (compute_rsi,      {"window": 6},  None),
    "rsi14":       (compute_rsi,      {"window": 14}, None),
    "atr14":       (compute_atr,      {"window": 14}, None),
    "boll_upper":  (compute_boll,     {"window": 20}, "boll_upper"),
    "boll_mid":    (compute_boll,     {"window": 20}, "boll_mid"),
    "boll_lower":  (compute_boll,     {"window": 20}, "boll_lower"),
    "vol_ratio":   (compute_vol_ratio, {},            None),
    "avg_vol_5d":  (compute_avg_vol,  {"window": 5},  None),
    "avg_vol_20d": (compute_avg_vol,  {"window": 20}, None),
}
