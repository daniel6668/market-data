"""mootdx 数据源 — 通达信 TCP 协议（不限频、不封 IP、零 Token）

A 股日线数据的主要来源，替代 Tushare daily 接口。
股票列表仍需 Tushare 或东财（mootdx 不提供列表信息）。
"""
import socket
import logging
from datetime import datetime, date
from typing import Optional

import pandas as pd
from mootdx.quotes import Quotes

from .base import DataSource

logger = logging.getLogger(__name__)

# 通达信服务器列表（2026-06 实测可用）
_TDX_SERVERS = [
    ("119.97.185.59", 7709),
    ("124.70.133.119", 7709),
    ("116.205.183.150", 7709),
    ("123.60.73.44", 7709),
    ("116.205.163.254", 7709),
    ("121.36.225.169", 7709),
]


def _probe_server(ip: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False


def _find_server() -> Optional[tuple]:
    for ip, port in _TDX_SERVERS:
        if _probe_server(ip, port):
            return (ip, port)
    return None


class MootdxSource(DataSource):
    """通达信 TCP 数据源"""

    def __init__(self):
        server = _find_server()
        if not server:
            raise RuntimeError("所有通达信服务器均不可达（防火墙可能阻断 TCP 7709）")
        self._server = server
        self._client: Optional[Quotes] = None

    def _get_client(self) -> Quotes:
        if self._client is None:
            self._client = Quotes.factory(market="std", server=self._server)
        return self._client

    # ── 股票列表（mootdx 不支持，空实现，由上层 fallback 到 Tushare）──

    def get_stock_list(self, market: str = "A") -> pd.DataFrame:
        """mootdx 不提供股票列表，返回空 DataFrame，
        Pipeline 层自动 fallback 到 TushareSource。

        其他 market 同理。
        """
        return pd.DataFrame()

    # ── A 股日线 ──

    def get_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        通过 mootdx TCP 获取 A 股日线 OHLCV。

        参数:
          ts_code: 股票代码（6 位数字，如 "600036"）
          start_date / end_date: YYYYMMDD

        返回:
          DataFrame 包含: ts_code, trade_date, open, high, low, close, vol, amount
          ⚠️ 注意: mootdx 返回【不复权】原始价格
        """
        client = self._get_client()

        # mootdx bars 返回最近 offset 条日线（frequency=9 为日线）
        # 不支持 start/end 直接过滤，所以取较多条再手动筛选
        try:
            df = client.bars(symbol=ts_code, frequency=9, offset=800)
        except Exception as e:
            logger.warning(f"mootdx bars({ts_code}) 失败: {e}")
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        # mootdx 的 DataFrame 有 'datetime' 同时作为 index 名和列名，
        # reset_index 会冲突。先删掉列中的 datetime，reset 后再统一处理。
        if "datetime" in df.columns:
            df = df.drop(columns=["datetime"])
        df = df.reset_index()

        # 标准化列名
        col_map = {
            "datetime": "trade_date",
            "open": "open",
            "close": "close",
            "high": "high",
            "low": "low",
            "vol": "vol",
            "volume": "vol",
            "amount": "amount",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        # trade_date 修正：从 datetime 提取日期
        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

        # 按日期过滤
        if "trade_date" in df.columns:
            try:
                start_d = datetime.strptime(start_date, "%Y%m%d").date()
                end_d = datetime.strptime(end_date, "%Y%m%d").date()
                df = df[(df["trade_date"] >= start_d) & (df["trade_date"] <= end_d)]
            except (ValueError, KeyError):
                pass

        # 确保必要列存在
        for col in ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]:
            if col not in df.columns:
                if col == "ts_code":
                    df[col] = ts_code
                elif col == "amount":
                    df[col] = pd.NA
                else:
                    df[col] = pd.NA

        # 只保留标准列
        keep = [c for c in ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]
                if c in df.columns]
        return df[keep]

    @property
    def supports_extra(self) -> bool:
        """mootdx 不支持 daily_basic / adj_factor"""
        return False
