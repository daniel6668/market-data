"""数据源抽象基类 — 统一接口，支持注册式分发"""
from abc import ABC, abstractmethod
import pandas as pd


class DataSource(ABC):
    """所有数据源的抽象基类。

    子类需实现 get_stock_list / get_daily。
    可选实现 get_daily_basic / get_adj_factor（仅 A 股数据源需要）。
    """

    @abstractmethod
    def get_stock_list(self, market: str = "A") -> pd.DataFrame:
        """获取股票列表，返回 DataFrame 列对齐 stock_info 表结构。

        必须包含: ts_code, name, market 列。
        可选包含: list_date, delist_date, industry, area, exchange, is_hs, list_status。
        """
        ...

    @abstractmethod
    def get_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取日线数据。

        必须包含: ts_code, trade_date 列。
        可选包含: open, high, low, close, vol, amount 等。
        """
        ...

    def get_daily_basic(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取日线基本面（可选，仅 A 股数据源实现）。

        返回: ts_code, trade_date, turnover_rate, pe, pb, total_mv, circ_mv 等。
        """
        return pd.DataFrame()

    def get_adj_factor(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取复权因子（可选，仅 A 股数据源实现）。

        返回: ts_code, trade_date, adj_factor。
        """
        return pd.DataFrame()

    @property
    def supports_extra(self) -> bool:
        """是否支持额外数据（basic + adj_factor）。A 股数据源返回 True。"""
        return False
