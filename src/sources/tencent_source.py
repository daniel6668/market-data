"""腾讯财经数据源 — 实时 PE/PB/市值/换手率/涨跌停

HTTP GET (GBK)，不限频不封 IP，批量获取 up to ~50 只股票。
替代 Tushare daily_basic 接口。
"""
import logging
import urllib.request
from datetime import date

import pandas as pd

from .base import DataSource

logger = logging.getLogger(__name__)

TENCENT_URL = "https://qt.gtimg.cn/q="

# 腾讯财经字段索引 → 标准化字段
_FIELD_MAP = {
    "name": 1,
    "price": 3,
    "last_close": 4,
    "open": 5,
    "change_pct": 32,
    "high": 33,
    "low": 34,
    "amount_wan": 37,
    "turnover_pct": 38,
    "pe_ttm": 39,
    "amplitude_pct": 43,
    "mcap_yi": 44,
    "float_mcap_yi": 45,
    "pb": 46,
    "limit_up": 47,
    "limit_down": 48,
    "vol_ratio": 49,
    "pe_static": 52,
}


def _tencent_prefix(code: str) -> str:
    if code.startswith(("6", "9")):
        return f"sh{code}"
    if code.startswith("8"):
        return f"bj{code}"
    return f"sz{code}"


class TencentSource(DataSource):
    """腾讯财经数据源（PE/PB/市值，不提供股票列表和日线）"""

    def get_stock_list(self, market: str = "A") -> pd.DataFrame:
        return pd.DataFrame()

    def get_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return pd.DataFrame()

    def get_daily_basic(self, ts_code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """获取实时 PE/PB/市值等基本面数据（单只股票）。

        注意：腾讯接口是【实时数据】，不支持历史查询。
        如需历史 PE，需从 daily_basic 表累积读取。
        """
        codes = [ts_code]
        prefixed = [_tencent_prefix(c) for c in codes]
        url = TENCENT_URL + ",".join(prefixed)

        req = urllib.request.Request(url)
        req.add_header("User-Agent", "Mozilla/5.0")

        try:
            resp = urllib.request.urlopen(req, timeout=10)
            data = resp.read().decode("gbk")
        except Exception as e:
            logger.warning(f"腾讯接口失败: {e}")
            return pd.DataFrame()

        rows = []
        for line in data.strip().split(";"):
            if not line.strip() or "=" not in line or '"' not in line:
                continue
            vals = line.split('"')[1].split("~")
            if len(vals) < 53:
                continue

            raw_code = line.split("=")[0].split("_")[-1][2:]
            rows.append({
                "ts_code": raw_code,
                "trade_date": date.today(),
                "pe": float(vals[_FIELD_MAP["pe_ttm"]]) if vals[_FIELD_MAP["pe_ttm"]] else None,
                "pb": float(vals[_FIELD_MAP["pb"]]) if vals[_FIELD_MAP["pb"]] else None,
                "total_mv": float(vals[_FIELD_MAP["mcap_yi"]]) * 1e8 if vals[_FIELD_MAP["mcap_yi"]] else None,
                "circ_mv": float(vals[_FIELD_MAP["float_mcap_yi"]]) * 1e8 if vals[_FIELD_MAP["float_mcap_yi"]] else None,
                "turnover_rate": float(vals[_FIELD_MAP["turnover_pct"]]) if vals[_FIELD_MAP["turnover_pct"]] else None,
                "close": float(vals[_FIELD_MAP["price"]]) if vals[_FIELD_MAP["price"]] else None,
            })

        return pd.DataFrame(rows)

    @property
    def supports_extra(self) -> bool:
        return False
