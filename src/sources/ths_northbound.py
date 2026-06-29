"""同花顺北向资金数据源 — 沪股通/深股通日级累计净买入

实现本地 CSV 自缓存：每次拉取实时数据后自动写入本地 CSV，
历史越跑越丰富。
"""
import logging
from pathlib import Path

import pandas as pd
import requests

from .base import DataSource

logger = logging.getLogger(__name__)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/117.0.0.0 Safari/537.36"

HSGT_HEADERS = {
    "User-Agent": UA,
    "Host": "data.hexin.cn",
    "Referer": "https://data.hexin.cn/",
}

CACHE_DIR = Path.home() / ".tradingagents" / "cache"


def _northbound_cache_path() -> Path:
    """北向资金本地 CSV 缓存路径"""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / "northbound_daily.csv"


def _save_snapshot(date: str, hgt: float, sgt: float):
    """写入/更新当天北向收盘数据到 CSV"""
    path = _northbound_cache_path()
    rows = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").strip().split("\n")[1:]:
            parts = line.split(",")
            if len(parts) == 3 and parts[0]:
                rows[parts[0]] = line
    rows[date] = f"{date},{hgt},{sgt}"
    with open(path, "w", encoding="utf-8") as f:
        f.write("date,hgt,sgt\n")
        for d in sorted(rows.keys()):
            f.write(rows[d] + "\n")


def _load_history(n: int = 60) -> pd.DataFrame:
    """读取最近 N 天北向历史"""
    path = _northbound_cache_path()
    if not path.exists():
        return pd.DataFrame({"date": [], "hgt_yi": [], "sgt_yi": []})
    df = pd.read_csv(path)
    return df.tail(n).rename(columns={"hgt": "hgt_yi", "sgt": "sgt_yi"})


class ThsNorthboundSource(DataSource):
    """同花顺北向资金数据源"""

    def get_stock_list(self, market: str = "A") -> pd.DataFrame:
        return pd.DataFrame()

    def get_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return pd.DataFrame()

    @property
    def supports_extra(self) -> bool:
        return False

    def get_daily_flow(self) -> pd.DataFrame:
        """获取当日北向资金分钟流向，聚合成日级快照

        Returns DataFrame columns: date, hgt_yi(沪股通累计净买入,亿元),
                                   sgt_yi(深股通累计净买入,亿元)
        """
        url = "https://data.hexin.cn/market/hsgtApi/method/dayChart/"
        try:
            r = requests.get(url, headers=HSGT_HEADERS, timeout=10)
            d = r.json()
        except Exception as e:
            logger.warning(f"北向资金请求失败: {e}")
            return pd.DataFrame()

        times = d.get("time", [])
        hgt = d.get("hgt", [])
        sgt = d.get("sgt", [])

        if not times:
            return pd.DataFrame()

        # 取最后一个有效值（收盘累计）
        hgt_last = None
        sgt_last = None
        n = min(len(hgt), len(sgt))
        for i in range(n - 1, -1, -1):
            if hgt[i] is not None and hgt_last is None:
                hgt_last = hgt[i]
            if sgt[i] is not None and sgt_last is None:
                sgt_last = sgt[i]
            if hgt_last is not None and sgt_last is not None:
                break

        if hgt_last is None and sgt_last is None:
            return pd.DataFrame()

        # 获取日期
        from datetime import datetime
        today = times[-1][:10] if times else datetime.now().strftime("%Y-%m-%d")

        # 写入本地缓存
        try:
            _save_snapshot(today, hgt_last or 0, sgt_last or 0)
        except Exception as e:
            logger.debug(f"北向缓存写入失败: {e}")

        return pd.DataFrame([{
            "date": today,
            "hgt_yi": hgt_last or 0,
            "sgt_yi": sgt_last or 0,
        }])

    def get_history(self, days: int = 60) -> pd.DataFrame:
        """读取本地缓存的北向历史数据"""
        return _load_history(days)
