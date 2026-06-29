"""新浪财经数据源 — 财报三表（资产负债表/利润表/现金流量表）

HTTP API, quotes.sina.cn, 免费无 key, 不限频。
"""
import logging
from datetime import date

import pandas as pd
import requests

from .base import DataSource

logger = logging.getLogger(__name__)

SINA_FINANCE_URL = "https://quotes.sina.cn/cn/api/openapi.php/CompanyFinanceService.getFinanceReport2022"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class SinaSource(DataSource):
    """新浪财经数据源（财报三表）"""

    def get_stock_list(self, market: str = "A") -> pd.DataFrame:
        return pd.DataFrame()

    def get_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return pd.DataFrame()

    def get_financial_report(self, ts_code: str, report_type: str = "lrb", num_periods: int = 8) -> pd.DataFrame:
        """获取财报数据

        report_type: lrb(利润表) / fzb(资产负债表) / llb(现金流量表)
        num_periods: 取最近 N 期

        返回 DataFrame: period, ts_code, <科目1>, <科目2>, ...
        """
        prefix = "sh" if ts_code.startswith(("6", "9")) else "sz"
        paper_code = f"{prefix}{ts_code}"

        try:
            r = requests.get(
                SINA_FINANCE_URL,
                params={"paperCode": paper_code, "source": report_type, "type": "0",
                        "page": "1", "num": str(num_periods)},
                headers={"User-Agent": UA},
                timeout=15,
            )
            d = r.json()
        except Exception as e:
            logger.warning(f"新浪财报 {ts_code} 失败: {e}")
            return pd.DataFrame()

        report_list = d.get("result", {}).get("data", {}).get("report_list", {}) or {}
        if not report_list:
            return pd.DataFrame()

        rows = []
        for period in sorted(report_list.keys(), reverse=True)[:num_periods]:
            obj = report_list[period]
            row = {
                "period": f"{period[:4]}-{period[4:6]}-{period[6:8]}",
                "ts_code": ts_code,
            }
            for it in obj.get("data", []) or []:
                title = it.get("item_title", "")
                if not title or it.get("item_value") is None:
                    continue
                try:
                    row[title] = float(it["item_value"])
                except (TypeError, ValueError):
                    row[title] = it["item_value"]
            rows.append(row)

        return pd.DataFrame(rows)

    def get_income_statement(self, ts_code: str) -> pd.DataFrame:
        """利润表"""
        return self.get_financial_report(ts_code, "lrb", 8)

    def get_balance_sheet(self, ts_code: str) -> pd.DataFrame:
        """资产负债表"""
        return self.get_financial_report(ts_code, "fzb", 8)

    def get_cashflow(self, ts_code: str) -> pd.DataFrame:
        """现金流量表"""
        return self.get_financial_report(ts_code, "llb", 8)

    @property
    def supports_extra(self) -> bool:
        return False
