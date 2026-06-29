"""东方财富 push2 系列数据源 — 资金流 / 研报 / 行业板块

仅用于东财独有数据，已内置 em_get() 串行限流防封。
datacenter-web 全系已下线（2026-06），改用 push2/push2his/reportapi。
"""
import logging
import time
import random
from datetime import date

import pandas as pd
import requests

from .base import DataSource

logger = logging.getLogger(__name__)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
EM_MIN_INTERVAL = 1.0

# 全局节流
_em_last_call = [0.0]
_em_session: requests.Session | None = None


def _em():
    global _em_session
    if _em_session is None:
        _em_session = requests.Session()
        _em_session.headers.update({"User-Agent": UA, "Referer": "https://quote.eastmoney.com/"})
        _em_session.trust_env = False
    wait = EM_MIN_INTERVAL - (time.time() - _em_last_call[0])
    if wait > 0:
        time.sleep(wait + random.uniform(0.1, 0.5))
    _em_last_call[0] = time.time()
    return _em_session


def _safe_float(v):
    try:
        return float(v) if v and str(v).strip() else None
    except (TypeError, ValueError):
        return None


class EastMoneySource(DataSource):
    """东方财富数据源（资金流 / 研报 / 行业）"""

    def get_stock_list(self, market: str = "A") -> pd.DataFrame:
        return pd.DataFrame()

    def get_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return pd.DataFrame()

    # ── 日级资金流（主力/大单/中单/小单/超大单）──

    def get_fund_flow(self, ts_code: str, days: int = 120) -> pd.DataFrame:
        """个股日级资金流向（最近 days 个交易日）"""
        mkt = "1" if ts_code.startswith(("6", "9")) else "0"
        url = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
        params = {
            "secid": f"{mkt}.{ts_code}",
            "fields1": "f1,f2,f3,f7",
            "fields2": "f51,f52,f53,f54,f55,f56,f57",
            "lmt": str(min(days, 120)),
        }
        # 最多重试 2 次，东财 API 间歇性返回空
        for attempt in range(2):
            try:
                r = _em().get(url, params=params, timeout=15)
                d = r.json()
                if d.get("data") is not None:
                    break
            except Exception as e:
                if attempt == 0:
                    time.sleep(1.5)
                    continue
                logger.warning(f"push2his 资金流 {ts_code} 失败: {e}")
                return pd.DataFrame()
        klines = (d.get("data") or {}).get("klines", [])
        rows = []
        for line in klines:
            parts = line.split(",")
            if len(parts) >= 7:
                rows.append({
                    "ts_code": ts_code,
                    "trade_date": parts[0],
                    "main_net": float(parts[1]) if parts[1] != "-" else 0,
                    "small_net": float(parts[2]) if parts[2] != "-" else 0,
                    "mid_net": float(parts[3]) if parts[3] != "-" else 0,
                    "large_net": float(parts[4]) if parts[4] != "-" else 0,
                    "super_net": float(parts[5]) if parts[5] != "-" else 0,
                    "main_pct": float(parts[6]) if len(parts) >= 7 and parts[6] != "-" else None,
                })
        return pd.DataFrame(rows)

    # ── 研报列表 ──

    def get_reports(self, ts_code: str, max_pages: int = 3) -> pd.DataFrame:
        """获取个股研报列表

        字段: publish_date, org_name, title, info_code,
              predict_eps_this, predict_eps_next, predict_eps_next2, rating
        """
        all_records = []
        url = "https://reportapi.eastmoney.com/report/list"

        for page in range(1, max_pages + 1):
            params = {
                "industryCode": "*", "pageSize": "100", "industry": "*",
                "rating": "*", "ratingChange": "*",
                "beginTime": "2024-01-01", "endTime": "2030-01-01",
                "pageNo": str(page), "fields": "", "qType": "0",
                "code": ts_code, "p": str(page),
            }
            try:
                r = _em().get(url, params=params,
                             headers={"Referer": "https://data.eastmoney.com/"}, timeout=30)
                d = r.json()
                rows = d.get("data") or []
                if not rows:
                    break
                all_records.extend(rows)
                total_pages = d.get("TotalPage", 1) or 1
                if page >= total_pages:
                    break
            except Exception as e:
                logger.warning(f"研报 {ts_code} 第{page}页失败: {e}")
                break

        return pd.DataFrame([{
            "ts_code": ts_code,
            "publish_date": r.get("publishDate", "")[:10],
            "org_name": r.get("orgSName", ""),
            "title": r.get("title", ""),
            "info_code": r.get("infoCode", ""),
            "eps_2026": _safe_float(r.get("predictThisYearEps")),
            "eps_2027": _safe_float(r.get("predictNextYearEps")),
            "eps_2028": _safe_float(r.get("predictNextTwoYearEps")),
            "rating": r.get("emRatingName", ""),
        } for r in all_records])

    # ── 行业板块排名 ──

    def get_industry_ranking(self, top_n: int = 20, target_date: date = None) -> pd.DataFrame:
        """全市场行业板块涨跌幅排名（~100 个行业）

        target_date: 目标日期，None=今天（如果今天无数据则自动往前找）
        """
        if target_date is None:
            target_date = date.today()

        params = {
            "pn": "1", "pz": "100", "po": "1", "np": "1",
            "fltt": "2", "invt": "2",
            "fs": "m:90+t:2",
            "fields": "f2,f3,f4,f12,f14,f104,f105,f128,f136,f140",
        }
        # 如果当天数据全为 0，往前试最近 3 个交易日
        for offset in range(3):
            try:
                r = _em().get(url, params=params, timeout=15)
                d = r.json()
                items = d.get("data", {}).get("diff", [])
                if items and any(float(i.get("f3", 0)) != 0 for i in items):
                    break  # 有有效数据
            except Exception as e:
                if offset == 2:
                    logger.warning(f"行业排名失败: {e}")
                    return pd.DataFrame()
            time.sleep(1.0)
            target_date = date.fromordinal(target_date.toordinal() - 1)
        else:
            return pd.DataFrame()
        if not items:
            return pd.DataFrame()

        rows = []
        for i, item in enumerate(items):
            rows.append({
                "rank": i + 1,
                "name": item.get("f14", ""),
                "code": item.get("f12", ""),
                "change_pct": item.get("f3", 0),
                "up_count": item.get("f104", 0),
                "down_count": item.get("f105", 0),
                "leader": item.get("f140", ""),
                "leader_change": item.get("f136", 0),
                "trade_date": date.today(),
            })
        return pd.DataFrame(rows[:top_n])

    @property
    def supports_extra(self) -> bool:
        return False
