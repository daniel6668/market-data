"""东方财富数据中心统一数据源

覆盖 6 种数据（融资融券/龙虎榜/大宗交易/股东户数/分红送转/限售解禁），
共用 datacenter-web.eastmoney.com 入口，内置串行限流。
"""
import logging
import time
import random

import pandas as pd
import requests

from .base import DataSource

logger = logging.getLogger(__name__)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
DATACENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EM_MIN_INTERVAL = 1.0

_em_last_call = [0.0]
_em_session: requests.Session | None = None


def _em():
    global _em_session
    if _em_session is None:
        _em_session = requests.Session()
        _em_session.headers.update({
            "User-Agent": UA,
            "Referer": "https://data.eastmoney.com/",
        })
        _em_session.trust_env = False
        try:
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            adapter = HTTPAdapter(max_retries=Retry(
                total=3, connect=3, backoff_factor=0.6,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
            ))
            _em_session.mount("https://", adapter)
            _em_session.mount("http://", adapter)
        except Exception:
            pass

    wait = EM_MIN_INTERVAL - (time.time() - _em_last_call[0])
    if wait > 0:
        time.sleep(wait + random.uniform(0.1, 0.5))
    _em_last_call[0] = time.time()
    return _em_session


def _em_datacenter_get(report_name, filter_str="",
                        page_size=50,
                        sort_columns="",
                        sort_types="-1"):
    params = {
        "reportName": report_name,
        "columns": "ALL",
        "filter": filter_str,
        "pageNumber": "1",
        "pageSize": str(page_size),
        "sortColumns": sort_columns,
        "sortTypes": sort_types,
        "source": "WEB",
        "client": "WEB",
    }
    try:
        r = _em().get(DATACENTER_URL, params=params, timeout=15)
        d = r.json()
        if d.get("result") and d["result"].get("data"):
            return d["result"]["data"]
    except Exception as e:
        logger.warning(f"datacenter [{report_name}] fail: {e}")
    return []


class EastMoneyDatacenterSource(DataSource):

    def get_stock_list(self, market="A"):
        return pd.DataFrame()

    def get_daily(self, ts_code, start_date, end_date):
        return pd.DataFrame()

    @property
    def supports_extra(self):
        return False

    def get_margin_trading(self, ts_code, page_size=30):
        data = _em_datacenter_get(
            "RPTA_WEB_RZRQ_GGMX",
            filter_str='(SCODE="' + ts_code + '")',
            page_size=page_size,
            sort_columns="DATE",
            sort_types="-1",
        )
        if not data:
            return pd.DataFrame()
        rows = []
        for row in data:
            rows.append({
                "ts_code": ts_code,
                "trade_date": str(row.get("DATE", ""))[:10] if row.get("DATE") else "",
                "rzye": row.get("RZYE"),
                "rzmre": row.get("RZMRE"),
                "rzche": row.get("RZCHE"),
                "rqye": row.get("RQYE"),
                "rqmcl": row.get("RQMCL"),
                "rqchl": row.get("RQCHL"),
                "rzrqye": row.get("RZRQYE"),
            })
        return pd.DataFrame(rows)

    def get_dragon_tiger(self, ts_code, trade_date, look_back=30):
        from datetime import datetime, timedelta
        import json

        end_dt = datetime.strptime(trade_date, "%Y-%m-%d") if trade_date else datetime.now()
        start_dt = end_dt - timedelta(days=look_back)
        start_str = start_dt.strftime("%Y-%m-%d")
        end_str = end_dt.strftime("%Y-%m-%d")

        data = _em_datacenter_get(
            "RPT_DAILYBILLBOARD_DETAILSNEW",
            filter_str="(TRADE_DATE>='" + start_str + "')" +
                       "(TRADE_DATE<='" + end_str + "')" +
                       "(SECURITY_CODE=\"" + ts_code + "\")",
            page_size=50,
            sort_columns="TRADE_DATE",
            sort_types="-1",
        )
        if not data:
            return pd.DataFrame()

        latest_date = str(data[0].get("TRADE_DATE", ""))[:10]
        buy_seats = []
        sell_seats = []
        if latest_date:
            buy_data = _em_datacenter_get(
                "RPT_BILLBOARD_DAILYDETAILSBUY",
                filter_str="(TRADE_DATE='" + latest_date + "')" +
                           "(SECURITY_CODE=\"" + ts_code + "\")",
                page_size=10,
                sort_columns="BUY",
                sort_types="-1",
            )
            for row in (buy_data or [])[:5]:
                buy_seats.append({
                    "name": row.get("OPERATEDEPT_NAME", ""),
                    "buy_amt_wan": round((row.get("BUY") or 0) / 10000, 1),
                    "sell_amt_wan": round((row.get("SELL") or 0) / 10000, 1),
                    "net_wan": round((row.get("NET") or 0) / 10000, 1),
                })
            sell_data = _em_datacenter_get(
                "RPT_BILLBOARD_DAILYDETAILSSELL",
                filter_str="(TRADE_DATE='" + latest_date + "')" +
                           "(SECURITY_CODE=\"" + ts_code + "\")",
                page_size=10,
                sort_columns="SELL",
                sort_types="-1",
            )
            for row in (sell_data or [])[:5]:
                sell_seats.append({
                    "name": row.get("OPERATEDEPT_NAME", ""),
                    "buy_amt_wan": round((row.get("BUY") or 0) / 10000, 1),
                    "sell_amt_wan": round((row.get("SELL") or 0) / 10000, 1),
                    "net_wan": round((row.get("NET") or 0) / 10000, 1),
                })

        rows = []
        for row in data:
            rows.append({
                "ts_code": ts_code,
                "trade_date": str(row.get("TRADE_DATE", ""))[:10],
                "reason": row.get("EXPLANATION", ""),
                "net_buy_wan": round((row.get("BILLBOARD_NET_AMT") or 0) / 10000, 1),
                "turnover_pct": round(float(row.get("TURNOVERRATE") or 0), 2),
                "close": row.get("CLOSE_PRICE"),
                "change_pct": round(float(row.get("CHANGE_RATE") or 0), 2),
                "buy_seats": json.dumps(buy_seats, ensure_ascii=False) if buy_seats else None,
                "sell_seats": json.dumps(sell_seats, ensure_ascii=False) if sell_seats else None,
            })
        return pd.DataFrame(rows)

    def get_block_trade(self, ts_code, page_size=20):
        data = _em_datacenter_get(
            "RPT_DATA_BLOCKTRADE",
            filter_str='(SECURITY_CODE="' + ts_code + '")',
            page_size=page_size,
            sort_columns="TRADE_DATE",
            sort_types="-1",
        )
        if not data:
            return pd.DataFrame()
        rows = []
        for row in data:
            close = row.get("CLOSE_PRICE") or 0
            deal = row.get("DEAL_PRICE") or 0
            premium = ((deal / close - 1) * 100) if close else 0
            rows.append({
                "ts_code": ts_code,
                "trade_date": str(row.get("TRADE_DATE", ""))[:10],
                "deal_price": deal,
                "close": close,
                "premium_pct": round(premium, 2),
                "deal_vol": row.get("DEAL_VOLUME"),
                "deal_amt": row.get("DEAL_AMT"),
                "buyer": row.get("BUYER_NAME", ""),
                "seller": row.get("SELLER_NAME", ""),
            })
        return pd.DataFrame(rows)

    def get_holder_num(self, ts_code, page_size=10):
        data = _em_datacenter_get(
            "RPT_HOLDERNUMLATEST",
            filter_str='(SECURITY_CODE="' + ts_code + '")',
            page_size=page_size,
            sort_columns="END_DATE",
            sort_types="-1",
        )
        if not data:
            return pd.DataFrame()
        rows = []
        for row in data:
            rows.append({
                "ts_code": ts_code,
                "end_date": str(row.get("END_DATE", ""))[:10],
                "holder_num": row.get("HOLDER_NUM"),
                "change_num": row.get("HOLDER_NUM_CHANGE"),
                "change_ratio_pct": row.get("HOLDER_NUM_RATIO"),
                "avg_free_shares": row.get("AVG_FREE_SHARES"),
            })
        return pd.DataFrame(rows)

    def get_dividend(self, ts_code, page_size=20):
        data = _em_datacenter_get(
            "RPT_SHAREBONUS_DET",
            filter_str='(SECURITY_CODE="' + ts_code + '")',
            page_size=page_size,
            sort_columns="EX_DIVIDEND_DATE",
            sort_types="-1",
        )
        if not data:
            return pd.DataFrame()
        rows = []
        for row in data:
            rows.append({
                "ts_code": ts_code,
                "ex_date": str(row.get("EX_DIVIDEND_DATE", ""))[:10],
                "bonus_rmb": row.get("PRETAX_BONUS_RMB"),
                "transfer_ratio": row.get("TRANSFER_RATIO"),
                "bonus_ratio": row.get("BONUS_RATIO"),
                "plan": row.get("ASSIGN_PROGRESS", ""),
            })
        return pd.DataFrame(rows)

    def get_lockup_expiry(self, ts_code, trade_date, forward_days=90):
        from datetime import datetime, timedelta

        if not trade_date:
            trade_date = datetime.now().strftime("%Y-%m-%d")
        end_dt = datetime.strptime(trade_date, "%Y-%m-%d") + timedelta(days=forward_days)
        end_str = end_dt.strftime("%Y-%m-%d")

        data = _em_datacenter_get(
            "RPT_LIFT_STAGE",
            filter_str='(SECURITY_CODE="' + ts_code + '")' +
                       "(FREE_DATE>='2000-01-01')" +
                       "(FREE_DATE<='" + end_str + "')",
            page_size=30,
            sort_columns="FREE_DATE",
            sort_types="-1",
        )
        if not data:
            return pd.DataFrame()
        rows = []
        for row in data:
            rows.append({
                "ts_code": ts_code,
                "free_date": str(row.get("FREE_DATE", ""))[:10],
                "stock_type": row.get("LIMITED_STOCK_TYPE", ""),
                "free_shares": row.get("FREE_SHARES_NUM"),
                "free_ratio": row.get("FREE_RATIO"),
            })
        return pd.DataFrame(rows)
