"""工具模块 — 限速器、日志、交易日历、配置加载"""
import time
import logging
import yaml
from pathlib import Path
from typing import Any
from typing import Optional


class RateLimiter:
    """滑动窗口限速器，支持突发 + 平滑限速"""
    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self.calls: list = []

    def wait(self) -> None:
        """等待直到可以发下一个请求"""
        now = time.time()
        # 清理过期的调用记录
        self.calls = [t for t in self.calls if now - t < self.period]
        if len(self.calls) >= self.max_calls:
            # 等 oldest call 过期
            sleep_time = self.calls[0] + self.period - now + 0.1
            if sleep_time > 0:
                time.sleep(sleep_time)
            # 递归重试（清理后）
            self.wait()
            return
        self.calls.append(time.time())

    @property
    def remaining(self) -> int:
        """剩余可用调用次数"""
        now = time.time()
        self.calls = [t for t in self.calls if now - t < self.period]
        return max(0, self.max_calls - len(self.calls))


class TradingCalendar:
    """交易日历 — 判断某天是否为交易日

    优先使用从数据库加载的精确交易日历（含节假日），
    未加载时 fallback 到周末检查。
    """

    _trading_days: set = set()  # 类级缓存：所有交易日的字符串集合

    @classmethod
    def load_from_db(cls, conn: Any, exchange: str = "SSE") -> int:
        """从数据库加载交易日历到内存"""
        try:
            rows = conn.execute(
                "SELECT cal_date FROM trade_calendar "
                "WHERE exchange = ? AND is_open = True",
                [exchange]
            ).fetchall()
            cls._trading_days = {str(r[0]) for r in rows}
            return len(cls._trading_days)
        except Exception:
            return 0

    @classmethod
    def ensure_calendar(cls, conn: Any, ts_source: Any = None,
                        exchange: str = "SSE") -> None:
        """确保交易日历已加载。数据库无数据时尝试从 Tushare 拉取。"""
        from .db import has_trade_calendar, save_trade_calendar
        if not has_trade_calendar(conn, exchange):
            if ts_source is None:
                return
            try:
                df = ts_source.get_trade_calendar(exchange=exchange)
                if not df.empty:
                    save_trade_calendar(conn, df)
            except Exception:
                pass  # 拉取失败，fallback 到周末检查
        cls.load_from_db(conn, exchange)

    @staticmethod
    def is_weekend(date_str: str) -> bool:
        """判断是否为周末"""
        from datetime import datetime
        dt = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
        return dt.weekday() >= 5

    @classmethod
    def is_trading_day(cls, market: str, date_str: str) -> bool:
        """判断是否为交易日。优先用数据库日历，fallback 到周末检查。"""
        ds = str(date_str)[:10]
        if cls._trading_days:
            return ds in cls._trading_days
        return not cls.is_weekend(ds)


def load_config(config_path: str = "config.yaml") -> dict:
    """加载配置文件，不存在则返回默认配置"""
    path = Path(config_path)
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    # 返回默认配置
    return {
        "tushare": {"token": ""},
        "database": {"path": "data/market.duckdb"},
        "data": {
            "start_date": "2015-01-01",
            "markets": ["A", "ETF", "HK", "US"]
        },
        "rate_limit": {
            "tushare": {"max_calls": 190, "period": 60},
            "akshare": {"max_calls": 48, "period": 60},
            "yfinance": {"max_calls": 1900, "period": 3600},
        },
        "retry": {"max_attempts": 3, "backoff_base": 5},
        "logging": {"level": "INFO", "file": "data/pipeline.log"},
    }


def setup_logger(config: dict) -> logging.Logger:
    """配置日志器"""
    log_cfg = config.get("logging", {})
    logger = logging.getLogger("market_data")
    logger.setLevel(getattr(logging, log_cfg.get("level", "INFO")))
    
    # 文件 handler
    log_file = Path(log_cfg.get("file", "data/pipeline.log"))
    log_file.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    
    # 控制台 handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger
