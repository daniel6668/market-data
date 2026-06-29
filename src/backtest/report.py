"""回测结果数据结构"""
from dataclasses import dataclass, field
import pandas as pd


@dataclass
class BacktestResult:
    """回测绩效指标"""
    total_return: float = 0.0
    annual_return: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    n_trades: int = 0
    equity_curve: pd.DataFrame = field(default_factory=pd.DataFrame)
    trades: pd.DataFrame = field(default_factory=pd.DataFrame)
    benchmark_return: float = 0.0

    def to_dict(self) -> dict:
        return {
            "total_return": round(self.total_return, 2),
            "annual_return": round(self.annual_return, 2),
            "sharpe_ratio": round(self.sharpe_ratio, 2),
            "max_drawdown": round(self.max_drawdown, 2),
            "win_rate": round(self.win_rate, 2),
            "profit_factor": round(self.profit_factor, 2),
            "n_trades": self.n_trades,
            "benchmark_return": round(self.benchmark_return, 2),
        }

    def summary(self) -> str:
        d = self.to_dict()
        return (
            f"总收益: {d['total_return']}% | 年化: {d['annual_return']}% | "
            f"夏普: {d['sharpe_ratio']} | 最大回撤: {d['max_drawdown']}% | "
            f"胜率: {d['win_rate']}% | 交易: {d['n_trades']}次"
        )
