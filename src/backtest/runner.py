"""回测引擎 — 基于 vectorbt"""
import logging
import duckdb
import pandas as pd
import numpy as np
import vectorbt as vbt
from .report import BacktestResult

logger = logging.getLogger(__name__)


class BacktestRunner:
    """回测执行器"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def _load_prices(self, ts_code: str, start: str, end: str) -> pd.Series:
        """加载复权后收盘价"""
        df = self.conn.execute("""
            SELECT a.trade_date, a.close * COALESCE(b.adj_factor, 1) as adj_close
            FROM a_daily a
            LEFT JOIN a_adj_factor b ON a.ts_code = b.ts_code AND a.trade_date = b.trade_date
            WHERE a.ts_code = ? AND a.trade_date >= ? AND a.trade_date <= ?
            ORDER BY a.trade_date
        """, [ts_code, start, end]).fetchdf()
        if df.empty:
            return pd.Series(dtype=float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df.set_index("trade_date")["adj_close"]

    def _generate_signals(self, prices: pd.Series,
                           condition_buy: pd.Series,
                           condition_sell: pd.Series) -> tuple:
        """生成买卖信号"""
        entries = condition_buy.loc[prices.index].fillna(False).astype(bool)
        exits = condition_sell.loc[prices.index].fillna(False).astype(bool)
        entries = entries & ~entries.shift(1).fillna(False)
        exits = exits & ~exits.shift(1).fillna(False)
        return entries, exits

    def run_single(self, ts_code: str,
                   condition_buy: pd.Series,
                   condition_sell: pd.Series,
                   start: str, end: str,
                   commission: float = 0.0002) -> BacktestResult:
        """单股回测

        condition_buy/condition_sell: boolean Series, index=date
        commission: 手续费率，默认万二
        """
        prices = self._load_prices(ts_code, start, end)
        if prices.empty:
            return BacktestResult()

        entries, exits = self._generate_signals(prices, condition_buy, condition_sell)

        try:
            pf = vbt.Portfolio.from_signals(
                prices, entries, exits,
                freq="1D",
                init_cash=100000,
                fees=commission,
                slippage=0.001,
            )
        except Exception as e:
            logger.warning(f"回测 {ts_code} 失败: {e}")
            return BacktestResult()

        stats = pf.stats()
        equity = pf.value()

        bh_return = (prices.iloc[-1] / prices.iloc[0] - 1) * 100

        result = BacktestResult(
            total_return=float(stats.get("Total Return [%]", 0)),
            annual_return=float(stats.get("Annual Return [%]", 0)),
            sharpe_ratio=float(stats.get("Sharpe Ratio", 0)),
            max_drawdown=float(stats.get("Max Drawdown [%]", 0)),
            win_rate=float(stats.get("Win Rate [%]", 0)),
            profit_factor=float(stats.get("Profit Factor", 0)),
            n_trades=int(stats.get("Total Trades", 0)),
            equity_curve=pd.DataFrame({"date": equity.index, "value": equity.values}),
            benchmark_return=bh_return,
        )
        return result

    def run_comparison(self, results: dict[str, BacktestResult]) -> pd.DataFrame:
        """多策略/多股票回测结果对比"""
        rows = []
        for name, r in results.items():
            d = r.to_dict()
            d["name"] = name
            rows.append(d)
        return pd.DataFrame(rows)
