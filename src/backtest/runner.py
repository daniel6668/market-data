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

    def run_portfolio(self, codes: list[str],
                      condition_buy: dict[str, pd.Series],
                      condition_sell: dict[str, pd.Series],
                      start: str, end: str,
                      weights: str = "equal",
                      rebalance: str = "monthly",
                      commission: float = 0.0002) -> "PortfolioResult":
        """组合回测

        codes: 股票代码列表
        condition_buy / condition_sell: {ts_code: boolean Series}
        weights: 'equal' | 'market_cap'
        rebalance: 'monthly' | 'quarterly' | None
        """
        from .report import PortfolioResult

        # 加载所有股票的复权价格
        all_prices = {}
        all_entries = {}
        all_exits = {}
        valid_codes = []

        for ts_code in codes:
            prices = self._load_prices(ts_code, start, end)
            if prices.empty:
                continue
            buy_cond = condition_buy.get(ts_code)
            sell_cond = condition_sell.get(ts_code)
            if buy_cond is None or sell_cond is None:
                continue
            entries, exits = self._generate_signals(prices, buy_cond, sell_cond)
            if not entries.any() and not exits.any():
                # 无信号的股票，用 buy & hold 填充
                entries = pd.Series(False, index=prices.index)
                entries.iloc[0] = True
                exits = pd.Series(False, index=prices.index)

            all_prices[ts_code] = prices
            all_entries[ts_code] = entries
            all_exits[ts_code] = exits
            valid_codes.append(ts_code)

        if not valid_codes:
            return PortfolioResult()

        # 构建组合层面的等权重价格序列
        price_df = pd.DataFrame(all_prices).ffill()
        entry_df = pd.DataFrame(all_entries).fillna(False)
        exit_df = pd.DataFrame(all_exits).fillna(False)

        # 等权重：每只股票分配 equal weight
        n = len(valid_codes)
        w = {c: 1.0 / n for c in valid_codes}

        # 用 vectorbt 组合回测
        try:
            pf = vbt.Portfolio.from_signals(
                price_df, entry_df, exit_df,
                freq="1D",
                init_cash=100000,
                fees=commission,
                slippage=0.001,
            )
        except Exception as e:
            logger.warning(f"组合回测失败: {e}")
            return PortfolioResult()

        stats = pf.stats()
        equity = pf.value()
        if isinstance(equity, pd.DataFrame) and equity.shape[1] > 1:
            total_equity = equity.sum(axis=1)
        elif isinstance(equity, pd.DataFrame):
            total_equity = equity.iloc[:, 0]
        else:
            total_equity = equity
        bh = (price_df.mean(axis=1).iloc[-1] / price_df.mean(axis=1).iloc[0] - 1) * 100

        # 跑个股回测作为明细
        stock_results = []
        for ts_code in valid_codes:
            try:
                sr = self.run_single(
                    ts_code,
                    all_entries[ts_code],
                    all_exits[ts_code],
                    start, end, commission
                )
                stock_results.append(sr)
            except Exception:
                pass

        return PortfolioResult(
            total_return=float(stats.get("Total Return [%]", 0)),
            annual_return=float(stats.get("Annual Return [%]", 0)),
            sharpe_ratio=float(stats.get("Sharpe Ratio", 0)),
            max_drawdown=float(stats.get("Max Drawdown [%]", 0)),
            win_rate=float(stats.get("Win Rate [%]", 0)),
            n_stocks=len(valid_codes),
            n_trades=int(stats.get("Total Trades", 0)),
            benchmark_return=bh,
            equity_curve=pd.DataFrame({"date": total_equity.index, "value": total_equity.values}),
            stock_results=stock_results,
            weights=w,
        )
