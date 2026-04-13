"""
Tradinator — Portfolio Analytics.

Calculates retrospective portfolio performance metrics from the ledger
history: total return, period return, max drawdown, Sharpe ratio, and
exposure breakdown.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import math


class PortfolioAnalytics:
    """Compute performance metrics from the portfolio ledger history."""

    RISK_FREE_RATE = 0.04        # annual risk-free rate for Sharpe calculation
    ANNUALIZATION_FACTOR = 252   # trading days per year

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, ledger_snapshot: dict) -> dict:
        """Return a dict of portfolio performance analytics."""
        history = ledger_snapshot.get("history", [])
        balance = ledger_snapshot.get("balance", 0.0)
        cash = ledger_snapshot.get("cash", 0.0)
        positions = ledger_snapshot.get("positions", [])
        timestamp = ledger_snapshot.get("timestamp", "")

        returns = self._compute_returns(history)

        total_return = self._total_return(history)
        period_return = self._period_return(history)
        max_drawdown = self._max_drawdown(history)
        sharpe = self._sharpe_ratio(returns)
        volatility = self._annualized_volatility(returns)
        exposure = self._current_exposure(balance, cash, positions)

        analytics = {
            "total_return_pct": total_return,
            "period_return_pct": period_return,
            "max_drawdown_pct": max_drawdown,
            "sharpe_ratio": sharpe,
            "volatility_annual_pct": volatility,
            "current_exposure": exposure,
            "history_length": len(history),
            "timestamp": timestamp,
        }

        self._print_summary(analytics)

        return analytics

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_returns(history: list) -> list:
        """Compute period-over-period returns from balance history."""
        if len(history) < 2:
            return []
        returns = []
        for i in range(1, len(history)):
            prev = history[i - 1]["balance"]
            curr = history[i]["balance"]
            if prev != 0.0:
                returns.append((curr - prev) / prev)
            else:
                returns.append(0.0)
        return returns

    @staticmethod
    def _total_return(history: list) -> float | None:
        """Compute total return percentage from first to last snapshot."""
        if len(history) < 2:
            return None
        first = history[0]["balance"]
        last = history[-1]["balance"]
        if first == 0.0:
            return None
        return (last - first) / first * 100.0

    @staticmethod
    def _period_return(history: list) -> float | None:
        """Compute return percentage from second-to-last to last snapshot."""
        if len(history) < 2:
            return None
        prev = history[-2]["balance"]
        curr = history[-1]["balance"]
        if prev == 0.0:
            return None
        return (curr - prev) / prev * 100.0

    @staticmethod
    def _max_drawdown(history: list) -> float | None:
        """Find the maximum peak-to-trough decline as a percentage."""
        if len(history) < 2:
            return None
        peak = history[0]["balance"]
        max_dd = 0.0
        for record in history:
            balance = record["balance"]
            if balance > peak:
                peak = balance
            if peak != 0.0:
                drawdown = (peak - balance) / peak * 100.0
                if drawdown > max_dd:
                    max_dd = drawdown
        return max_dd

    def _sharpe_ratio(self, returns: list) -> float | None:
        """Compute annualized Sharpe ratio from period returns."""
        if len(returns) < 2:
            return None
        mean_r = sum(returns) / len(returns)
        std_r = self._population_std(returns)
        if std_r == 0.0:
            return None
        af = self.ANNUALIZATION_FACTOR
        annualized_return = mean_r * af
        annualized_std = std_r * math.sqrt(af)
        return (annualized_return - self.RISK_FREE_RATE) / annualized_std

    def _annualized_volatility(self, returns: list) -> float | None:
        """Compute annualized return volatility as a percentage."""
        if len(returns) < 2:
            return None
        std_r = self._population_std(returns)
        return std_r * math.sqrt(self.ANNUALIZATION_FACTOR) * 100.0

    @staticmethod
    def _current_exposure(
        balance: float, cash: float, positions: list
    ) -> dict:
        """Compute invested vs cash percentages of the portfolio."""
        if balance == 0.0:
            return {
                "invested_pct": 0.0,
                "cash_pct": 0.0,
                "position_count": len(positions),
            }
        cash_pct = cash / balance * 100.0
        invested_pct = (balance - cash) / balance * 100.0
        return {
            "invested_pct": invested_pct,
            "cash_pct": cash_pct,
            "position_count": len(positions),
        }

    @staticmethod
    def _population_std(values: list) -> float:
        """Compute population standard deviation (divide by N)."""
        n = len(values)
        if n == 0:
            return 0.0
        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / n
        return math.sqrt(variance)

    @staticmethod
    def _print_summary(analytics: dict) -> None:
        """Print a brief summary of key performance metrics."""
        parts = []

        total = analytics["total_return_pct"]
        if total is not None:
            parts.append(f"total_return={total:+.2f}%")

        dd = analytics["max_drawdown_pct"]
        if dd is not None:
            parts.append(f"max_drawdown={dd:.2f}%")

        sharpe = analytics["sharpe_ratio"]
        if sharpe is not None:
            parts.append(f"sharpe={sharpe:.2f}")

        exposure = analytics["current_exposure"]
        parts.append(f"invested={exposure['invested_pct']:.1f}%")

        parts.append(f"snapshots={analytics['history_length']}")

        print(f"[PortfolioAnalytics] {', '.join(parts)}")
