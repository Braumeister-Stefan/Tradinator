"""
Tradinator — Strategy Eval.

Performs basic pre-trade signal validation.  Acts as a quality gate:
are these signals reasonable before we act on them?  Phase 1 uses
placeholder checks and stub risk metrics.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import math


class StrategyEval:
    """Validate trading signals with basic quality and risk checks."""

    VALID_DIRECTIONS = {"BUY", "SELL", "HOLD"}
    MIN_SIGNAL_STRENGTH = 0.01  # minimum strength to pass
    MAX_SIGNALS_PCT = 1.0       # max fraction of universe (reserved for Phase 2)
    RISK_FREE_RATE = 0.04       # annual risk-free rate for Sharpe stub
    MIN_DATA_POINTS = 20        # minimum price history length for validation

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, signals: dict, market_data: dict) -> dict:
        """Validate each signal and return only those that pass all checks."""
        raw_signals = signals.get("signals", {})
        prices = market_data.get("prices", {})

        validated = {}
        passed_count = 0
        rejected_count = 0

        for epic, signal in raw_signals.items():
            close_prices = prices.get(epic, {}).get("close", [])
            result = self._validate_signal(epic, signal, close_prices)
            validated[epic] = result

            if result["validation"]["passed"]:
                passed_count += 1
            else:
                rejected_count += 1

        validated = self._apply_filters(validated)

        total = passed_count + rejected_count
        print(f"[StrategyEval] Validation: {passed_count}/{total} signals passed")

        return {
            "signals": validated,
            "timestamp": signals.get("timestamp", ""),
            "summary": {
                "total": total,
                "passed": passed_count,
                "rejected": rejected_count,
            },
        }

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _validate_signal(self, epic: str, signal: dict, close_prices: list) -> dict:
        """Run all checks on a single signal and augment it with validation info."""
        data_quality = self._check_data_quality(close_prices)
        sharpe = self._estimate_sharpe(close_prices)
        volatility = self._estimate_volatility(close_prices)

        direction = signal.get("direction", "HOLD")
        strength = signal.get("strength", 0.0)

        if direction not in self.VALID_DIRECTIONS:
            passed = False
        elif direction == "HOLD":
            passed = True
        else:
            passed = (
                strength >= self.MIN_SIGNAL_STRENGTH
                and data_quality == "sufficient"
            )

        return {
            "direction": direction,
            "strength": strength,
            "strategy": signal.get("strategy", ""),
            "validation": {
                "passed": passed,
                "sharpe_estimate": sharpe,
                "volatility": volatility,
                "data_quality": data_quality,
            },
        }

    def _check_data_quality(self, close_prices: list) -> str:
        """Return 'sufficient' if enough data points exist, else 'insufficient'."""
        if len(close_prices) >= self.MIN_DATA_POINTS:
            return "sufficient"
        return "insufficient"

    def _estimate_sharpe(self, close_prices: list) -> float | None:
        """Placeholder Sharpe ratio from daily returns (annualized)."""
        if len(close_prices) < self.MIN_DATA_POINTS:
            return None

        daily_returns = self._daily_returns(close_prices)
        if not daily_returns:
            return None

        mean_return = sum(daily_returns) / len(daily_returns)
        ann_return = mean_return * 252

        ann_std = self._annualized_std(daily_returns)
        if ann_std == 0:
            return None

        return (ann_return - self.RISK_FREE_RATE) / ann_std

    def _estimate_volatility(self, close_prices: list) -> float | None:
        """Standard deviation of daily returns (not annualized)."""
        if len(close_prices) < self.MIN_DATA_POINTS:
            return None

        daily_returns = self._daily_returns(close_prices)
        if not daily_returns:
            return None

        return self._std(daily_returns)

    def _apply_filters(self, validated_signals: dict) -> dict:
        """Remove signals that did not pass validation."""
        filtered = {}
        for epic, signal in validated_signals.items():
            if signal["validation"]["passed"]:
                filtered[epic] = signal
        return filtered

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _daily_returns(close_prices: list) -> list:
        """Compute simple daily returns: (p[i] - p[i-1]) / p[i-1]."""
        returns = []
        for i in range(1, len(close_prices)):
            prev = close_prices[i - 1]
            if prev == 0:
                continue
            returns.append((close_prices[i] - prev) / prev)
        return returns

    @staticmethod
    def _std(values: list) -> float:
        """Population standard deviation of a list of floats."""
        if not values:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        return math.sqrt(variance)

    @classmethod
    def _annualized_std(cls, daily_returns: list) -> float:
        """Annualized standard deviation: std(daily) * sqrt(252)."""
        return cls._std(daily_returns) * math.sqrt(252)
