"""
Tradinator — Signal Engine.

Generates BUY / SELL / HOLD signals from market data using a simple
dual moving-average crossover strategy (placeholder logic for Phase 1).

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import datetime


class SignalEngine:
    """Create trading signals from market data using a moving-average crossover."""

    FAST_WINDOW = 10        # short moving average period
    SLOW_WINDOW = 30        # long moving average period
    SIGNAL_THRESHOLD = 0.0  # minimum absolute strength to generate a signal

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, market_data: dict) -> dict:
        """Generate a signal for every instrument in market_data."""
        prices = market_data.get("prices", {})
        signals = {}
        counts = {"BUY": 0, "SELL": 0, "HOLD": 0}

        for epic, fields in prices.items():
            close_prices = fields.get("close", [])
            signal = self._generate_signal(close_prices)
            signals[epic] = signal
            counts[signal["direction"]] += 1

        print(
            f"[SignalEngine] Generated signals: "
            f"{counts['BUY']} BUY, {counts['SELL']} SELL, {counts['HOLD']} HOLD"
        )

        return {
            "signals": signals,
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _generate_signal(self, close_prices: list) -> dict:
        """Compute fast/slow MA crossover and return a direction + strength."""
        if len(close_prices) < self.SLOW_WINDOW:
            return {
                "direction": "HOLD",
                "strength": 0.0,
                "strategy": "ma_crossover",
            }

        # Optimization: calculate উভয় sums in a single slice if windows overlap
        # but for simplicity and clarity, we just take the last N elements once.
        slow_slice = close_prices[-self.SLOW_WINDOW:]
        fast_slice = slow_slice[-self.FAST_WINDOW:]

        slow_ma = sum(slow_slice) / self.SLOW_WINDOW
        fast_ma = sum(fast_slice) / self.FAST_WINDOW

        raw_strength = fast_ma - slow_ma
        # Optimization: Use the already calculated fast_slice sum for normalization 
        # instead of a full pass over close_prices if appropriate, but keeping 
        # consistency with original logic which used full list average.
        avg_price = sum(close_prices) / len(close_prices)
        
        strength = 0.0
        if avg_price != 0:
            strength = min(abs(raw_strength) / avg_price, 1.0)

        if strength <= self.SIGNAL_THRESHOLD:
            direction = "HOLD"
        elif raw_strength > 0:
            direction = "BUY"
        else:
            direction = "SELL"

        return {
            "direction": direction,
            "strength": strength,
            "strategy": "ma_crossover",
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_moving_average(prices: list, window: int) -> float | None:
        """Return the simple moving average of the last *window* prices."""
        if len(prices) < window:
            return None
        return sum(prices[-window:]) / window

    @staticmethod
    def _normalize_strength(raw_strength: float, close_prices: list) -> float:
        """Normalize the raw MA gap to a 0.0–1.0 range using average price level."""
        if not close_prices:
            return 0.0
        avg_price = sum(close_prices) / len(close_prices)
        if avg_price == 0:
            return 0.0
        return min(abs(raw_strength) / avg_price, 1.0)
