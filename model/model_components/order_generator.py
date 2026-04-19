"""
Tradinator — Order Generator.

Converts target portfolio weights into specific orders by computing the
delta between the target portfolio and current positions.  Pure arithmetic
— no API calls.

Phase 1 simplification: weight-to-size conversion uses notional value
(target_size = weight * total_value) rather than real-time prices.  This
is a placeholder that will be replaced when live price feeds are wired in.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""


class OrderGenerator:
    """Convert a target-weight portfolio into executable order dicts."""

    MIN_ORDER_SIZE = 0.01       # minimum order size (fractional shares)
    ROUNDING_PRECISION = 2      # decimal places for order sizes
    LIMIT_ORDER_THRESHOLD = 0.5 # threshold for LIMIT order eligibility

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, target_portfolio: dict, broker_state: dict, market_data: dict = None) -> dict:
        """Compute the orders needed to move from current positions to target weights."""
        positions = broker_state.get("positions", [])
        weights = target_portfolio.get("weights", {})
        total_value = target_portfolio.get("total_value", 0.0)
        metadata = market_data.get("metadata", {}) if market_data else {}

        latest_prices = {}
        if market_data:
            for epic, fields in market_data.get("prices", {}).items():
                closes = fields.get("close", [])
                if closes and closes[-1] is not None:
                    latest_prices[epic] = closes[-1]

        current_holdings = self._get_current_holdings(positions)
        target_sizes = self._compute_target_sizes(weights, total_value, latest_prices, metadata)
        deltas = self._compute_deltas(target_sizes, current_holdings)
        orders, skipped = self._generate_orders(deltas, current_holdings, metadata, latest_prices)

        summary = {
            "total_orders": len(orders),
            "buy_orders": sum(1 for o in orders if o["direction"] == "BUY"),
            "sell_orders": sum(1 for o in orders if o["direction"] == "SELL"),
        }

        skip_reasons = {}
        for s in skipped:
            r = s.get("reason", "unknown")
            skip_reasons[r] = skip_reasons.get(r, 0) + 1
        skip_breakdown = ", ".join(f"{v} {k}" for k, v in skip_reasons.items())
        skip_suffix = f" ({skip_breakdown})" if skip_breakdown else ""

        print(
            f"[OrderGenerator] Generated {summary['total_orders']} order(s): "
            f"{summary['buy_orders']} BUY, {summary['sell_orders']} SELL, "
            f"{len(skipped)} skipped{skip_suffix}"
        )

        return {"orders": orders, "summary": summary, "skipped": skipped}

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _get_current_holdings(self, positions: list) -> dict:
        """Convert the positions list into {instrument_id: signed_size}; BUY=positive, SELL=negative."""
        holdings = {}
        for pos in positions:
            instrument_id = pos.get("instrument_id")
            size = float(pos.get("size", 0))
            direction = pos.get("direction")
            signed = size if direction == "BUY" else -size
            # Accumulate in case multiple positions share an instrument.
            holdings[instrument_id] = holdings.get(instrument_id, 0.0) + signed
        return holdings

    def _compute_target_sizes(
        self, weights: dict, total_value: float,
        latest_prices: dict | None = None, metadata: dict | None = None,
    ) -> dict:
        """Convert weights to target sizes in contract units.

        target_size = (weight * total_value) / (latest_price * scaling_factor).
        The scaling_factor converts mid-prices to the IG "points" price
        (e.g. EURUSD 1.1350 → 11350 with scalingFactor=10000).
        Falls back to notional value when no price is available.
        """
        if latest_prices is None:
            latest_prices = {}
        if metadata is None:
            metadata = {}
        target_sizes = {}
        for instrument_id, weight in weights.items():
            notional = weight * total_value
            price = latest_prices.get(instrument_id)
            scaling = metadata.get(instrument_id, {}).get("scaling_factor", 1)
            effective_price = price * scaling if price and price > 0 else 0
            if effective_price > 0:
                target_sizes[instrument_id] = notional / effective_price
            else:
                target_sizes[instrument_id] = notional
        return target_sizes

    def _compute_deltas(self, target_sizes: dict, current_holdings: dict) -> dict:
        """Compute per-instrument delta (target − current).

        Covers three cases:
        1. Instrument in targets only  → full buy.
        2. Instrument in both          → partial adjustment.
        3. Instrument in holdings only → full close.
        """
        all_instruments = set(target_sizes) | set(current_holdings)
        deltas = {}
        for instrument_id in all_instruments:
            target = target_sizes.get(instrument_id, 0.0)
            current = current_holdings.get(instrument_id, 0.0)
            deltas[instrument_id] = target - current
        return deltas

    def _generate_orders(self, deltas: dict, current_holdings: dict, metadata=None, latest_prices=None) -> tuple:
        """Convert deltas into order dicts, filtering by per-instrument constraints."""
        if metadata is None:
            metadata = {}
        if latest_prices is None:
            latest_prices = {}
        skipped = []
        orders = []
        for instrument_id, delta in deltas.items():
            if instrument_id not in latest_prices:
                skipped.append({"instrument_id": instrument_id, "reason": "no price data"})
                print(f"[OrderGenerator] ⚠ Skipped {instrument_id}: no price data")
                continue

            abs_delta = abs(delta)

            instrument_meta = metadata.get(instrument_id, {})
            min_deal_size = instrument_meta.get("min_deal_size", self.MIN_ORDER_SIZE)

            increment = instrument_meta.get("min_size_increment", 1.0) or 1.0
            size = round(abs_delta / increment) * increment
            size = round(size, 8)  # float artefact guard

            max_deal_size = instrument_meta.get("max_deal_size")
            if max_deal_size is not None:
                size = min(size, max_deal_size)

            if size == 0:
                skipped.append({"instrument_id": instrument_id, "reason": "rounds to zero"})
                print(f"[OrderGenerator] ⚠ Skipped {instrument_id}: rounds to zero")
                continue

            if size < min_deal_size:
                skipped.append({"instrument_id": instrument_id, "reason": "below min size"})
                print(f"[OrderGenerator] ⚠ Skipped {instrument_id}: below min size")
                continue

            direction = "BUY" if delta > 0 else "SELL"
            current = current_holdings.get(instrument_id, 0.0)
            reason = self._classify_reason(current, delta)

            orders.append(
                {
                    "instrument_id": instrument_id,
                    "direction": direction,
                    "size": size,
                    "order_type": "MARKET",
                    "limit_level": None,
                    "time_in_force": "FILL_OR_KILL",
                    "reason": reason,
                }
            )
        return (orders, skipped)

    @staticmethod
    def _classify_reason(current: float, delta: float) -> str:
        """Determine the human-readable reason for an order."""
        target = current + delta

        if current == 0.0 and target > 0:
            return "new_position"
        if target == 0.0 and current > 0:
            return "close"
        if delta > 0 and current > 0:
            return "increase"
        if delta < 0 and current > 0:
            return "decrease"
        # Fallback covers short-side or edge cases.
        return "close"
