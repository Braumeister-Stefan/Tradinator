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
        prices = self._extract_prices(positions, market_data)
        target_sizes = self._compute_target_sizes(weights, total_value, prices)
        deltas = self._compute_deltas(target_sizes, current_holdings)
        orders, skipped = self._generate_orders(deltas, current_holdings, metadata, latest_prices)

        summary = {
            "total_orders": len(orders),
            "buy_orders": sum(1 for o in orders if o["direction"] == "BUY"),
            "sell_orders": sum(1 for o in orders if o["direction"] == "SELL"),
        }

        print(
            f"[OrderGenerator] Generated {summary['total_orders']} order(s): "
            f"{summary['buy_orders']} BUY, {summary['sell_orders']} SELL, "
            f"{len(skipped)} skipped"
        )

        return {"orders": orders, "summary": summary, "skipped": skipped}

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _get_current_holdings(self, positions: list) -> dict:
        """Convert the positions list into {epic: signed_size}; BUY=positive, SELL=negative."""
        holdings = {}
        for pos in positions:
            epic = pos.get("epic")
            size = float(pos.get("size", 0))
            direction = pos.get("direction")
            signed = size if direction == "BUY" else -size
            # Accumulate in case multiple positions share an epic.
            holdings[epic] = holdings.get(epic, 0.0) + signed
        return holdings

    def _extract_prices(self, positions: list, market_data: dict = None) -> dict:
        """Build {epic: price} from market data (latest close) and position levels."""
        prices = {}
        # Use latest close price from market data when available
        if market_data:
            for epic, fields in market_data.get("prices", {}).items():
                closes = fields.get("close", [])
                if closes and closes[-1] is not None:
                    prices[epic] = closes[-1]
        # Fall back to position open level for epics not in market data
        for pos in positions:
            epic = pos.get("epic")
            level = float(pos.get("level", 0))
            if epic and level > 0 and epic not in prices:
                prices[epic] = level
        return prices

    def _compute_target_sizes(
        self, weights: dict, total_value: float, prices: dict
    ) -> dict:
        """Convert weights to target sizes.

        Phase 1 placeholder: target_size = weight * total_value / price.
        If no price is available the divisor defaults to 1.0, making
        target_size equal to notional value.
        """
        target_sizes = {}
        for epic, weight in weights.items():
            price = prices.get(epic, 1.0)
            target_sizes[epic] = (weight * total_value) / price
        return target_sizes

    def _compute_deltas(self, target_sizes: dict, current_holdings: dict) -> dict:
        """Compute per-epic delta (target − current).

        Covers three cases:
        1. Epic in targets only  → full buy.
        2. Epic in both          → partial adjustment.
        3. Epic in holdings only → full close.
        """
        all_epics = set(target_sizes) | set(current_holdings)
        deltas = {}
        for epic in all_epics:
            target = target_sizes.get(epic, 0.0)
            current = current_holdings.get(epic, 0.0)
            deltas[epic] = target - current
        return deltas

    def _generate_orders(self, deltas: dict, current_holdings: dict, metadata=None, latest_prices=None) -> tuple:
        """Convert deltas into order dicts, filtering by per-instrument constraints."""
        if metadata is None:
            metadata = {}
        if latest_prices is None:
            latest_prices = {}
        skipped = []
        orders = []
        for epic, delta in deltas.items():
            abs_delta = abs(delta)

            epic_meta = metadata.get(epic, {})
            min_deal_size = epic_meta.get("min_deal_size", self.MIN_ORDER_SIZE)
            lot_size = epic_meta.get("lot_size", 1.0)

            if lot_size > 0:
                size = int(abs_delta / lot_size) * lot_size
            else:
                size = abs_delta

            if size < min_deal_size:
                if size == 0:
                    reason_text = f"rounds to 0 at lot_size {lot_size}"
                else:
                    reason_text = f"size {size:.4f} below min {min_deal_size}"
                skipped.append({"epic": epic, "reason": reason_text})
                print(f"[OrderGenerator] ⚠ Skipped {epic}: {reason_text}")
                continue

            direction = "BUY" if delta > 0 else "SELL"
            current = current_holdings.get(epic, 0.0)
            reason = self._classify_reason(current, delta)

            orders.append(
                {
                    "epic": epic,
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
