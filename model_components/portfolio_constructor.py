"""
Tradinator — Portfolio Constructor.

Converts validated signals into target portfolio weights, subject to
position size limits and cash reserve constraints.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""


class PortfolioConstructor:
    """Build a target-weight portfolio from validated trading signals."""

    WEIGHTING_METHOD = "signal_strength"  # "equal" or "signal_strength"
    REBALANCE_TOLERANCE = 0.02            # ignore rebalancing if delta < 2%

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, validated_signals: dict, broker_state: dict) -> dict:
        """Convert validated signals into a target portfolio of weights."""
        actionable = self._filter_actionable_signals(validated_signals)
        raw_weights = self._compute_raw_weights(actionable)

        max_position_pct = self.config.get("max_position_pct", 0.25)
        cash_reserve_pct = self.config.get("cash_reserve_pct", 0.05)

        weights = self._normalize_weights(
            raw_weights, max_position_pct, cash_reserve_pct
        )
        target_portfolio = self._build_target_portfolio(weights, broker_state)

        position_count = sum(1 for w in weights.values() if w > 0)
        cash_pct = target_portfolio["cash_weight"] * 100
        print(
            f"[PortfolioConstructor] Target portfolio: "
            f"{position_count} position(s), cash weight {cash_pct:.1f}%"
        )

        return target_portfolio

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _filter_actionable_signals(self, validated_signals: dict) -> dict:
        """Keep only signals where validation passed and direction is BUY or SELL."""
        signals = validated_signals.get("signals", {})
        actionable = {}

        for epic, signal in signals.items():
            validation = signal.get("validation", {})
            if not validation.get("passed", False):
                continue
            if signal.get("direction") not in ("BUY", "SELL"):
                continue
            actionable[epic] = signal

        return actionable

    def _compute_raw_weights(self, actionable_signals: dict) -> dict:
        """Compute unnormalized weights from actionable signals."""
        buy_signals = {
            epic: sig
            for epic, sig in actionable_signals.items()
            if sig.get("direction") == "BUY"
        }

        if not buy_signals:
            return {}

        raw_weights = {}

        if self.WEIGHTING_METHOD == "equal":
            for epic in buy_signals:
                raw_weights[epic] = 1.0
        else:
            # signal_strength: weight proportional to strength
            for epic, sig in buy_signals.items():
                raw_weights[epic] = max(sig.get("strength", 0.0), 0.0)

        return raw_weights

    def _normalize_weights(
        self,
        raw_weights: dict,
        max_position_pct: float,
        cash_reserve_pct: float,
    ) -> dict:
        """Normalize weights so sum <= (1 - cash_reserve_pct) and each <= max_position_pct."""
        if not raw_weights:
            return {}

        investable = 1.0 - cash_reserve_pct
        total_raw = sum(raw_weights.values())

        if total_raw == 0:
            return {epic: 0.0 for epic in raw_weights}

        # First pass: scale so sum equals investable budget.
        weights = {
            epic: (w / total_raw) * investable for epic, w in raw_weights.items()
        }

        # Iteratively cap at max_position_pct and redistribute excess.
        for _ in range(len(weights)):
            capped = {}
            excess = 0.0
            uncapped_total = 0.0

            for epic, w in weights.items():
                if w > max_position_pct:
                    capped[epic] = max_position_pct
                    excess += w - max_position_pct
                else:
                    capped[epic] = w
                    uncapped_total += w

            if excess == 0:
                break

            # Redistribute excess proportionally among uncapped positions.
            if uncapped_total == 0:
                weights = capped
                break

            weights = {}
            for epic, w in capped.items():
                if w < max_position_pct:
                    weights[epic] = w + excess * (w / uncapped_total)
                else:
                    weights[epic] = w

        return weights

    def _build_target_portfolio(self, weights: dict, broker_state: dict) -> dict:
        """Assemble the target_portfolio output dict."""
        total_value = float(broker_state.get("balance", 0))
        weight_sum = sum(weights.values())
        cash_weight = 1.0 - weight_sum

        return {
            "weights": dict(weights),
            "cash_weight": cash_weight,
            "total_value": total_value,
            "method": self.WEIGHTING_METHOD,
        }
