"""
Tradinator — Order Executor.

Sends paper orders to the IG demo environment via the broker session and
records whether each order was accepted or rejected.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import datetime
import time


class OrderExecutor:
    """Send orders to IG and record acceptance/rejection for each."""

    EXECUTION_DELAY = 0.5   # seconds to wait between orders
    CURRENCY_CODE = "USD"   # default currency for orders
    EXPIRY = "-"            # DFB (daily funded bet) / no expiry for CFDs

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, orders: dict, broker_state: dict, market_data: dict = None) -> dict:
        """Execute every order in the list and return an execution log."""
        ig = broker_state["session"]
        positions = broker_state.get("positions", [])
        order_list = orders.get("orders", [])
        metadata = market_data.get("metadata", {}) if market_data else {}

        executions = []
        for i, order in enumerate(order_list):
            result = self._execute_order(ig, order, positions, metadata)
            executions.append(result)
            print(
                f"[OrderExecutor] {result['direction']} {result['epic']} "
                f"x{result['size']} → {result['status']}"
            )
            if result.get("rejection_reason"):
                print(
                    f"[OrderExecutor] ⚠ REJECTED {result['epic']}: "
                    f"{result['rejection_reason']}"
                )
            if i < len(order_list) - 1:
                time.sleep(self.EXECUTION_DELAY)

        execution_log = self._build_execution_log(executions)

        summary = execution_log["summary"]
        print(
            f"[OrderExecutor] Done — {summary['total']} order(s): "
            f"{summary['accepted']} accepted, {summary['rejected']} rejected, "
            f"{summary['errors']} error(s)"
        )

        return execution_log

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _execute_order(self, ig, order: dict, positions: list, metadata: dict = None) -> dict:
        """Send a single order to IG and return an execution dict."""
        reason = order.get("reason", "")
        is_close = reason in ("close", "decrease")

        try:
            if is_close:
                deal_reference = self._close_position(ig, order, positions)
            else:
                deal_reference = self._open_position(ig, order, metadata or {})

            confirmation = self._confirm_deal(ig, deal_reference)
            broker_reason = confirmation.get("reason", "")
            status = confirmation["status"]
            rejection_reason = broker_reason if status == "REJECTED" else ""
            return {
                "epic": order["epic"],
                "direction": order["direction"],
                "size": order["size"],
                "status": status,
                "deal_reference": deal_reference,
                "deal_id": confirmation["deal_id"],
                "reason": broker_reason if status == "REJECTED" else reason,
                "rejection_reason": rejection_reason,
                "timestamp": datetime.datetime.utcnow().isoformat(),
            }
        except Exception as exc:
            return {
                "epic": order["epic"],
                "direction": order["direction"],
                "size": order["size"],
                "status": "ERROR",
                "deal_reference": None,
                "deal_id": None,
                "reason": str(exc),
                "rejection_reason": "",
                "timestamp": datetime.datetime.utcnow().isoformat(),
            }

    def _open_position(self, ig, order: dict, metadata: dict = None) -> str:
        """Open a new position via the IG API and return the deal reference."""
        meta = (metadata or {}).get(order["epic"], {})
        currency = meta.get("currency", self.CURRENCY_CODE)
        if currency == "Unknown":
            currency = self.CURRENCY_CODE
        response = ig.create_open_position(
            currency_code=currency,
            direction=order["direction"],
            epic=order["epic"],
            expiry=self.EXPIRY,
            force_open=True,
            guaranteed_stop=False,
            level=None,
            limit_distance=None,
            limit_level=None,
            order_type="MARKET",
            quote_id=None,
            size=order["size"],
            stop_distance=None,
            stop_level=None,
            trailing_stop=False,
            trailing_stop_increment=None,
        )
        return response["dealReference"]

    def _close_position(self, ig, order: dict, positions: list) -> str:
        """Close (fully or partially) an existing position and return the deal reference."""
        deal_id = self._find_deal_id(order["epic"], positions)
        original_direction = self._find_position_direction(
            order["epic"], positions
        )
        close_direction = "SELL" if original_direction == "BUY" else "BUY"

        response = ig.close_open_position(
            deal_id=deal_id,
            direction=close_direction,
            epic=order["epic"],
            expiry=self.EXPIRY,
            level=None,
            order_type="MARKET",
            quote_id=None,
            size=order["size"],
        )
        return response["dealReference"]

    def _confirm_deal(self, ig, deal_reference: str) -> dict:
        """Fetch deal confirmation and return status and deal_id."""
        confirmation = ig.fetch_deal_by_deal_reference(deal_reference)
        return {
            "status": confirmation.get("dealStatus", "REJECTED"),
            "deal_id": confirmation.get("dealId"),
            "reason": confirmation.get("reason", ""),
        }

    def _build_execution_log(self, executions: list) -> dict:
        """Assemble the execution log with per-order results and a summary."""
        accepted = sum(1 for e in executions if e["status"] == "ACCEPTED")
        rejected = sum(1 for e in executions if e["status"] == "REJECTED")
        errors = sum(1 for e in executions if e["status"] == "ERROR")
        return {
            "executions": executions,
            "summary": {
                "total": len(executions),
                "accepted": accepted,
                "rejected": rejected,
                "errors": errors,
            },
        }

    @staticmethod
    def _find_deal_id(epic: str, positions: list) -> str:
        """Look up the deal_id for an epic in the current positions list."""
        for pos in positions:
            if pos.get("epic") == epic:
                return pos["deal_id"]
        raise ValueError(f"No open position found for epic {epic}")

    @staticmethod
    def _find_position_direction(epic: str, positions: list) -> str:
        """Look up the direction of the existing position for an epic."""
        for pos in positions:
            if pos.get("epic") == epic:
                return pos["direction"]
        raise ValueError(f"No open position found for epic {epic}")
