"""
Tradinator — Order Executor.

Sends paper orders via the broker adapter and records whether each order
was accepted or rejected.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import datetime
import time


class OrderExecutor:
    """Send orders via the broker adapter and record acceptance/rejection."""

    EXECUTION_DELAY = 0.5   # seconds to wait between orders
    CURRENCY_CODE = "USD"   # default currency for orders

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, orders: dict, broker_state: dict) -> dict:
        """Execute every order in the list and return an execution log."""
        adapter = broker_state["adapter"]
        positions = broker_state.get("positions", [])
        order_list = orders.get("orders", [])

        executions = []
        for i, order in enumerate(order_list):
            result = self._execute_order(adapter, order, positions)
            executions.append(result)
            print(
                f"[OrderExecutor] {result['direction']} {result['instrument_id']} "
                f"x{result['size']} → {result['status']}"
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

    def _execute_order(self, adapter, order: dict, positions: list) -> dict:
        """Send a single order via the adapter and return an execution dict."""
        reason = order.get("reason", "")
        is_close = reason in ("close", "decrease")

        try:
            if is_close:
                deal_id = self._find_deal_id(order["instrument_id"], positions)
                original_direction = self._find_position_direction(
                    order["instrument_id"], positions
                )
                close_direction = "SELL" if original_direction == "BUY" else "BUY"
                resp = adapter.close_position(
                    deal_id=deal_id,
                    direction=close_direction,
                    instrument_id=order["instrument_id"],
                    size=order["size"],
                    order_type="MARKET",
                )
            else:
                resp = adapter.open_position(
                    instrument_id=order["instrument_id"],
                    direction=order["direction"],
                    size=order["size"],
                    order_type="MARKET",
                    currency_code=self.CURRENCY_CODE,
                )

            deal_reference = resp["deal_reference"]
            confirmation = adapter.confirm_deal(deal_reference)
            return {
                "instrument_id": order["instrument_id"],
                "direction": order["direction"],
                "size": order["size"],
                "status": confirmation["status"],
                "deal_reference": deal_reference,
                "deal_id": confirmation["deal_id"],
                "reason": reason,
                "timestamp": datetime.datetime.utcnow().isoformat(),
            }
        except Exception as exc:
            return {
                "instrument_id": order["instrument_id"],
                "direction": order["direction"],
                "size": order["size"],
                "status": "ERROR",
                "deal_reference": None,
                "deal_id": None,
                "reason": str(exc),
                "timestamp": datetime.datetime.utcnow().isoformat(),
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
    def _find_deal_id(instrument_id: str, positions: list) -> str:
        """Look up the deal_id for an instrument in the current positions list."""
        for pos in positions:
            if pos.get("instrument_id") == instrument_id:
                return pos["deal_id"]
        raise ValueError(f"No open position found for instrument {instrument_id}")

    @staticmethod
    def _find_position_direction(instrument_id: str, positions: list) -> str:
        """Look up the direction of the existing position for an instrument."""
        for pos in positions:
            if pos.get("instrument_id") == instrument_id:
                return pos["direction"]
        raise ValueError(f"No open position found for instrument {instrument_id}")
