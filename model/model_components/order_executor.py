"""
Tradinator — Order Executor.

Sends paper orders via the broker adapter and records whether each order
was accepted or rejected.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import datetime
import json
import os
import time


class OrderExecutor:
    """Send orders via the broker adapter and record acceptance/rejection."""

    EXECUTION_DELAY = 0.5   # seconds to wait between orders
    CURRENCY_CODE = "GBP"   # default currency for orders
    EXPIRY = "-"            # DFB (daily funded bet) / no expiry for CFDs
    ORDERBOOK_FILENAME = "orderbook.json"

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, orders: dict, broker_state: dict, market_data: dict = None) -> dict:
        """Execute every order in the list and return an execution log."""
        adapter = broker_state["adapter"]
        positions = broker_state.get("positions", [])
        order_list = orders.get("orders", [])
        metadata = market_data.get("metadata", {}) if market_data else {}
        orderbook = self._load_orderbook()

        executions = []
        for i, order in enumerate(order_list):
            result = self._execute_order(adapter, order, positions, metadata)
            executions.append(result)
            self._record_to_orderbook(orderbook, order, result)
            print(
                f"[OrderExecutor] {result['direction']} {result['instrument_id']} "
                f"x{result['size']} → {result['status']}"
            )
            if result.get("rejection_reason"):
                print(
                    f"[OrderExecutor] ⚠ REJECTED {result['instrument_id']}: "
                    f"{result['rejection_reason']}"
                )
            if i < len(order_list) - 1:
                time.sleep(self.EXECUTION_DELAY)

        execution_log = self._build_execution_log(executions)

        summary = execution_log["summary"]
        rejection_reasons = {}
        for e in executions:
            if e["status"] == "REJECTED" and e.get("rejection_reason"):
                r = e["rejection_reason"]
                rejection_reasons[r] = rejection_reasons.get(r, 0) + 1
        reject_breakdown = ", ".join(f"{v} {k}" for k, v in rejection_reasons.items())
        reject_suffix = f" ({reject_breakdown})" if reject_breakdown else ""
        print(
            f"[OrderExecutor] Executed {summary['total']} order(s): "
            f"{summary['accepted']} FILLED, {summary['rejected']} REJECTED{reject_suffix}"
        )
        self._save_orderbook(orderbook)

        return execution_log

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _execute_order(self, adapter, order: dict, positions: list, metadata: dict = None) -> dict:
        """Send a single order via the adapter and return an execution dict."""
        reason = order.get("reason", "")
        is_close = reason in ("close", "decrease")
        currency = metadata.get(order["instrument_id"], {}).get("currency", "GBP") if metadata else "GBP"

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
            elif order.get("order_type") == "LIMIT":
                resp = adapter.open_position(
                    instrument_id=order["instrument_id"],
                    direction=order["direction"],
                    size=order["size"],
                    order_type="LIMIT",
                    currency_code=currency,
                )
            else:
                resp = adapter.open_position(
                    instrument_id=order["instrument_id"],
                    direction=order["direction"],
                    size=order["size"],
                    order_type="MARKET",
                    currency_code=currency,
                )

            deal_reference = resp["deal_reference"]
            confirmation = adapter.confirm_deal(deal_reference)
            status = confirmation["status"]
            rejection_reason = confirmation.get("reason", "") if status == "REJECTED" else ""
            return {
                "instrument_id": order["instrument_id"],
                "direction": order["direction"],
                "size": order["size"],
                "status": status,
                "deal_reference": deal_reference,
                "deal_id": confirmation["deal_id"],
                "reason": rejection_reason if status == "REJECTED" else reason,
                "rejection_reason": rejection_reason,
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
                "rejection_reason": "",
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

    def _load_orderbook(self) -> dict:
        """Load the order book from disk, or return an empty one."""
        output_dir = self.config.get("output_dir", "data/output")
        path = os.path.join(output_dir, self.ORDERBOOK_FILENAME)
        if not os.path.isfile(path):
            return {"orders": [], "last_reconciled_at": None}
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            return {"orders": [], "last_reconciled_at": None}

    def _save_orderbook(self, orderbook: dict) -> None:
        """Persist the order book to disk."""
        output_dir = self.config.get("output_dir", "data/output")
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, self.ORDERBOOK_FILENAME)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(orderbook, fh, indent=2)

    def _record_to_orderbook(self, orderbook: dict, order: dict, result: dict) -> None:
        """Append an order record to the in-memory order book."""
        status = result.get("status", "ERROR")
        order_type = order.get("order_type", "MARKET")

        if status == "ACCEPTED" and order_type == "LIMIT":
            state = "WORKING"
        elif status == "ACCEPTED":
            state = "FILLED"
        elif status == "REJECTED":
            state = "CANCELLED"
        else:
            state = "CANCELLED"

        now = datetime.datetime.utcnow().isoformat()
        orderbook["orders"].append({
            "order_id": result.get("deal_reference") or now,
            "instrument_id": order.get("instrument_id", ""),
            "direction": order.get("direction", ""),
            "size": order.get("size", 0),
            "order_type": order_type,
            "limit_level": order.get("limit_level"),
            "time_in_force": order.get("time_in_force", "FILL_OR_KILL"),
            "state": state,
            "deal_reference": result.get("deal_reference"),
            "deal_id": result.get("deal_id"),
            "reason": result.get("reason", ""),
            "created_at": now,
            "updated_at": now,
        })

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
