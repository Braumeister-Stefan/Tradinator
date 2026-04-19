"""
Tradinator — Reconciliation.

Synchronises the local order book with the IG broker's working orders,
updating states and reflecting fills, cancellations, and expirations.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import datetime
import json
import os


class Reconciliation:
    """Reconcile local order book against live broker state."""

    ORDERBOOK_FILENAME = "orderbook.json"

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, broker_state: dict) -> dict:
        """Check working orders against the broker and update the local order book."""
        output_dir = self.config.get("output_dir", "data/output")
        orderbook = self._load_orderbook(output_dir)

        working_orders = [
            o for o in orderbook.get("orders", [])
            if o.get("state") == "WORKING"
        ]

        if not working_orders:
            print("[Reconciliation] No working orders to reconcile.")
            return broker_state

        adapter = broker_state.get("adapter")
        if adapter is None:
            print("[Reconciliation] ⚠ No broker adapter, skipping reconciliation.")
            return broker_state

        # Access the raw broker session for working-order queries.
        # This is IG-specific; other adapters will skip reconciliation
        # until their adapter exposes equivalent functionality.
        ig = getattr(adapter, "_ig", None)
        if ig is None:
            print("[Reconciliation] ⚠ No broker session available, skipping reconciliation.")
            return broker_state

        try:
            live_orders = ig.fetch_working_orders()
            live_deal_ids = set()
            for wo in live_orders.get("workingOrders", []):
                wd = wo.get("workingOrderData", {})
                live_deal_ids.add(wd.get("dealId", ""))
        except Exception as exc:
            print(f"[Reconciliation] ⚠ Could not fetch working orders: {exc}")
            return broker_state

        n_updated = 0
        n_working = 0
        now = datetime.datetime.utcnow().isoformat()

        position_deal_ids = {
            p.get("deal_id") for p in broker_state.get("positions", [])
            if p.get("deal_id")
        }

        for order in orderbook.get("orders", []):
            if order.get("state") != "WORKING":
                continue
            deal_id = order.get("deal_id")
            if not deal_id:
                # Cannot reconcile without a deal_id; skip.
                n_working += 1
                continue
            if deal_id in live_deal_ids:
                n_working += 1
            else:
                # Order disappeared from working list.  Check positions
                # to distinguish a fill from a cancellation/expiry.
                # Note: IG may assign a new deal_id on fill, so this
                # check is best-effort; the fallback is CANCELLED.
                if deal_id in position_deal_ids:
                    order["state"] = "FILLED"
                else:
                    order["state"] = "CANCELLED"
                order["updated_at"] = now
                n_updated += 1

        orderbook["last_reconciled_at"] = now
        self._save_orderbook(output_dir, orderbook)

        print(
            f"[Reconciliation] Reconciled: {n_updated} order(s) updated, "
            f"{n_working} still working."
        )
        return broker_state

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _load_orderbook(self, output_dir: str) -> dict:
        """Load order book from disk, or return empty structure."""
        path = os.path.join(output_dir, self.ORDERBOOK_FILENAME)
        if not os.path.isfile(path):
            return {"orders": [], "last_reconciled_at": None}
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            return {"orders": [], "last_reconciled_at": None}

    def _save_orderbook(self, output_dir: str, orderbook: dict) -> None:
        """Write order book to disk."""
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, self.ORDERBOOK_FILENAME)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(orderbook, fh, indent=2)
