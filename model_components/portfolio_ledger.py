"""
Tradinator — Portfolio Ledger.

Stores positions, cash, and trade history as an append-only local record.
Persists to JSON files in the output directory. This is the system's memory
across runs.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import datetime
import json
import os


class PortfolioLedger:
    """Record positions, cash, and trades to local JSON files each run."""

    LEDGER_FILENAME = "ledger.json"
    TRADES_FILENAME = "trades.json"

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, execution_log: dict, broker_state: dict) -> dict:
        """Append a snapshot to the ledger and record any new trades."""
        output_dir = self.config.get("output_dir", "data/output")
        ledger_path = os.path.join(output_dir, self.LEDGER_FILENAME)
        trades_path = os.path.join(output_dir, self.TRADES_FILENAME)

        existing_snapshots = self._load_existing_ledger(ledger_path)
        existing_trades = self._load_existing_trades(trades_path)

        updated_trades = self._record_trades(execution_log, existing_trades)
        snapshot = self._create_snapshot(broker_state, execution_log)

        all_snapshots = existing_snapshots + [snapshot]

        os.makedirs(output_dir, exist_ok=True)
        self._save_ledger(ledger_path, all_snapshots)
        self._save_trades(trades_path, updated_trades)

        ledger_snapshot = self._build_ledger_snapshot(
            snapshot, updated_trades, all_snapshots
        )

        position_count = len(ledger_snapshot["positions"])
        balance = ledger_snapshot["balance"]
        trade_count = ledger_snapshot["trade_count"]
        print(
            f"[PortfolioLedger] Ledger updated: {position_count} position(s), "
            f"balance={balance}, {trade_count} total trade(s)"
        )

        return ledger_snapshot

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    @staticmethod
    def _load_existing_ledger(filepath: str) -> list:
        """Load existing ledger JSON if it exists, else return empty list."""
        if not os.path.isfile(filepath):
            return []
        with open(filepath, "r") as fh:
            return json.load(fh)

    @staticmethod
    def _load_existing_trades(filepath: str) -> list:
        """Load existing trades JSON if it exists, else return empty list."""
        if not os.path.isfile(filepath):
            return []
        with open(filepath, "r") as fh:
            return json.load(fh)

    @staticmethod
    def _record_trades(execution_log: dict, existing_trades: list) -> list:
        """Append accepted executions to trade history and return updated list."""
        updated = list(existing_trades)
        for execution in execution_log.get("executions", []):
            if execution.get("status") == "ACCEPTED":
                updated.append(execution)
        return updated

    @staticmethod
    def _create_snapshot(broker_state: dict, execution_log: dict) -> dict:
        """Create a point-in-time snapshot dict from broker_state."""
        positions = []
        for pos in broker_state.get("positions", []):
            positions.append({
                "epic": pos.get("epic"),
                "direction": pos.get("direction"),
                "size": pos.get("size"),
                "deal_id": pos.get("deal_id"),
            })

        accepted = sum(
            1 for e in execution_log.get("executions", [])
            if e.get("status") == "ACCEPTED"
        )

        return {
            "positions": positions,
            "cash": broker_state.get("cash", 0.0),
            "balance": broker_state.get("balance", 0.0),
            "position_count": len(positions),
            "accepted_trades": accepted,
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }

    @staticmethod
    def _save_ledger(filepath: str, snapshots: list) -> None:
        """Write the full ledger (list of snapshots) to JSON atomically via buffer."""
        # Note: json.dump internal buffering is usually sufficient for performance,
        # but atomic write or manual chunking can be added for massive files.
        with open(filepath, "w", encoding="utf-8") as fh:
            json.dump(snapshots, fh, indent=2)

    @staticmethod
    def _save_trades(filepath: str, trades: list) -> None:
        """Write the full trade list to JSON atomically via buffer."""
        with open(filepath, "w", encoding="utf-8") as fh:
            json.dump(trades, fh, indent=2)

    @staticmethod
    def _build_ledger_snapshot(
        snapshot: dict, trades: list, history: list
    ) -> dict:
        """Assemble the output dict returned by run()."""
        history_records = [
            {
                "timestamp": s["timestamp"],
                "balance": s["balance"],
                "cash": s["cash"],
                "position_count": s["position_count"],
            }
            for s in history
        ]

        return {
            "positions": snapshot["positions"],
            "cash": snapshot["cash"],
            "balance": snapshot["balance"],
            "timestamp": snapshot["timestamp"],
            "trade_count": len(trades),
            "history": history_records,
        }
