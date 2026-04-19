"""
Tradinator — Broker Connector.

Thin orchestrator that selects the appropriate broker adapter based on
configuration, connects to the brokerage, and assembles the broker_state
dict consumed by all downstream pipeline components.

The raw broker client is never exposed — only the adapter instance is
passed through ``broker_state["adapter"]`` so that DataPipeline and
OrderExecutor can call normalised methods.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

from .ig_adapter import IGBrokerAdapter


# Registry of supported brokers — add new adapters here.
_ADAPTER_REGISTRY = {
    "ig": IGBrokerAdapter,
}

# Lazy import guard for optional IBKR adapter so that ``ib_async`` is not
# required unless the user explicitly selects the ``ibkr`` broker.
try:
    from .ibkr_adapter import IBKRBrokerAdapter  # noqa: F401
    _ADAPTER_REGISTRY["ibkr"] = IBKRBrokerAdapter
except ImportError:
    pass


class BrokerConnector:
    """Select broker adapter, connect, and build broker_state."""

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self) -> dict:
        """Connect to the configured broker and return broker_state."""
        adapter = self._create_adapter()
        connection = adapter.connect()
        account_info = adapter.get_account_info()
        positions = adapter.get_positions()
        instruments = list(self.config.get("universe", []))
        return self._build_broker_state(
            adapter, positions, account_info, instruments, connection
        )

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _create_adapter(self):
        """Instantiate the broker adapter named in config (default ``ig``)."""
        broker_name = self.config.get("broker", "ig").lower()
        adapter_cls = _ADAPTER_REGISTRY.get(broker_name)
        if adapter_cls is None:
            available = ", ".join(sorted(_ADAPTER_REGISTRY))
            raise RuntimeError(
                f"Unknown broker '{broker_name}'. "
                f"Available brokers: {available}"
            )
        return adapter_cls(self.config)

    @staticmethod
    def _build_broker_state(adapter, positions, account_info, instruments, connection):
        """Assemble the broker_state dict consumed by downstream components."""
        return {
            "adapter": adapter,
            "account_id": connection.get("account_id", ""),
            "positions": positions,
            "cash": account_info["cash"],
            "balance": account_info["balance"],
            "instruments": instruments,
        }
