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

# Registry of supported brokers — add new adapters here.
_ADAPTER_REGISTRY: dict = {}

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
        self._adapter = None
        self._connection: dict = {}

    def run(self) -> dict:
        """Connect to the configured broker (once) and return broker_state.

        The adapter is cached across calls so that a single IBKR session
        (one clientId) is reused for the lifetime of the BrokerConnector.
        Re-creating the adapter on every call would request a second
        connection with the same clientId and trigger IBKR error 326
        ("client id already in use").
        """
        if self._adapter is None or not self._is_adapter_alive(self._adapter):
            self._adapter = self._create_adapter()
            self._connection = self._adapter.connect()
        adapter = self._adapter
        print("[BrokerConnector][DIAG] calling get_account_info()...")
        account_info = adapter.get_account_info()
        print(f"[BrokerConnector][DIAG] get_account_info() returned: {account_info}")
        print("[BrokerConnector][DIAG] calling get_positions()...")
        positions = adapter.get_positions()
        print(f"[BrokerConnector][DIAG] get_positions() returned {len(positions)} position(s)")
        instruments = list(self.config.get("universe", []))
        return self._build_broker_state(
            adapter, positions, account_info, instruments, self._connection
        )

    @staticmethod
    def _is_adapter_alive(adapter) -> bool:
        """Return True if the adapter still holds an active broker session."""
        ib = getattr(adapter, "_ib", None)
        if ib is None:
            return False
        is_connected = getattr(ib, "isConnected", None)
        try:
            return bool(is_connected()) if callable(is_connected) else True
        except Exception:
            return False

    def close(self) -> None:
        """Disconnect the cached adapter, if any. Safe to call multiple times.

        Releases the IBKR clientId so the next process start can reuse it
        without hitting error 326 during the broker's session-cleanup window.
        """
        adapter = self._adapter
        if adapter is None:
            return
        ib = getattr(adapter, "_ib", None)
        if ib is not None:
            try:
                if getattr(ib, "isConnected", lambda: False)():
                    ib.disconnect()
            except Exception:
                pass
        self._adapter = None
        self._connection = {}

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _create_adapter(self):
        """Instantiate the broker adapter named in config; supports ``ibkr`` (default)."""
        broker_name = self.config.get("broker", "ibkr").lower()
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
