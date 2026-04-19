"""
Tradinator — IBKR Brokerage Adapter (placeholder).

Placeholder implementation of the BrokerAdapter protocol for Interactive
Brokers (IBKR).  All methods raise ``NotImplementedError`` until a future
PR wires up the ``ib_async`` library.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""


class IBKRBrokerAdapter:
    """IBKR implementation of the BrokerAdapter protocol (not yet implemented)."""

    def __init__(self, config: dict):
        self.config = config

    # ------------------------------------------------------------------
    # BrokerAdapter interface — all raise NotImplementedError for now.
    # ------------------------------------------------------------------

    def connect(self) -> dict:
        raise NotImplementedError("IBKR adapter is not yet implemented.")

    def get_account_info(self) -> dict:
        raise NotImplementedError("IBKR adapter is not yet implemented.")

    def get_positions(self) -> list[dict]:
        raise NotImplementedError("IBKR adapter is not yet implemented.")

    def fetch_historical_prices(
        self, instrument_id: str, resolution: str, lookback: int
    ) -> list[dict]:
        raise NotImplementedError("IBKR adapter is not yet implemented.")

    def fetch_instrument_info(self, instrument_id: str) -> dict:
        raise NotImplementedError("IBKR adapter is not yet implemented.")

    def open_position(
        self,
        instrument_id: str,
        direction: str,
        size: float,
        order_type: str,
        currency_code: str,
    ) -> dict:
        raise NotImplementedError("IBKR adapter is not yet implemented.")

    def close_position(
        self,
        deal_id: str,
        direction: str,
        instrument_id: str,
        size: float,
        order_type: str,
    ) -> dict:
        raise NotImplementedError("IBKR adapter is not yet implemented.")

    def confirm_deal(self, deal_reference: str) -> dict:
        raise NotImplementedError("IBKR adapter is not yet implemented.")
