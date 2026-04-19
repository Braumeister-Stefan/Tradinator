"""
Tradinator — Broker Adapter Protocol.

Defines the interface that every brokerage adapter must implement.
All methods return plain dicts/lists — no broker-specific types leak
into the rest of the pipeline.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class BrokerAdapter(Protocol):
    """Contract that every brokerage adapter must satisfy.

    Each method returns normalised data structures so that the pipeline
    components (DataPipeline, OrderExecutor, etc.) never depend on a
    specific broker's SDK or response schema.
    """

    def connect(self) -> dict:
        """Authenticate and establish a broker session.

        Returns
        -------
        dict
            ``{"account_id": str}``
        """
        ...

    def get_account_info(self) -> dict:
        """Fetch account balance and available cash.

        Returns
        -------
        dict
            ``{"cash": float, "balance": float}``
        """
        ...

    def get_positions(self) -> list[dict]:
        """Fetch all open positions.

        Returns
        -------
        list[dict]
            Each dict contains::

                {
                    "instrument_id": str,
                    "direction":     str,   # "BUY" or "SELL"
                    "size":          float,
                    "level":         float,
                    "deal_id":       str,
                    "profit_loss":   float,
                }
        """
        ...

    def fetch_historical_prices(
        self, instrument_id: str, resolution: str, lookback: int
    ) -> list[dict]:
        """Fetch historical OHLCV price bars for a single instrument.

        Returns
        -------
        list[dict]
            Each dict contains::

                {
                    "close":     float | None,
                    "high":      float | None,
                    "low":       float | None,
                    "open":      float | None,
                    "volume":    float | None,
                    "bid_close": float | None,
                    "timestamp": str,
                }
        """
        ...

    def fetch_instrument_info(self, instrument_id: str) -> dict:
        """Fetch display name and currency for an instrument.

        Returns
        -------
        dict
            ``{"instrument_name": str, "instrument_id": str, "currency": str}``
        """
        ...

    def open_position(
        self,
        instrument_id: str,
        direction: str,
        size: float,
        order_type: str,
        currency_code: str,
    ) -> dict:
        """Place an order to open (or increase) a position.

        Returns
        -------
        dict
            ``{"deal_reference": str}``
        """
        ...

    def close_position(
        self,
        deal_id: str,
        direction: str,
        instrument_id: str,
        size: float,
        order_type: str,
    ) -> dict:
        """Place an order to close (or decrease) a position.

        Returns
        -------
        dict
            ``{"deal_reference": str}``
        """
        ...

    def confirm_deal(self, deal_reference: str) -> dict:
        """Check whether a deal was accepted or rejected.

        Returns
        -------
        dict
            ``{"status": str, "deal_id": str}``
            where *status* is ``"ACCEPTED"`` or ``"REJECTED"``.
        """
        ...
