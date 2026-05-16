"""
Tradinator — IBKR Brokerage Adapter.

Implements the BrokerAdapter protocol for Interactive Brokers using the
``ib_insync`` library.  All IB-specific API calls, response parsing,
credential handling, and paper-trading enforcement live here.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import os
from datetime import datetime

from dotenv import load_dotenv

# Guard: import cleanly even when ib_insync is absent.
try:
    from ib_insync import IB, Contract, MarketOrder, Trade
    _IB_AVAILABLE = True
except ImportError:  # pragma: no cover
    IB = None  # type: ignore[assignment,misc]
    Contract = None  # type: ignore[assignment,misc]
    MarketOrder = None  # type: ignore[assignment,misc]
    Trade = None  # type: ignore[assignment,misc]
    _IB_AVAILABLE = False

# Three dirname calls: model_components → model → Tradinator (project root).
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

# Resolution labels understood by this adapter → IBKR bar-size strings.
_RESOLUTION_MAP: dict[str, str] = {
    "DAY": "1 day",
    "HOUR": "1 hour",
    "MIN": "1 min",
}

# Hard-coded contract lookup for common instruments.
# Keys are canonical symbol strings used throughout Tradinator.
def _build_contract_map() -> dict:
    """Return a dict mapping canonical symbol → ib_insync Contract."""
    if not _IB_AVAILABLE:
        return {}
    return {
        # --- Indices (IND) ---
        # FTSE 100 is not directly available as secType='IND' on IBKR;
        # the nearest liquid proxy is the LIFFE futures contract 'Z' on ICEEUSOFT.
        "FTSE":   Contract(symbol="Z",      secType="FUT", exchange="ICEEUSOFT", currency="GBP"),
        "DAX":    Contract(symbol="DAX",    secType="IND", exchange="EUREX",     currency="EUR"),
        "CAC":    Contract(symbol="CAC40",  secType="IND", exchange="MONEP",     currency="EUR"),
        "IBEX":   Contract(symbol="IBEX35", secType="IND", exchange="MEFFRV",    currency="EUR"),
        "AEX":    Contract(symbol="EOE",    secType="IND", exchange="FTA",       currency="EUR"),
        "SMI":    Contract(symbol="SMI",    secType="IND", exchange="SOFFEX",    currency="CHF"),
        "OMX":    Contract(symbol="OMX",    secType="IND", exchange="OMS",       currency="SEK"),
        "NIKKEI": Contract(symbol="N225",   secType="IND", exchange="OSE.JPN",   currency="JPY"),
        "HSI":    Contract(symbol="HSI",    secType="IND", exchange="HKFE",      currency="HKD"),
        # --- Forex (CASH) ---
        "EURUSD": Contract(symbol="EUR",    secType="CASH", exchange="IDEALPRO", currency="USD"),
        "GBPUSD": Contract(symbol="GBP",    secType="CASH", exchange="IDEALPRO", currency="USD"),
        "USDJPY": Contract(symbol="USD",    secType="CASH", exchange="IDEALPRO", currency="JPY"),
        "USDCHF": Contract(symbol="USD",    secType="CASH", exchange="IDEALPRO", currency="CHF"),
        "AUDUSD": Contract(symbol="AUD",    secType="CASH", exchange="IDEALPRO", currency="USD"),
        "NZDUSD": Contract(symbol="NZD",    secType="CASH", exchange="IDEALPRO", currency="USD"),
        "USDCAD": Contract(symbol="USD",    secType="CASH", exchange="IDEALPRO", currency="CAD"),
        # --- Futures ---
        "CL":     Contract(symbol="CL",     secType="FUT",  exchange="NYMEX",    currency="USD"),
        "GC":     Contract(symbol="GC",     secType="FUT",  exchange="COMEX",    currency="USD"),
    }


class IBKRBrokerAdapter:
    """IBKR implementation of the BrokerAdapter protocol via ib_insync."""

    PAPER_PORT = 4002   # TWS/IB Gateway paper-trading port

    def __init__(self, config: dict):
        """Initialise adapter; no network activity until connect() is called."""
        self.config = config
        self._ib: "IB | None" = None
        self._account_id: str = ""
        self._pending_trades: "dict[str, Trade]" = {}
        self._contract_map: dict = _build_contract_map()

    # ------------------------------------------------------------------
    # BrokerAdapter interface
    # ------------------------------------------------------------------

    def connect(self) -> dict:
        """Load credentials, enforce paper-only policy, and connect to IB."""
        if not _IB_AVAILABLE:
            raise RuntimeError(
                "ib_insync is not installed; cannot connect to IBKR."
            )
        load_dotenv(
            os.path.join(PROJECT_ROOT, "secrets", ".env"), override=True
        )
        host = os.environ.get("IBKR_HOST", "127.0.0.1")
        port_str = os.environ.get("IBKR_PORT", str(self.PAPER_PORT))
        client_id_str = os.environ.get("IBKR_CLIENT_ID", "1")
        self._account_id = os.environ.get("IBKR_ACCOUNT_ID", "")

        try:
            port = int(port_str)
            client_id = int(client_id_str)
        except ValueError as exc:
            raise RuntimeError(
                f"IBKR_PORT and IBKR_CLIENT_ID must be integers; "
                f"got IBKR_PORT={port_str!r}, IBKR_CLIENT_ID={client_id_str!r}."
            ) from exc

        paper_only = os.environ.get("IBKR_PAPER_ONLY", "true").lower()
        if paper_only == "true" and port != self.PAPER_PORT:
            raise RuntimeError(
                f"IBKR_PAPER_ONLY=true but IBKR_PORT={port}; "
                f"paper trading requires port {self.PAPER_PORT}."
            )

        self._ib = IB()
        try:
            self._ib.connect(host, port, clientId=client_id)
        except Exception as exc:
            msg = (
                "Tradinator could not connect to IBKR.\n"
                f"{exc}\n"
                "Next steps:\n"
                "1. Start TWS or IB Gateway and confirm it listens on port 4002 (paper trading).\n"
                "2. Verify IBKR_HOST=127.0.0.1, IBKR_PORT=4002, IBKR_CLIENT_ID=1 in secrets/.env.\n"
                "3. Ensure no other session is using the same IBKR_CLIENT_ID."
            )
            raise RuntimeError(msg) from exc
        print(f"[IBKRBrokerAdapter] Connected to {host}:{port} (clientId={client_id})")
        return {"account_id": self._account_id}

    def get_account_info(self) -> dict:
        """Fetch TotalCashValue and NetLiquidation from the IB account summary."""
        ib = self._require_session()
        summary = ib.accountSummary(self._account_id)
        values: dict[str, float] = {}
        for item in summary:
            if item.tag == "TotalCashValue":
                values["cash"] = float(item.value)
            elif item.tag == "NetLiquidation":
                values["balance"] = float(item.value)
        return {
            "cash": values.get("cash", 0.0),
            "balance": values.get("balance", 0.0),
        }

    def get_positions(self) -> list[dict]:
        """Fetch open portfolio positions and normalise to adapter schema."""
        ib = self._require_session()
        raw = ib.portfolio()
        positions: list[dict] = []
        for pos in raw:
            if self._account_id and pos.account != self._account_id:
                continue
            positions.append({
                "instrument_id": pos.contract.symbol,
                "direction":     "BUY" if pos.position > 0 else "SELL",
                "size":          abs(float(pos.position)),
                "level":         float(pos.averageCost),
                "deal_id":       str(pos.contract.conId),
                "profit_loss":   float(pos.unrealizedPNL),
            })
        return positions

    def fetch_historical_prices(
        self, instrument_id: str, resolution: str, lookback: int
    ) -> list[dict]:
        """Fetch historical OHLCV bars for a given lookback window."""
        ib = self._require_session()
        contract = self._resolve_contract(instrument_id)
        bar_size = _RESOLUTION_MAP.get(resolution.upper(), "1 day")
        # Multiply by 2 to account for non-trading days (weekends, holidays)
        # so that at least `lookback` trading bars are returned.
        duration = f"{lookback * 2} D"
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow="MIDPOINT",
            useRTH=True,
        )
        return [_bar_to_dict(b) for b in bars]

    def fetch_historical_prices_by_date_range(
        self, instrument_id: str, resolution: str, from_date: str
    ) -> list[dict]:
        """Fetch historical OHLCV bars from from_date to now."""
        ib = self._require_session()
        contract = self._resolve_contract(instrument_id)
        bar_size = _RESOLUTION_MAP.get(resolution.upper(), "1 day")
        # Parse from_date; pad duration by 5 days to avoid off-by-one at boundaries.
        parsed = datetime.fromisoformat(from_date.rstrip("Z").split(".")[0])
        days_back = (datetime.now() - parsed).days + 5
        duration = f"{max(days_back, 1)} D"
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow="MIDPOINT",
            useRTH=True,
        )
        return [_bar_to_dict(b) for b in bars]

    def fetch_instrument_info(self, instrument_id: str) -> dict:
        """Fetch contract details and map to the standard instrument-info schema."""
        ib = self._require_session()
        contract = self._resolve_contract(instrument_id)
        details_list = ib.reqContractDetails(contract)
        dealing_enabled = bool(details_list)
        if details_list:
            det = details_list[0]
            cd = det.contract
            return {
                "instrument_name":    det.longName or instrument_id,
                "instrument_id":      instrument_id,
                "currency":           cd.currency or "Unknown",
                "min_deal_size":      float(det.minSize or 1.0),
                "max_deal_size":      None,
                "min_size_increment": float(det.sizeIncrement or 1.0),
                "scaling_factor":     1.0,
                "dealing_enabled":    dealing_enabled,
                "buy_allowed":        True,
                "sell_allowed":       True,
            }
        # Fallback when no details returned.
        return {
            "instrument_name":    instrument_id,
            "instrument_id":      instrument_id,
            "currency":           "Unknown",
            "min_deal_size":      1.0,
            "max_deal_size":      None,
            "min_size_increment": 1.0,
            "scaling_factor":     1.0,
            "dealing_enabled":    False,
            "buy_allowed":        True,
            "sell_allowed":       True,
        }

    def open_position(
        self,
        instrument_id: str,
        direction: str,
        size: float,
        order_type: str,
        currency_code: str,
    ) -> dict:
        """Place a market order to open a position."""
        ib = self._require_session()
        contract = self._resolve_contract(instrument_id)
        order = MarketOrder(direction.upper(), size)
        trade = ib.placeOrder(contract, order)
        self._pending_trades[str(trade.order.orderId)] = trade
        return {"deal_reference": str(trade.order.orderId)}

    def close_position(
        self,
        deal_id: str,
        direction: str,
        instrument_id: str,
        size: float,
        order_type: str,
    ) -> dict:
        """Place a market order to close a position identified by conId."""
        ib = self._require_session()
        # deal_id is conId string; build a minimal Contract for the close order.
        contract = Contract(conId=int(deal_id), exchange="SMART")
        order = MarketOrder(direction.upper(), size)
        trade = ib.placeOrder(contract, order)
        self._pending_trades[str(trade.order.orderId)] = trade
        return {"deal_reference": str(trade.order.orderId)}

    def confirm_deal(self, deal_reference: str) -> dict:
        """Poll a pending trade for up to 10 s and return its final status."""
        ib = self._require_session()
        trade = self._pending_trades.get(deal_reference)
        if trade is None:
            return {"status": "REJECTED", "deal_id": None}

        # Poll until done or timeout.
        for _ in range(10):
            if trade.isDone():
                break
            ib.sleep(1.0)

        status_str = trade.orderStatus.status
        if status_str in ("Filled", "Submitted", "PreSubmitted"):
            mapped = "ACCEPTED"
        elif status_str in ("Cancelled", "Inactive", "ApiCancelled"):
            mapped = "REJECTED"
        else:
            # Still working after timeout — treat as accepted working order.
            mapped = "ACCEPTED"

        deal_id = (
            str(trade.contract.conId) if mapped == "ACCEPTED" else None
        )
        return {"status": mapped, "deal_id": deal_id}

    def fetch_working_orders(self) -> list[dict]:
        """Return all Submitted/PreSubmitted orders currently open at IB."""
        ib = self._require_session()
        ib.sleep(0)  # pump the ib_insync event loop
        working: list[dict] = []
        for trade in ib.openTrades():
            if trade.orderStatus.status not in ("Submitted", "PreSubmitted"):
                continue
            working.append({
                # permId is the exchange-assigned permanent ID (stable across
                # sessions); orderId is session-local and used for deal_reference.
                "order_id":      str(trade.order.permId),
                "instrument_id": trade.contract.symbol,
                "direction":     trade.order.action,
                "size":          float(trade.order.totalQuantity),
                "order_type":    trade.order.orderType,
            })
        return working

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _require_session(self) -> "IB":
        """Return the active IB connection or raise if not yet connected."""
        if self._ib is None or not self._ib.isConnected():
            raise RuntimeError(
                "IBKR adapter not connected; call connect() first"
            )
        return self._ib

    def _resolve_contract(self, instrument_id: str) -> "Contract":
        """Map a canonical instrument symbol to an ib_insync Contract.

        Checks the hard-coded lookup table first; falls back to a generic
        US equity Contract when the symbol is not found.
        """
        if instrument_id in self._contract_map:
            return self._contract_map[instrument_id]
        # Default: treat unknown symbols as US equities on SMART routing.
        return Contract(
            symbol=instrument_id,
            secType="STK",
            exchange="SMART",
            currency="USD",
        )


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _bar_to_dict(bar) -> dict:
    """Convert an ib_insync BarData object to the standard OHLCV dict."""
    # IBKR returns -1 for volume on instruments where volume is unavailable
    # (e.g. indices and forex).
    raw_vol = getattr(bar, "volume", -1)
    volume = None if raw_vol == -1 else float(raw_vol)
    return {
        "open":      float(bar.open),
        "high":      float(bar.high),
        "low":       float(bar.low),
        "close":     float(bar.close),
        "volume":    volume,
        "timestamp": str(bar.date),
    }
