"""
Tradinator — IBKR Brokerage Adapter.

Implements the BrokerAdapter protocol for Interactive Brokers using the
``ib_insync`` library.  All IB-specific API calls, response parsing,
credential handling, and paper-trading enforcement live here.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import logging
import os
from datetime import datetime

from dotenv import load_dotenv

# Suppress known-benign ib_insync connect-time log noise.
#
# Root cause of the previous failure: Python's logging.Filter on a logger only
# filters records emitted *directly* by that logger.  Records that propagate up
# from child loggers are NOT re-filtered by the parent — only by the parent's
# *handlers*.  Attaching the filter to "ib_insync" therefore had no effect on
# records from "ib_insync.wrapper" or "ib_insync.ib".
#
# Fix: attach to the exact child loggers that emit the unwanted records.
#   ib_insync.wrapper — Error 321 "Group name cannot be null"
#                       (non-FA paper account, fired by reqAccountSummary("All"))
#   ib_insync.ib      — "positions request timed out" /
#                       "account updates request timed out"
#                       (startup subscriptions that never complete on paper)
class _SuppressIbConnectNoise(logging.Filter):
    _SUPPRESSED = (
        "Error 321,",
        "positions request timed out",
        "account updates request timed out",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not any(s in msg for s in self._SUPPRESSED)


_noise_filter = _SuppressIbConnectNoise()
logging.getLogger("ib_insync.wrapper").addFilter(_noise_filter)
logging.getLogger("ib_insync.ib").addFilter(_noise_filter)

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
        "SPX":    Contract(symbol="SPX",    secType="IND", exchange="CBOE",      currency="USD"),
        "NDX":    Contract(symbol="NDX",    secType="IND", exchange="NASDAQ",    currency="USD"),
        "RUT":    Contract(symbol="RUT",    secType="IND", exchange="RUSSELL",   currency="USD"),
        # FTSE 100 futures trade on ICE Futures Europe (exchange code ICEEU).
        # No expiry is set; _resolve_contract qualifies this stub to the front-month.
        "FTSE":   Contract(symbol="Z",      secType="FUT", exchange="ICEEU",     currency="GBP"),
        "DAX":    Contract(symbol="DAX",    secType="IND", exchange="EUREX",     currency="EUR"),
        "CAC":    Contract(symbol="CAC40",  secType="IND", exchange="MONEP",     currency="EUR"),
        "IBEX":   Contract(symbol="IBEX35", secType="IND", exchange="MEFFRV",    currency="EUR"),
        "AEX":    Contract(symbol="EOE",    secType="IND", exchange="FTA",       currency="EUR"),
        "SMI":    Contract(symbol="SMI",    secType="IND", exchange="EBS",       currency="CHF"),
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
        # Session-level cache for named-contract qualification.
        # Values: qualified Contract on success, None when IBKR returned no definition.
        self._qualified_named: "dict[str, Contract | None]" = {}

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
        # Cap all blocking req* calls so they cannot hang indefinitely.
        self._ib.RequestTimeout = 30
        # If no account ID was configured, derive it from the managed accounts
        # list that IBKR sends at connect time.
        if not self._account_id:
            managed = self._ib.managedAccounts()
            if managed:
                self._account_id = managed[0]
        # Poll until the accountValues cache is populated or until the deadline.
        # On paper accounts the startup reqAccountUpdates subscription often
        # times out — the log noise is suppressed — leaving the cache empty if
        # we simply sleep a fixed 5 s.  Polling detects population reliably.
        _ACCOUNT_READY_TIMEOUT = 30  # seconds
        _deadline = _ACCOUNT_READY_TIMEOUT
        while _deadline > 0:
            if self._ib.accountValues(self._account_id):
                break
            self._ib.sleep(1)
            _deadline -= 1
        else:
            print(
                "[IBKRBrokerAdapter][WARN] accountValues cache still empty after "
                f"{_ACCOUNT_READY_TIMEOUT}s; account info will read as 0."
            )
        print(f"[IBKRBrokerAdapter] Connected to {host}:{port} (clientId={client_id})")
        return {"account_id": self._account_id}

    def get_account_info(self) -> dict:
        """Fetch TotalCashValue and NetLiquidation from the IB account values cache."""
        ib = self._require_session()
        # accountValues() is a non-blocking cache read (populated by the
        # reqAccountUpdates subscription started during connect).
        # accountSummary() is blocking on first call and hangs on paper accounts.
        account_vals = ib.accountValues(self._account_id)
        # Prefer the BASE-currency aggregate; fall back to any currency so that
        # a non-BASE paper account (e.g. USD-denominated with no BASE tag) still
        # returns real data instead of silently yielding 0.0.
        base_vals: dict[str, float] = {}
        any_vals: dict[str, float] = {}
        for item in account_vals:
            if item.tag == "TotalCashValue":
                any_vals.setdefault("cash", float(item.value))
                if item.currency == "BASE":
                    base_vals["cash"] = float(item.value)
            elif item.tag == "NetLiquidation":
                any_vals.setdefault("balance", float(item.value))
                if item.currency == "BASE":
                    base_vals["balance"] = float(item.value)
        values = base_vals if base_vals else any_vals
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
                "conId": pos.contract.symbol,
                "direction":     "BUY" if pos.position > 0 else "SELL",
                "size":          abs(float(pos.position)),
                "level":         float(pos.averageCost),
                "deal_id":       str(pos.contract.conId),
                "profit_loss":   float(pos.unrealizedPNL),
            })
        return positions

    def fetch_historical_prices(
        self, conId: str, resolution: str, lookback: int
    ) -> list[dict]:
        """Fetch historical OHLCV bars for a given lookback window."""
        ib = self._require_session()
        contract = self._resolve_contract(conId)
        bar_size = _RESOLUTION_MAP.get(resolution.upper(), "1 day")
        # Multiply by 2 to account for non-trading days (weekends, holidays)
        # so that at least `lookback` trading bars are returned.
        duration = f"{lookback * 2} D"
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=_what_to_show(contract),
            useRTH=True,
        )
        return [_bar_to_dict(b) for b in bars]

    def fetch_historical_prices_by_date_range(
        self, conId: str, resolution: str, from_date: str
    ) -> list[dict]:
        """Fetch historical OHLCV bars from from_date to now."""
        ib = self._require_session()
        contract = self._resolve_contract(conId)
        bar_size = _RESOLUTION_MAP.get(resolution.upper(), "1 day")
        # Parse from_date; pad duration by 5 days to avoid off-by-one at boundaries.
        parsed = datetime.fromisoformat(from_date.rstrip("Z").split(".")[0])
        days_back = max((datetime.now() - parsed).days + 5, 1)
        # IBKR reqHistoricalData rejects durationStr in days when the span
        # exceeds 365 days (error 321 — "must be made in years"). Switch to
        # the Y unit, rounding up so the requested window is fully covered.
        if days_back > 365:
            years = (days_back + 364) // 365
            duration = f"{years} Y"
        else:
            duration = f"{days_back} D"
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=_what_to_show(contract),
            useRTH=True,
        )
        return [_bar_to_dict(b) for b in bars]

    def fetch_instrument_info(self, conId: str) -> dict:
        """Fetch contract details and map to the standard instrument-info schema."""
        ib = self._require_session()
        contract = self._resolve_contract(conId)
        details_list = ib.reqContractDetails(contract)
        dealing_enabled = bool(details_list)
        # Fractional-share trading via the API requires the IBKR account to be
        # explicitly enabled for it. When not enabled (the default), submitting a
        # non-integer share quantity for STK returns error 10243 and the order
        # is cancelled. Force the size increment / min deal size to whole shares
        # so OrderGenerator's rounding produces integer quantities.
        allow_fractional = bool(self.config.get("allow_fractional_shares", False))
        if details_list:
            det = details_list[0]
            cd = det.contract
            raw_increment = float(det.sizeIncrement or 1.0)
            raw_min_size = float(det.minSize or 1.0)
            if cd.secType == "STK" and not allow_fractional:
                size_increment = max(raw_increment, 1.0)
                min_deal_size = max(raw_min_size, 1.0)
            else:
                size_increment = raw_increment
                min_deal_size = raw_min_size
            return {
                "instrument_name":    det.longName or conId,
                "conId":      str(cd.conId),
                "currency":           cd.currency or "Unknown",
                "min_deal_size":      min_deal_size,
                "max_deal_size":      None,
                "min_size_increment": size_increment,
                "scaling_factor":     1.0,
                "dealing_enabled":    dealing_enabled,
                "buy_allowed":        True,
                "sell_allowed":       True,
            }
        # Fallback when no details returned.
        return {
            "instrument_name":    conId,
            "conId":      conId,
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
        conId: str,
        direction: str,
        size: float,
        order_type: str,
        currency_code: str,
    ) -> dict:
        """Place a market order to open a position."""
        ib = self._require_session()
        contract = self._resolve_contract(conId)
        order = MarketOrder(direction.upper(), size, tif=self.config.get("tif", "DAY"))
        trade = ib.placeOrder(contract, order)
        self._pending_trades[str(trade.order.orderId)] = trade
        return {"deal_reference": str(trade.order.orderId)}

    def close_position(
        self,
        deal_id: str,
        direction: str,
        conId: str,
        size: float,
        order_type: str,
    ) -> dict:
        """Place a market order to close a position identified by conId."""
        ib = self._require_session()
        # deal_id is conId string; build a minimal Contract for the close order.
        contract = Contract(conId=int(deal_id), exchange="SMART")
        order = MarketOrder(direction.upper(), size, tif=self.config.get("tif", "DAY"))
        trade = ib.placeOrder(contract, order)
        self._pending_trades[str(trade.order.orderId)] = trade
        return {"deal_reference": str(trade.order.orderId)}

    def confirm_deal(self, deal_reference: str) -> dict:
        """Poll a pending trade for up to 10 s and return its final status.

        Only orders that have reached the exchange (Submitted) or filled are
        treated as ACCEPTED. PreSubmitted at timeout means the order is still
        held by IB (e.g. market closed) and has not been routed — reporting
        it as ACCEPTED would record a phantom position, so it is REJECTED.
        """
        ib = self._require_session()
        trade = self._pending_trades.get(deal_reference)
        if trade is None:
            return {"status": "REJECTED", "deal_id": None, "reason": "unknown deal_reference"}

        # Poll until done or timeout.
        for _ in range(10):
            if trade.isDone():
                break
            ib.sleep(1.0)

        status_str = trade.orderStatus.status
        if status_str in ("Filled", "Submitted"):
            return {
                "status": "ACCEPTED",
                "deal_id": str(trade.contract.conId),
                "reason": "",
            }
        if status_str in ("Cancelled", "Inactive", "ApiCancelled"):
            last_msg = trade.log[-1].message if trade.log else ""
            return {
                "status": "REJECTED",
                "deal_id": None,
                "reason": last_msg or f"order {status_str.lower()} by broker",
            }
        # PreSubmitted or any other non-terminal state after timeout: order is
        # not live at the exchange. Do not record a position.
        return {
            "status": "REJECTED",
            "deal_id": None,
            "reason": f"order not routed (status={status_str}); market likely closed",
        }

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
                "conId": trade.contract.symbol,
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

    def _resolve_contract(self, conId: str) -> "Contract":
        """Map a canonical instrument identifier to an ib_insync Contract.

        Resolution order:
        1. Hard-coded lookup table (named symbols like "SPX", "DAX", "EURUSD").
           All named contracts are validated once per session via
           ``reqContractDetails``.  FUT stubs without an expiry are qualified
           to the front-month.  If IBKR returns no security definition a
           ``ValueError`` is raised so the pipeline removes the instrument.
        2. Numeric strings are treated as IBKR contract IDs and resolved via
           ``ib.qualifyContracts(Contract(conId=..., exchange="SMART"))``.
        3. Otherwise fall back to a generic US-equity ticker on SMART routing.
        """
        if conId in self._contract_map:
            # Return cached result from a prior qualify call this session.
            if conId in self._qualified_named:
                cached = self._qualified_named[conId]
                if cached is None:
                    raise ValueError(
                        f"No security definition for '{conId}' (cached from earlier failure)"
                    )
                return cached
            # First use: validate via reqContractDetails.
            stub = self._contract_map[conId]
            ib = self._require_session()
            try:
                details = ib.reqContractDetails(stub)
            except Exception as exc:
                self._qualified_named[conId] = None
                raise ValueError(
                    f"reqContractDetails failed for '{conId}': {exc}"
                ) from exc
            if not details:
                self._qualified_named[conId] = None
                raise ValueError(
                    f"No security definition found for '{conId}' "
                    f"({stub.secType}@{stub.exchange}) — "
                    "instrument removed from universe."
                )
            qualified = details[0].contract
            self._qualified_named[conId] = qualified
            return qualified
        if conId.isdigit():
            stub = Contract(conId=int(conId), exchange="SMART")
            ib = self._require_session()
            qualified = ib.qualifyContracts(stub)
            if qualified:
                return qualified[0]
            return stub
        # Default: treat unknown non-numeric identifiers as US-equity tickers.
        return Contract(
            symbol=conId,
            secType="STK",
            exchange="SMART",
            currency="USD",
        )


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _what_to_show(contract) -> str:
    """Return the correct IBKR ``whatToShow`` value for a contract type.

    IBKR only supports MIDPOINT for CASH (forex) contracts.  All other
    security types (IND, STK, FUT, ETF) require TRADES.
    """
    if getattr(contract, "secType", "") == "CASH":
        return "MIDPOINT"
    return "TRADES"


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
