"""
Tradinator — IG Brokerage Adapter.

Implements the BrokerAdapter protocol for the IG trading platform using
the ``trading_ig`` library.  All IG-specific API calls, response parsing,
credential handling, and rate-limit logic live here — nowhere else.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import os
import time

from dotenv import load_dotenv
from trading_ig import IGService


class IGBrokerAdapter:
    """IG implementation of the BrokerAdapter protocol."""

    TIMEOUT = 30
    MAX_RETRIES = 3
    ACC_TYPE_DEFAULT = "DEMO"
    PLACEHOLDER_VALUES = {
        "your_username_here",
        "your_password_here",
        "your_api_key_here",
    }

    RATE_LIMIT_DELAY = 0.2          # seconds between API calls
    EXPIRY = "-"                    # DFB / no expiry for CFDs
    DEFAULT_CURRENCY_CODE = "USD"

    def __init__(self, config: dict):
        self.config = config
        self._ig: IGService | None = None

    # ------------------------------------------------------------------
    # BrokerAdapter interface
    # ------------------------------------------------------------------

    def connect(self) -> dict:
        """Authenticate with IG and return the active account id."""
        username, password, api_key, acc_type = self._load_credentials()
        self._ig = self._create_session(username, password, api_key, acc_type)
        acc_number = os.environ.get("IG_ACC_NUMBER", "")
        return {"account_id": acc_number}

    def get_account_info(self) -> dict:
        """Fetch account balance and available cash from IG."""
        ig = self._require_session()
        data = ig.fetch_accounts()
        accounts = data.get("accounts", [])
        if not accounts:
            raise RuntimeError("No accounts returned by IG API")

        acc_number = os.environ.get("IG_ACC_NUMBER")
        if acc_number:
            account = next(
                (a for a in accounts if a.get("accountId") == acc_number),
                None,
            )
            if account is None:
                raise RuntimeError(
                    f"Account {acc_number} not found in IG accounts"
                )
        else:
            account = accounts[0]

        bal = account.get("balance", {})
        cash = float(bal.get("available", 0))
        balance = float(bal.get("balance", 0))
        print(f"Account balance: {balance:.2f}, available cash: {cash:.2f}")
        return {"cash": cash, "balance": balance}

    def get_positions(self) -> list[dict]:
        """Fetch open positions from IG and normalise into adapter schema."""
        ig = self._require_session()
        data = ig.fetch_open_positions()
        raw_positions = data.get("positions", [])

        positions: list[dict] = []
        for entry in raw_positions:
            pos = entry.get("position", {})
            mkt = entry.get("market", {})

            bid = float(mkt.get("bid", 0) or 0)
            offer = float(mkt.get("offer", 0) or 0)
            mid_price = (bid + offer) / 2 if (bid and offer) else 0

            open_level = float(pos.get("level", 0) or 0)
            size = float(pos.get("size", 0) or 0)
            direction = pos.get("direction", "BUY")

            if direction == "BUY":
                profit_loss = (mid_price - open_level) * size
            else:
                profit_loss = (open_level - mid_price) * size

            positions.append({
                "instrument_id": mkt.get("epic", ""),
                "direction": direction,
                "size": size,
                "level": open_level,
                "deal_id": pos.get("dealId", ""),
                "profit_loss": round(profit_loss, 2),
            })

        print(f"Found {len(positions)} open position(s)")
        return positions

    def fetch_historical_prices(
        self, instrument_id: str, resolution: str, lookback: int
    ) -> list[dict]:
        """Fetch historical OHLCV bars from IG for a single instrument."""
        ig = self._require_session()
        raw = ig.fetch_historical_prices_by_epic_and_num_points(
            instrument_id, resolution, lookback
        )
        bars = raw.get("prices", [])
        result: list[dict] = []
        for bar in bars:
            ts = bar.get("snapshotTimeUTC") or bar.get("snapshotTime")

            close_price = bar.get("closePrice")
            bid_close_val = None
            if close_price is not None:
                bid_close_val = close_price.get("bid")

            result.append({
                "close": self._mid(bar.get("closePrice")),
                "high": self._mid(bar.get("highPrice")),
                "low": self._mid(bar.get("lowPrice")),
                "open": self._mid(bar.get("openPrice")),
                "volume": bar.get("lastTradedVolume"),
                "bid_close": bid_close_val,
                "timestamp": ts,
            })
        return result

    def fetch_instrument_info(self, instrument_id: str) -> dict:
        """Fetch instrument name and currency from IG."""
        ig = self._require_session()
        defaults = {
            "instrument_name": instrument_id,
            "instrument_id": instrument_id,
            "currency": "Unknown",
        }
        try:
            market = ig.fetch_market_by_epic(instrument_id)
            instrument = market.get("instrument", {})
            return {
                "instrument_name": instrument.get("name", instrument_id),
                "instrument_id": instrument_id,
                "currency": instrument.get("currencies", [{}])[0].get(
                    "code", "Unknown"
                ),
            }
        except Exception as exc:
            print(
                f"[IGBrokerAdapter] WARNING: metadata fetch failed for "
                f"{instrument_id} — {exc}"
            )
            return defaults

    def open_position(
        self,
        instrument_id: str,
        direction: str,
        size: float,
        order_type: str,
        currency_code: str,
    ) -> dict:
        """Open a position via the IG API."""
        ig = self._require_session()
        response = ig.create_open_position(
            currency_code=currency_code,
            direction=direction,
            epic=instrument_id,
            expiry=self.EXPIRY,
            force_open=True,
            guaranteed_stop=False,
            level=None,
            limit_distance=None,
            limit_level=None,
            order_type=order_type,
            quote_id=None,
            size=size,
            stop_distance=None,
            stop_level=None,
            trailing_stop=False,
            trailing_stop_increment=None,
        )
        return {"deal_reference": response["dealReference"]}

    def close_position(
        self,
        deal_id: str,
        direction: str,
        instrument_id: str,
        size: float,
        order_type: str,
    ) -> dict:
        """Close a position via the IG API."""
        ig = self._require_session()
        response = ig.close_open_position(
            deal_id=deal_id,
            direction=direction,
            epic=instrument_id,
            expiry=self.EXPIRY,
            level=None,
            order_type=order_type,
            quote_id=None,
            size=size,
        )
        return {"deal_reference": response["dealReference"]}

    def confirm_deal(self, deal_reference: str) -> dict:
        """Confirm whether a deal was accepted or rejected."""
        ig = self._require_session()
        confirmation = ig.fetch_deal_by_deal_reference(deal_reference)
        return {
            "status": confirmation.get("dealStatus", "REJECTED"),
            "deal_id": confirmation.get("dealId"),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_session(self) -> IGService:
        """Return the active IGService or raise if not connected."""
        if self._ig is None:
            raise RuntimeError(
                "IGBrokerAdapter: connect() must be called before any API call."
            )
        return self._ig

    def _load_credentials(self):
        """Load IG credentials from environment variables (via .env if present)."""
        env_path = self.config.get("env_path")
        if env_path and os.path.isfile(env_path):
            load_dotenv(env_path)

        username = self._normalise_credential(os.environ.get("IG_USERNAME"))
        password = self._normalise_credential(os.environ.get("IG_PASSWORD"))
        api_key = self._normalise_credential(os.environ.get("IG_API_KEY"))
        acc_type = os.environ.get("IG_ACC_TYPE", self.ACC_TYPE_DEFAULT)

        if acc_type.upper() != "DEMO":
            raise RuntimeError(
                "Tradinator is restricted to paper trading (DEMO) only. "
                f"IG_ACC_TYPE is set to '{acc_type}'. Set it to 'DEMO' or remove it."
            )

        if not all([username, password, api_key]):
            missing = [
                name
                for name, val in [
                    ("IG_USERNAME", username),
                    ("IG_PASSWORD", password),
                    ("IG_API_KEY", api_key),
                ]
                if not val
            ]
            raise RuntimeError(
                f"Missing required IG credentials: {', '.join(missing)}. "
                "Set them as environment variables or in the .env file."
            )

        return username, password, api_key, acc_type

    def _normalise_credential(self, value):
        """Treat empty and template placeholder values as missing credentials."""
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned or cleaned in self.PLACEHOLDER_VALUES:
            return None
        return cleaned

    def _create_session(self, username, password, api_key, acc_type):
        """Create an IGService instance and establish a session with retries."""
        acc_number = os.environ.get("IG_ACC_NUMBER")
        ig = IGService(
            username,
            password,
            api_key,
            acc_type=acc_type,
            acc_number=acc_number,
            return_dataframe=False,
            return_munch=False,
        )

        last_error = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                ig.create_session(version="2")
                print(f"Connected to IG {acc_type.upper()}")
                return ig
            except Exception as exc:
                last_error = exc
                if attempt < self.MAX_RETRIES:
                    wait = 2 ** attempt
                    print(
                        f"Session creation failed (attempt {attempt}/"
                        f"{self.MAX_RETRIES}), retrying in {wait}s"
                    )
                    time.sleep(wait)

        raise RuntimeError(
            f"Failed to connect to IG after {self.MAX_RETRIES} attempts: {last_error}"
        )

    @staticmethod
    def _mid(price_field) -> float | None:
        """Compute mid price as (bid + ask) / 2, or None if data is missing."""
        if price_field is None:
            return None
        bid = price_field.get("bid")
        ask = price_field.get("ask")
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2
