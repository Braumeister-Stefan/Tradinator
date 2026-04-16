"""
Tradinator — Broker Connector.

Connects to the IG brokerage demo environment using the trading_ig library,
reads current positions, cash balance, and the tradable instrument universe.
Returns a broker_state dict that all downstream components consume.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import os
import time

from dotenv import load_dotenv
from trading_ig import IGService


class BrokerConnector:
    """Connects to IG and builds the broker_state dict for the pipeline."""

    TIMEOUT = 30            # API request timeout seconds
    MAX_RETRIES = 3         # session creation retry attempts
    ACC_TYPE_DEFAULT = "DEMO"
    PLACEHOLDER_VALUES = {
        "your_username_here",
        "your_password_here",
        "your_api_key_here",
    }

    def __init__(self, config):
        """Store config for later use by run()."""
        self.config = config

    def run(self) -> dict:
        """Connect to IG, fetch account data, and return broker_state."""
        username, password, api_key, acc_type = self._load_credentials()
        ig = self._create_session(username, password, api_key, acc_type)
        cash, balance = self._fetch_account_info(ig)
        positions = self._fetch_positions(ig)
        instruments = list(self.config.get("universe", []))
        return self._build_broker_state(ig, positions, cash, balance, instruments)

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

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
                # Restoring Version 2 as Session V3 causes 500 in fetch_accounts() on this DEMO account
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

    def _fetch_account_info(self, ig):
        """Fetch account list and return (cash, balance) for the active account."""
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
        return cash, balance

    def _fetch_positions(self, ig):
        """Fetch open positions and parse into a standardised list of dicts."""
        data = ig.fetch_open_positions()
        raw_positions = data.get("positions", [])

        positions = []
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
                "deal_id": pos.get("dealId", ""),
                "epic": mkt.get("epic", ""),
                "direction": direction,
                "size": size,
                "level": open_level,
                "profit_loss": round(profit_loss, 2),
            })

        print(f"Found {len(positions)} open position(s)")
        return positions

    def _build_broker_state(self, ig, positions, cash, balance, instruments):
        """Assemble the broker_state dict consumed by downstream components."""
        return {
            "session": ig,
            "positions": positions,
            "cash": cash,
            "balance": balance,
            "instruments": instruments,
        }
