"""
Tradinator — Data Pipeline.

Downloads historical market data for each instrument via the IG API and
performs basic cleaning (mid-price calculation, forward-fill).

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""


import time


class DataPipeline:
    """Fetch and clean historical price data for every instrument in the universe."""

    DEFAULT_RESOLUTION = "DAY"
    DEFAULT_LOOKBACK = 50
    FILL_METHOD = "ffill"  # forward-fill for missing values
    RATE_LIMIT_DELAY = 0.2  # 5 requests per second limit (0.2s delay)

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, broker_state: dict) -> dict:
        """Download prices for each instrument, clean them, and return market_data."""
        ig = broker_state["session"]
        instruments = broker_state["instruments"]
        resolution = self.config.get("resolution", self.DEFAULT_RESOLUTION)
        lookback = self.config.get("lookback", self.DEFAULT_LOOKBACK)

        prices = {}
        metadata = {}

        for i, epic in enumerate(instruments):
            # Throttle requests to stay within IG's rate limits (approx 5-10/s)
            if i > 0:
                time.sleep(self.RATE_LIMIT_DELAY)

            print(f"[DataPipeline] Fetching {epic} ({resolution}, {lookback} bars)…")
            try:
                raw = self._fetch_prices(ig, epic, resolution, lookback)
            except Exception as exc:
                print(f"[DataPipeline] WARNING: skipping {epic} — {exc}")
                continue

            parsed = self._parse_prices(raw, epic)
            if parsed is None:
                print(f"[DataPipeline] WARNING: no usable data for {epic}, skipping.")
                continue

            prices[epic] = parsed
            metadata[epic] = self._build_metadata(ig, epic)

        prices = self._clean_prices(prices)

        print(f"[DataPipeline] Done — {len(prices)} instrument(s) loaded.")
        return {
            "prices": prices,
            "metadata": metadata,
            "resolution": resolution,
            "lookback": lookback,
        }

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _fetch_prices(self, ig, epic: str, resolution: str, lookback: int) -> dict:
        """Call the IG API for historical price bars."""
        return ig.fetch_historical_prices_by_epic_and_num_points(
            epic, resolution, lookback
        )

    def _parse_prices(self, raw_response: dict, epic: str) -> dict | None:
        """Convert IG price bars into {close, high, low, open, volume} lists."""
        bars = raw_response.get("prices", [])
        if not bars:
            return None

        close, high, low, opn, volume = [], [], [], [], []

        for bar in bars:
            close.append(self._mid(bar.get("closePrice")))
            high.append(self._mid(bar.get("highPrice")))
            low.append(self._mid(bar.get("lowPrice")))
            opn.append(self._mid(bar.get("openPrice")))
            volume.append(bar.get("lastTradedVolume"))

        return {
            "close": close,
            "high": high,
            "low": low,
            "open": opn,
            "volume": volume,
        }

    def _clean_prices(self, prices: dict) -> dict:
        """Forward-fill None gaps and drop instruments that are entirely None."""
        cleaned = {}
        for epic, fields in prices.items():
            if self._all_none(fields):
                print(f"[DataPipeline] Dropping {epic} — all values are None.")
                continue
            cleaned[epic] = {
                key: self._forward_fill(values) for key, values in fields.items()
            }
        return cleaned

    def _build_metadata(self, ig, epic: str) -> dict:
        """Fetch instrument name and currency from the IG market endpoint."""
        defaults = {
            "instrument_name": epic,
            "epic": epic,
            "currency": "Unknown",
            "min_deal_size": 0.01,
            "lot_size": 1.0,
        }
        try:
            market = ig.fetch_market_by_epic(epic)
            instrument = market.get("instrument", {})
            return {
                "instrument_name": instrument.get("name", epic),
                "epic": epic,
                "currency": instrument.get("currencies", [{}])[0].get(
                    "code", "Unknown"
                ),
                "min_deal_size": float(market.get("dealingRules", {}).get("minDealSize", {}).get("value", 0.01)),
                "lot_size": float(market.get("instrument", {}).get("lotSize", 1.0)),
            }
        except Exception as exc:
            print(f"[DataPipeline] WARNING: metadata fetch failed for {epic} — {exc}")
            return defaults

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

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

    @staticmethod
    def _forward_fill(values: list) -> list:
        """Replace None entries with the nearest non-None value.

        Forward-fills first, then back-fills any remaining leading Nones.
        """
        # Forward fill
        filled = []
        last = None
        for v in values:
            if v is not None:
                last = v
            filled.append(last)
        # Back-fill leading Nones
        first_valid = None
        for v in filled:
            if v is not None:
                first_valid = v
                break
        if first_valid is not None:
            filled = [first_valid if v is None else v for v in filled]
        return filled

    @staticmethod
    def _all_none(fields: dict) -> bool:
        """Return True if every value in every field list is None."""
        return all(
            all(v is None for v in values) for values in fields.values()
        )
