"""
Tradinator — Data Pipeline.

Downloads historical market data for each instrument via the IG API and
performs basic cleaning (mid-price calculation, forward-fill).
Persists consolidated series to an xlsx master file as a side effect.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""


import os
import time

import pandas as pd


class DataPipeline:
    """Fetch and clean historical price data for every instrument in the universe."""

    DEFAULT_RESOLUTION = "DAY"
    DEFAULT_LOOKBACK = 50
    FILL_METHOD = "ffill"  # forward-fill for missing values
    RATE_LIMIT_DELAY = 0.2  # 5 requests per second limit (0.2s delay)
    SERIES_FILE = "data/input/universe_series.xlsx"  # master file path
    HISTORIC_DIR = "data/input/historic_series"  # historic ingest folder
    SHEET_NAMES = ("mid_close", "bid_close", "mid_open")  # the three sheet names

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

        # --- Persistence side effect (R11) ---
        try:
            live_frames = self._build_dataframes(prices)
            master = self._load_series_file(self.SERIES_FILE)
            if master is None:
                master = {name: pd.DataFrame() for name in self.SHEET_NAMES}
            master = self._ingest_historic_files(master, self.HISTORIC_DIR)
            master = self._merge_series(live_frames, master)
            if not self._validate_series_schema(master):
                print("[DataPipeline] WARNING: master series failed validation.")
            self._save_series_file(master, self.SERIES_FILE)
        except Exception as exc:
            print(f"[DataPipeline] WARNING: persistence failed — {exc}")

        print(f"[DataPipeline] Done — {len(prices)} instrument(s) loaded.")
        return {
            "prices": prices,
            "metadata": metadata,
            "resolution": resolution,
            "lookback": lookback,
        }

    def ingest_historic(self) -> None:
        """Standalone entry point to load, ingest historic files, validate, and save."""
        master = self._load_series_file(self.SERIES_FILE)
        if master is None:
            master = {name: pd.DataFrame() for name in self.SHEET_NAMES}
        master = self._ingest_historic_files(master, self.HISTORIC_DIR)
        if not self._validate_series_schema(master):
            print("[DataPipeline] WARNING: master series failed validation after ingest.")
        self._save_series_file(master, self.SERIES_FILE)

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _fetch_prices(self, ig, epic: str, resolution: str, lookback: int) -> dict:
        """Call the IG API for historical price bars."""
        return ig.fetch_historical_prices_by_epic_and_num_points(
            epic, resolution, lookback
        )

    def _parse_prices(self, raw_response: dict, epic: str) -> dict | None:
        """Convert IG price bars into {close, high, low, open, volume, bid_close, timestamps} lists."""
        bars = raw_response.get("prices", [])
        if not bars:
            return None

        close, high, low, opn, volume = [], [], [], [], []
        bid_close = []
        timestamps = []

        for bar in bars:
            close.append(self._mid(bar.get("closePrice")))
            high.append(self._mid(bar.get("highPrice")))
            low.append(self._mid(bar.get("lowPrice")))
            opn.append(self._mid(bar.get("openPrice")))
            volume.append(bar.get("lastTradedVolume"))

            # Extract bid component only of closePrice
            close_price = bar.get("closePrice")
            if close_price is not None:
                bid_close.append(close_price.get("bid"))
            else:
                bid_close.append(None)

            # Extract timestamp string
            ts = bar.get("snapshotTimeUTC") or bar.get("snapshotTime")
            timestamps.append(ts)

        return {
            "close": close,
            "high": high,
            "low": low,
            "open": opn,
            "volume": volume,
            "bid_close": bid_close,
            "timestamps": timestamps,
        }

    def _clean_prices(self, prices: dict) -> dict:
        """Forward-fill None gaps and drop instruments that are entirely None."""
        cleaned = {}
        for epic, fields in prices.items():
            if self._all_none(fields):
                print(f"[DataPipeline] Dropping {epic} — all values are None.")
                continue
            cleaned[epic] = {
                key: (values if key == "timestamps" else self._forward_fill(values))
                for key, values in fields.items()
            }
        return cleaned

    def _build_metadata(self, ig, epic: str) -> dict:
        """Fetch instrument name and currency from the IG market endpoint."""
        defaults = {
            "instrument_name": epic,
            "epic": epic,
            "currency": "Unknown",
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
            }
        except Exception as exc:
            print(f"[DataPipeline] WARNING: metadata fetch failed for {epic} — {exc}")
            return defaults

    # ------------------------------------------------------------------
    # DataFrame / xlsx methods
    # ------------------------------------------------------------------

    def _build_dataframes(self, prices: dict) -> dict:
        """Convert per-epic price lists into dict of three DataFrames."""
        sheet_mapping = {
            "mid_close": "close",
            "bid_close": "bid_close",
            "mid_open": "open",
        }
        frames = {}
        for sheet_name, field_key in sheet_mapping.items():
            series_dict = {}
            for epic, fields in prices.items():
                ts = fields.get("timestamps", [])
                vals = fields.get(field_key, [])
                index = pd.to_datetime(ts, utc=True, errors="coerce")
                series_dict[epic] = pd.Series(vals, index=index, dtype=float)
            if series_dict:
                frames[sheet_name] = pd.DataFrame(series_dict)
            else:
                frames[sheet_name] = pd.DataFrame()
        return frames

    def _load_series_file(self, path: str) -> dict | None:
        """Read an existing multi-sheet xlsx file into dict-of-DataFrames."""
        if not os.path.isfile(path):
            return None
        sheets = {}
        with pd.ExcelFile(path, engine="openpyxl") as xls:
            for name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=name, index_col=0)
                df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
                sheets[name] = df
        return sheets

    def _save_series_file(self, series: dict, path: str) -> None:
        """Write dict-of-DataFrames to a multi-sheet xlsx file, sorted ascending."""
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for name, df in series.items():
                sorted_df = df.sort_index(ascending=True)
                # Excel does not support timezone-aware datetimes; strip tz
                # before writing. _load_series_file restores UTC on read.
                if sorted_df.index.tz is not None:
                    sorted_df = sorted_df.copy()
                    sorted_df.index = sorted_df.index.tz_localize(None)
                sorted_df.to_excel(writer, sheet_name=name)

    def _validate_series_schema(self, series: dict) -> bool:
        """Check structural validity of a series dict. Return True if valid."""
        valid = True

        # (1) All three expected sheet names present
        for name in self.SHEET_NAMES:
            if name not in series:
                print(f"[DataPipeline] VALIDATION: missing sheet '{name}'.")
                valid = False
        if not valid:
            return False

        sheets = [series[name] for name in self.SHEET_NAMES]

        for name in self.SHEET_NAMES:
            df = series[name]

            # (2) Index is datetime-typed (empty DataFrames are exempt)
            if not df.empty and not pd.api.types.is_datetime64_any_dtype(df.index):
                print(f"[DataPipeline] VALIDATION: sheet '{name}' index is not datetime.")
                valid = False

            # (3) All data values numeric (allow NaN)
            for col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    print(
                        f"[DataPipeline] VALIDATION: sheet '{name}', column '{col}' "
                        "is not numeric."
                    )
                    valid = False

            # (5) Index is sorted ascending
            if not df.index.is_monotonic_increasing:
                print(f"[DataPipeline] VALIDATION: sheet '{name}' index is not sorted ascending.")
                valid = False

        # (4) Column names and index values identical across all three sheets
        ref_cols = sheets[0].columns
        ref_index = sheets[0].index
        for name, df in zip(self.SHEET_NAMES[1:], sheets[1:]):
            if list(df.columns) != list(ref_cols):
                print(
                    f"[DataPipeline] VALIDATION: columns of '{name}' differ from "
                    f"'{self.SHEET_NAMES[0]}'."
                )
                valid = False
            if not df.index.equals(ref_index):
                print(
                    f"[DataPipeline] VALIDATION: index of '{name}' differs from "
                    f"'{self.SHEET_NAMES[0]}'."
                )
                valid = False

        return valid

    def _merge_series(self, primary: dict, secondary: dict) -> dict:
        """Merge two series dicts; primary values win on overlap via combine_first."""
        if not primary and not secondary:
            return {name: pd.DataFrame() for name in self.SHEET_NAMES}
        if not primary:
            return {name: secondary.get(name, pd.DataFrame()).copy() for name in self.SHEET_NAMES}
        if not secondary:
            return {name: primary.get(name, pd.DataFrame()).copy() for name in self.SHEET_NAMES}

        merged = {}
        all_keys = set(primary) | set(secondary)
        for name in all_keys:
            p = primary.get(name, pd.DataFrame())
            s = secondary.get(name, pd.DataFrame())
            merged[name] = p.combine_first(s)
        return merged

    def _ingest_historic_files(self, master: dict, folder_path: str) -> dict:
        """Scan folder for xlsx files and merge each into master."""
        if not os.path.isdir(folder_path):
            return master
        for filename in sorted(os.listdir(folder_path)):
            if not filename.endswith(".xlsx"):
                continue
            filepath = os.path.join(folder_path, filename)
            print(f"[DataPipeline] Ingesting historic file: {filename}")
            loaded = self._load_series_file(filepath)
            if loaded is None:
                print(f"[DataPipeline] WARNING: could not load '{filename}', skipping.")
                continue
            if not self._validate_series_schema(loaded):
                print(f"[DataPipeline] WARNING: '{filename}' failed validation, skipping.")
                continue
            master = self._merge_series(master, loaded)
        return master

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
        """Return True if every value in every field list is None (skipping timestamps)."""
        return all(
            all(v is None for v in values)
            for key, values in fields.items()
            if key != "timestamps"
        )
