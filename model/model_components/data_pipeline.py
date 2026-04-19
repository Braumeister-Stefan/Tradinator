"""
Tradinator — Data Pipeline.

Downloads historical market data for each instrument via the broker adapter
and performs basic cleaning (forward-fill).  Persists consolidated series to
an xlsx master file as a side effect.

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
    RATE_LIMIT_DELAY = 1.0  # seconds between API calls
    SERIES_FILE = "data/input/universe_series.xlsx"  # master file path
    HISTORIC_DIR = "data/input/historic_series"  # historic ingest folder
    SHEET_NAMES = ("mid_close", "bid_close", "mid_open")  # the three sheet names

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, broker_state: dict) -> dict:
        """Download prices for each instrument, clean them, and return market_data."""
        adapter = broker_state["adapter"]
        instruments = broker_state["instruments"]
        resolution = self.config.get("resolution", self.DEFAULT_RESOLUTION)
        lookback = self.config.get("lookback", self.DEFAULT_LOOKBACK)

        prices = {}

        for i, instrument_id in enumerate(instruments):
            if i > 0:
                time.sleep(self.RATE_LIMIT_DELAY)

            print(f"[DataPipeline] Fetching {instrument_id} ({resolution}, {lookback} bars)…")
            try:
                bars = adapter.fetch_historical_prices(
                    instrument_id, resolution, lookback
                )
            except Exception as exc:
                print(f"[DataPipeline] WARNING: skipping {instrument_id} — {exc}")
                continue

            parsed = self._bars_to_columns(bars)
            if parsed is None:
                print(f"[DataPipeline] WARNING: no usable data for {instrument_id}, skipping.")
                continue

            prices[instrument_id] = parsed

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
            "metadata": {inst: {"instrument_name": inst, "epic": inst} for inst in prices},
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

    @staticmethod
    def _bars_to_columns(bars: list[dict]) -> dict | None:
        """Convert adapter price bars into column-oriented {field: [values]} dict."""
        if not bars:
            return None

        close, high, low, opn, volume = [], [], [], [], []
        bid_close: list = []
        timestamps: list = []

        for bar in bars:
            close.append(bar.get("close"))
            high.append(bar.get("high"))
            low.append(bar.get("low"))
            opn.append(bar.get("open"))
            volume.append(bar.get("volume"))
            bid_close.append(bar.get("bid_close"))
            timestamps.append(bar.get("timestamp"))

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
        for instrument_id, fields in prices.items():
            if self._all_none(fields):
                print(f"[DataPipeline] Dropping {instrument_id} — all values are None.")
                continue
            cleaned[instrument_id] = {
                key: (values if key == "timestamps" else self._forward_fill(values))
                for key, values in fields.items()
            }
        return cleaned

    # ------------------------------------------------------------------
    # DataFrame / xlsx methods
    # ------------------------------------------------------------------

    def _build_dataframes(self, prices: dict) -> dict:
        """Convert per-instrument price lists into dict of three DataFrames."""
        sheet_mapping = {
            "mid_close": "close",
            "bid_close": "bid_close",
            "mid_open": "open",
        }
        frames = {}
        for sheet_name, field_key in sheet_mapping.items():
            series_dict = {}
            for instrument_id, fields in prices.items():
                ts = fields.get("timestamps", [])
                vals = fields.get(field_key, [])
                index = pd.to_datetime(ts, utc=True, errors="coerce")
                series_dict[instrument_id] = pd.Series(vals, index=index, dtype=float)
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
