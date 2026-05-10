"""
Tradinator — Data Pipeline.

Downloads historical market data for each instrument via the broker adapter
and performs basic cleaning (forward-fill).  Persists consolidated series to
an xlsx master file as a side effect.

When the broker adapter cannot return usable data for an instrument, a
secondary fetch via ``YHFinanceFetcher`` (Yahoo Finance) is attempted.
If both sources fail, the instrument is skipped.  A per-run
``candidates_report.csv`` is written to ``data/output/`` recording the
data-source outcome for every universe instrument.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""


import csv
import os
import time

import pandas as pd

from .yh_finance_fetcher import YHFinanceFetcher


class DataPipeline:
    """Fetch and clean historical price data for every instrument in the universe."""

    DEFAULT_RESOLUTION = "DAY"
    DEFAULT_LOOKBACK = 50
    FILL_METHOD = "ffill"  # forward-fill for missing values
    RATE_LIMIT_DELAY = 1.0  # seconds between API calls
    SERIES_FILE = "data/input/universe_series.xlsx"  # master file path
    HISTORIC_DIR = "data/input/historic_series"  # historic ingest folder
    SHEET_NAMES = ("mid_close", "bid_close", "mid_open")  # the three sheet names
    CANDIDATES_REPORT_FILENAME = "candidates_report.csv"  # written to output_dir

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
        # Track the data source used for each instrument.
        data_sources: dict[str, str] = {}
        # Track whether broker / YH succeeded per instrument for the report.
        broker_available: dict[str, bool] = {}
        yh_available: dict[str, bool] = {}

        yh_fetcher = YHFinanceFetcher()

        for i, instrument_id in enumerate(instruments):
            if i > 0:
                time.sleep(self.RATE_LIMIT_DELAY)

            print(f"[DataPipeline] Fetching {instrument_id} ({resolution}, {lookback} bars)…")

            # --- Primary: broker adapter ---
            broker_ok = False
            parsed = None
            try:
                bars = adapter.fetch_historical_prices(
                    instrument_id, resolution, lookback
                )
                parsed = self._bars_to_columns(bars)
                if parsed is not None and not self._all_none(parsed):
                    broker_ok = True
            except Exception as exc:
                print(f"[DataPipeline] WARNING: broker fetch failed for {instrument_id} — {exc}")

            broker_available[instrument_id] = broker_ok

            if broker_ok:
                prices[instrument_id] = parsed
                data_sources[instrument_id] = "broker"
                yh_available[instrument_id] = False
                continue

            # --- Fallback: YH Finance ---
            print(
                f"[DataPipeline] Broker data unavailable for {instrument_id}, "
                "trying YH Finance fallback…"
            )
            yh_bars = yh_fetcher.fetch_historical_prices(instrument_id, resolution, lookback)
            yh_parsed = self._bars_to_columns(yh_bars) if yh_bars else None

            if yh_parsed is not None and not self._all_none(yh_parsed):
                prices[instrument_id] = yh_parsed
                data_sources[instrument_id] = "yh_finance"
                yh_available[instrument_id] = True
                print(f"[DataPipeline] YH Finance fallback succeeded for {instrument_id}.")
            else:
                yh_available[instrument_id] = False
                print(f"[DataPipeline] WARNING: no usable data for {instrument_id}, skipping.")

        prices = self._clean_prices(prices)

        # --- Investable universe log ---
        universe_size = len(instruments)
        investable_size = len(prices)
        pct = (investable_size / universe_size * 100) if universe_size > 0 else 0.0
        print(
            f"[DataPipeline] Investable universe: {investable_size} epic(s) "
            f"({pct:.1f}% of {universe_size} considered candidates)"
        )

        # --- Candidates report (non-blocking side effect) ---
        try:
            self._write_candidates_report(
                instruments, prices, data_sources, broker_available, yh_available
            )
        except Exception as exc:
            print(f"[DataPipeline] WARNING: candidates report write failed — {exc}")

        # --- Fetch instrument metadata with dealing rules (REQ-3) ---
        instrument_metadata = {}
        for i, instrument_id in enumerate(prices):
            if i > 0:
                time.sleep(self.RATE_LIMIT_DELAY)
            try:
                instrument_metadata[instrument_id] = adapter.fetch_instrument_info(instrument_id)
            except Exception as exc:
                print(f"[DataPipeline] WARNING: metadata fetch failed for {instrument_id} — {exc}")
                instrument_metadata[instrument_id] = {
                    "instrument_name": instrument_id,
                    "instrument_id": instrument_id,
                    "currency": "Unknown",
                    "min_deal_size": 0.01,
                    "max_deal_size": None,
                    "min_size_increment": 1.0,
                    "scaling_factor": 1,
                }

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
            "metadata": instrument_metadata,
            "resolution": resolution,
            "lookback": lookback,
            "data_sources": data_sources,
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
    # Candidates report
    # ------------------------------------------------------------------

    def _write_candidates_report(
        self,
        instruments: list[str],
        prices: dict,
        data_sources: dict,
        broker_available: dict,
        yh_available: dict,
    ) -> None:
        """Write a CSV report of data-source outcomes for every universe instrument.

        The file is written to ``{output_dir}/candidates_report.csv``.
        The ``validation_passed`` column is left blank here and filled in
        later by ``StrategyEval``.
        """
        output_dir = self.config.get("output_dir", "data/output")
        os.makedirs(output_dir, exist_ok=True)
        report_path = os.path.join(output_dir, self.CANDIDATES_REPORT_FILENAME)

        rows = []
        for instrument_id in instruments:
            fields = prices.get(instrument_id, {})
            close_vals = fields.get("close", [])
            non_zero_points = sum(
                1 for v in close_vals if v is not None and v != 0
            )
            source = data_sources.get(instrument_id, "none")
            rows.append({
                "epic": instrument_id,
                "data_source": source,
                "non_zero_data_points": non_zero_points,
                "broker_data_available": broker_available.get(instrument_id, False),
                "yh_data_available": yh_available.get(instrument_id, False),
                "validation_passed": "",  # filled in by StrategyEval
            })

        fieldnames = [
            "epic",
            "data_source",
            "non_zero_data_points",
            "broker_data_available",
            "yh_data_available",
            "validation_passed",
        ]
        with open(report_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

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
