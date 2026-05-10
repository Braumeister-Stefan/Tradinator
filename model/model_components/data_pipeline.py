"""
Tradinator — Data Pipeline.

Downloads historical market data for each instrument via the broker adapter
and performs basic cleaning (forward-fill).  Persists consolidated series to
an xlsx master file as a side effect.

Two-tier validation integration
--------------------------------
T2 (data availability) validation status for each instrument is continuously
maintained in ``universe_candidates.json``:

- On a **cold-start** (no stored series for the instrument): fetches the last
  ``lookback`` bars via the fixed-count API.  This matches the intention that
  every first run retrieves the configured window of history.
- On a **subsequent run** (stored series exists): fetches only bars after the
  last stored timestamp using the date-range API, so the master file grows
  incrementally — one or more new bars per run rather than re-fetching the
  same window repeatedly.

If the most-recent fetch returns zero bars the instrument's T2 status is
set to ``NO`` in ``universe_candidates.json`` and it is removed from
``universe.json``.  Gaps (NaN values) in an otherwise-populated series do
**not** disqualify an instrument.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""


import json
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
    CANDIDATES_PATH = "data/input/universe_candidates.json"  # candidate registry
    UNIVERSE_PATH = "data/input/universe.json"               # machine-read universe

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, broker_state: dict) -> dict:
        """Download prices for each instrument, clean them, and return market_data.

        Fetch strategy
        --------------
        1. Load the existing master series file **before** the fetch loop so the
           last stored timestamp can be determined per instrument.
        2. For instruments with no stored data (cold start): call the fixed-count
           API (``fetch_historical_prices``) with the configured ``lookback``.
        3. For instruments with existing stored data: call the date-range API
           (``fetch_historical_prices_by_date_range``) from the last stored
           timestamp + 1 second, retrieving only genuinely new bars.
        4. After each fetch, update the T2 status in ``universe_candidates.json``
           and — if the fetch returned zero bars — remove the instrument from
           ``universe.json``.
        """
        adapter = broker_state["adapter"]
        instruments = broker_state["instruments"]
        resolution = self.config.get("resolution", self.DEFAULT_RESOLUTION)
        lookback = self.config.get("lookback", self.DEFAULT_LOOKBACK)

        # Load master series before the fetch loop (needed for last-date lookup).
        master = self._load_series_file(self.SERIES_FILE)

        prices = {}

        for i, instrument_id in enumerate(instruments):
            if i > 0:
                time.sleep(self.RATE_LIMIT_DELAY)

            last_date = self._get_last_stored_date(instrument_id, master)

            if last_date is not None:
                # Incremental fetch — retrieve only bars after the last stored bar.
                print(
                    f"[DataPipeline] Incremental fetch {instrument_id}"
                    f" from {last_date} ({resolution})…"
                )
                try:
                    bars = adapter.fetch_historical_prices_by_date_range(
                        instrument_id, resolution, last_date
                    )
                except Exception as exc:
                    print(
                        f"[DataPipeline] WARNING: incremental fetch failed for"
                        f" {instrument_id} — {exc}. Falling back to fixed-count fetch."
                    )
                    try:
                        bars = adapter.fetch_historical_prices(
                            instrument_id, resolution, lookback
                        )
                    except Exception as exc2:
                        print(
                            f"[DataPipeline] WARNING: skipping {instrument_id} — {exc2}"
                        )
                        self._update_t2_status(instrument_id, "NO", f"fetch exception: {exc2}")
                        self._remove_from_universe(instrument_id)
                        continue

                parsed = self._bars_to_columns(bars)
                if parsed is None:
                    # Zero bars on an incremental fetch is normal on weekends and
                    # public holidays — it means no new bar has closed since the last
                    # stored bar.  This is NOT a T2 failure; do not remove the
                    # instrument from the universe.  Serve the existing stored series
                    # for this pipeline run instead of dropping the instrument.
                    print(
                        f"[DataPipeline] {instrument_id}: 0 new bars since {last_date}"
                        " (non-trading day or no new data) — retaining stored series."
                    )
                    parsed = self._reconstruct_from_master(instrument_id, master)
                    if parsed is None:
                        print(
                            f"[DataPipeline] WARNING: no stored series for {instrument_id},"
                            " skipping this run."
                        )
                        continue
                else:
                    self._update_t2_status(
                        instrument_id, "YES", f"{len(bars)} new bar(s) fetched"
                    )
            else:
                # Cold start — fetch the configured lookback window.
                print(
                    f"[DataPipeline] Cold-start fetch {instrument_id}"
                    f" ({resolution}, {lookback} bars)…"
                )
                try:
                    bars = adapter.fetch_historical_prices(
                        instrument_id, resolution, lookback
                    )
                except Exception as exc:
                    print(f"[DataPipeline] WARNING: skipping {instrument_id} — {exc}")
                    self._update_t2_status(instrument_id, "NO", f"fetch exception: {exc}")
                    self._remove_from_universe(instrument_id)
                    continue

                parsed = self._bars_to_columns(bars)
                if parsed is None:
                    # Cold-start returning zero bars is a genuine T2 failure:
                    # the instrument has no data at all on the broker's API.
                    print(
                        f"[DataPipeline] WARNING: no usable data for {instrument_id}"
                        " (cold-start) — removing from universe."
                    )
                    self._update_t2_status(
                        instrument_id, "NO", "cold-start fetch returned no usable bars"
                    )
                    self._remove_from_universe(instrument_id)
                    continue

                self._update_t2_status(
                    instrument_id, "YES", f"{len(bars)} bar(s) fetched (cold-start)"
                )

            prices[instrument_id] = parsed

        prices = self._clean_prices(prices)

        # --- Fetch instrument metadata with dealing rules ---
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
                    "dealing_enabled": True,
                    "buy_allowed": True,
                    "sell_allowed": True,
                }

        # --- Persistence side effect ---
        try:
            live_frames = self._build_dataframes(prices)
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
    # Incremental fetch helpers
    # ------------------------------------------------------------------

    def _get_last_stored_date(
        self, instrument_id: str, master: "dict | None"
    ) -> "str | None":
        """Return ISO-8601 UTC timestamp (last stored + 1 second) for incremental fetch.

        Uses the ``mid_close`` sheet as the reference.  Returns ``None`` when
        no stored data exists for *instrument_id*, indicating a cold start.
        """
        if master is None:
            return None
        ref_sheet = master.get(self.SHEET_NAMES[0])  # mid_close
        if ref_sheet is None or ref_sheet.empty:
            return None
        if instrument_id not in ref_sheet.columns:
            return None
        series = ref_sheet[instrument_id].dropna()
        if series.empty:
            return None
        last_ts = series.index.max()
        # Add 1 second so the last stored bar is not re-fetched.
        next_ts = last_ts + pd.Timedelta(seconds=1)
        return next_ts.strftime("%Y-%m-%dT%H:%M:%S")

    def _reconstruct_from_master(
        self, instrument_id: str, master: "dict | None"
    ) -> "dict | None":
        """Rebuild a bars-as-columns dict from stored master series for one instrument.

        Used when an incremental fetch returns zero bars (e.g., on weekends) so
        the pipeline can still provide the previously-stored series to downstream
        components without re-fetching.

        Returns ``None`` if no stored series exists for *instrument_id*.
        """
        if master is None:
            return None
        close_sheet = master.get("mid_close")
        open_sheet = master.get("mid_open")
        bid_sheet = master.get("bid_close")
        if close_sheet is None or close_sheet.empty:
            return None
        if instrument_id not in close_sheet.columns:
            return None
        close_series = close_sheet[instrument_id].dropna()
        if close_series.empty:
            return None
        timestamps = [ts.strftime("%Y-%m-%dT%H:%M:%S") for ts in close_series.index]
        open_vals = (
            open_sheet[instrument_id].reindex(close_series.index).tolist()
            if open_sheet is not None and instrument_id in open_sheet.columns
            else [None] * len(timestamps)
        )
        bid_vals = (
            bid_sheet[instrument_id].reindex(close_series.index).tolist()
            if bid_sheet is not None and instrument_id in bid_sheet.columns
            else [None] * len(timestamps)
        )
        return {
            "close": close_series.tolist(),
            "high": [None] * len(timestamps),
            "low": [None] * len(timestamps),
            "open": open_vals,
            "volume": [None] * len(timestamps),
            "bid_close": bid_vals,
            "timestamps": timestamps,
        }

    def _update_t2_status(
        self, instrument_id: str, t2_status: str, t2_reason: str
    ) -> None:
        """Update T2 status for an instrument in universe_candidates.json.

        Silently skips if the candidates file does not exist.  Failure to
        update is non-fatal — a warning is printed but the pipeline continues.
        """
        if not os.path.isfile(self.CANDIDATES_PATH):
            return
        try:
            with open(self.CANDIDATES_PATH) as f:
                data = json.load(f)

            now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            updated = False
            for candidate in data.get("candidates", []):
                if candidate.get("epic") == instrument_id:
                    candidate["t2_status"] = t2_status
                    candidate["t2_reason"] = t2_reason
                    candidate["valid"] = (
                        candidate.get("t1_status") == "PASS" and t2_status == "YES"
                    )
                    candidate["last_validated"] = now_utc
                    updated = True
                    break

            if updated:
                with open(self.CANDIDATES_PATH, "w") as f:
                    json.dump(data, f, indent=2)
                    f.write("\n")
        except Exception as exc:
            print(
                f"[DataPipeline] WARNING: could not update T2 status"
                f" for {instrument_id} — {exc}"
            )

    def _remove_from_universe(self, instrument_id: str) -> None:
        """Remove an instrument from universe.json when T2 fails (zero bars returned).

        Instruments are removed from the machine-read universe so the pipeline
        does not continue trying to trade them.  Re-run ``discover_universe.py``
        to re-add an instrument if data becomes available again.

        Silently skips if universe.json does not exist.  Failure is non-fatal.
        """
        if not os.path.isfile(self.UNIVERSE_PATH):
            return
        try:
            with open(self.UNIVERSE_PATH) as f:
                data = json.load(f)

            original_count = len(data.get("instruments", []))
            data["instruments"] = [
                inst for inst in data.get("instruments", [])
                if inst.get("epic") != instrument_id
            ]
            removed = original_count - len(data["instruments"])
            if removed > 0:
                print(
                    f"[DataPipeline] Removed {instrument_id} from universe.json"
                    " (T2=NO, zero bars in last fetch)."
                )
                with open(self.UNIVERSE_PATH, "w") as f:
                    json.dump(data, f, indent=2)
                    f.write("\n")
        except Exception as exc:
            print(
                f"[DataPipeline] WARNING: could not remove {instrument_id}"
                f" from universe.json — {exc}"
            )

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
