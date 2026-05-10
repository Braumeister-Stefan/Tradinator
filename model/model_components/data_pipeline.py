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

If the most-recent cold-start fetch returns zero bars, a YH Finance fallback
is attempted before marking the instrument as T2=NO.  When the broker adapter
cannot return usable data for an instrument, a secondary fetch via
``YHFinanceFetcher`` (Yahoo Finance) is attempted.  If both sources fail, the
instrument is skipped.  A per-run ``candidates_report.csv`` is written to
``data/output/`` recording the data-source outcome for every universe
instrument.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""


import csv
import json
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
    SHEET_NAMES = ("mid_close",)  # only mid-price close is stored
    CANDIDATES_PATH = "data/input/universe_candidates.json"
    UNIVERSE_PATH = "data/input/universe.json"
    CANDIDATES_REPORT_FILENAME = "candidates_report.csv"  # written to output_dir

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
           If the broker returns zero bars, YH Finance is tried as a fallback
           before marking the instrument as T2=NO.
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

        # P1-log/P2: load all candidates once for reporting and logging.
        all_candidates: list = []
        candidates_total = 0
        try:
            if os.path.isfile(self.CANDIDATES_PATH):
                with open(self.CANDIDATES_PATH) as f:
                    _cdata = json.load(f)
                all_candidates = _cdata.get("candidates", [])
                candidates_total = len(all_candidates)
        except Exception as _exc:
            print(f"[DataPipeline] WARNING: could not load candidates file — {_exc}")

        prices = {}
        # Track the data source used for each instrument.
        data_sources: dict[str, str] = {}
        # Track whether broker / YH succeeded per instrument for the report.
        broker_available: dict[str, bool] = {}
        yh_available: dict[str, bool] = {}
        # P10: track instruments removed from universe this run (for series cleanup).
        removed_instruments: list[str] = []

        yh_fetcher = YHFinanceFetcher()

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
                        last_t2 = self._get_candidate_t2_status(instrument_id)  # P7-log
                        print(
                            f"[DataPipeline] WARNING: skipping {instrument_id} — {exc2}"
                            f" (last known T2={last_t2})"
                        )
                        self._update_t2_status(instrument_id, "NO", f"fetch exception: {exc2}")
                        self._remove_from_universe(instrument_id)
                        removed_instruments.append(instrument_id)
                        broker_available[instrument_id] = False
                        yh_available[instrument_id] = False
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
                        broker_available[instrument_id] = False
                        yh_available[instrument_id] = False
                        continue
                else:
                    self._update_t2_status(
                        instrument_id, "YES", f"{len(bars)} new bar(s) fetched"
                    )

                prices[instrument_id] = parsed
                data_sources[instrument_id] = "broker"
                broker_available[instrument_id] = True
                yh_available[instrument_id] = False
            else:
                # Cold start — fetch the configured lookback window.
                print(
                    f"[DataPipeline] Cold-start fetch {instrument_id}"
                    f" ({resolution}, {lookback} bars)…"
                )
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
                    self._update_t2_status(
                        instrument_id, "YES", f"{len(bars)} bar(s) fetched (cold-start)"
                    )
                    prices[instrument_id] = parsed
                    data_sources[instrument_id] = "broker"
                    yh_available[instrument_id] = False
                    continue

                # Cold-start broker fetch failed — try YH Finance fallback.
                print(
                    f"[DataPipeline] Broker data unavailable for {instrument_id}, "
                    "trying YH Finance fallback…"
                )
                yh_bars = yh_fetcher.fetch_historical_prices(instrument_id, resolution, lookback)
                yh_parsed = self._bars_to_columns(yh_bars) if yh_bars else None

                if yh_parsed is not None and not self._all_none(yh_parsed):
                    print(f"[DataPipeline] YH Finance fallback succeeded for {instrument_id}.")
                    self._update_t2_status(
                        instrument_id, "YES",
                        f"{len(yh_bars)} bar(s) fetched via YH Finance fallback (cold-start)"
                    )
                    prices[instrument_id] = yh_parsed
                    data_sources[instrument_id] = "yh_finance"
                    yh_available[instrument_id] = True
                else:
                    # Both broker and YH Finance failed — remove from universe.
                    print(
                        f"[DataPipeline] WARNING: no usable data for {instrument_id}"
                        " (cold-start, both broker and YH Finance failed)"
                        " — removing from universe."
                    )
                    self._update_t2_status(
                        instrument_id, "NO",
                        "cold-start fetch returned no usable bars from broker or YH Finance"
                    )
                    self._remove_from_universe(instrument_id)
                    removed_instruments.append(instrument_id)
                    yh_available[instrument_id] = False

        prices = self._clean_prices(prices)

        # --- Investable universe log (P1) ---
        universe_size = len(instruments)
        investable_size = len(prices)
        pct_active = (investable_size / universe_size * 100) if universe_size > 0 else 0.0
        log_msg = (
            f"[DataPipeline] Investable universe: {investable_size}/{universe_size} active"
            f" ({pct_active:.1f}%)"
        )
        if candidates_total > 0:
            pct_total = investable_size / candidates_total * 100
            log_msg += f", {investable_size}/{candidates_total} across all candidates ({pct_total:.1f}%)"
        print(log_msg)

        # --- Candidates report (non-blocking side effect) ---
        try:
            self._write_candidates_report(
                instruments, prices, data_sources, broker_available, yh_available,
                master=master, all_candidates=all_candidates,
            )
        except Exception as exc:
            print(f"[DataPipeline] WARNING: candidates report write failed — {exc}")

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
            # P10: drop series columns for instruments removed from universe this run
            # so the scoper's in_series_not_in_universe list does not grow unbounded.
            for epic in removed_instruments:
                for sheet_name, df in master.items():
                    if epic in df.columns:
                        master[sheet_name] = df.drop(columns=[epic])
                        print(f"[DataPipeline] Dropped stale series column for {epic} (T2=NO).")
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
        master: "dict | None" = None,
        all_candidates: "list | None" = None,
    ) -> None:
        """Write a CSV report of data-source outcomes for every candidate instrument.

        P2: All entries from ``universe_candidates.json`` are included, not only
        instruments currently in ``universe.json``.  Candidates that failed T1 or T2
        appear with ``data_source=none`` and zero bar counts so the full funnel is
        visible without dropping failures from the sheet.

        P3: ``bars_fetched_this_run`` replaces the old ``non_zero_data_points`` column
        (which reflected the current run's fetched bars, not the master series total).
        A new ``total_bars_in_master`` column counts the non-NaN rows stored in the
        master series for each epic, making the discrepancy explicit.

        The ``validation_passed`` column is left blank here and filled in
        later by ``StrategyEval``.
        """
        output_dir = self.config.get("output_dir", "data/output")
        os.makedirs(output_dir, exist_ok=True)
        report_path = os.path.join(output_dir, self.CANDIDATES_REPORT_FILENAME)

        def _bars_fetched(instrument_id: str) -> int:
            """Count non-zero close values fetched this run for instrument_id."""
            close_vals = prices.get(instrument_id, {}).get("close", [])
            return sum(1 for v in close_vals if v is not None and v != 0)

        def _bars_in_master(instrument_id: str) -> int:
            """Count non-NaN rows in the master series for instrument_id."""
            if master is None:
                return 0
            ref_sheet = master.get(self.SHEET_NAMES[0])
            if ref_sheet is None or ref_sheet.empty:
                return 0
            if instrument_id not in ref_sheet.columns:
                return 0
            return int(ref_sheet[instrument_id].notna().sum())

        rows = []
        seen_epics: set[str] = set()

        # P2: iterate ALL candidates from universe_candidates.json first,
        # so failures are not silently dropped from the output sheet.
        for candidate in (all_candidates or []):
            cand_epic = candidate.get("epic", "")
            if not cand_epic:
                continue
            seen_epics.add(cand_epic)
            rows.append({
                "epic": cand_epic,
                "name": candidate.get("name", ""),
                "t1_status": candidate.get("t1_status", ""),
                "t2_status": candidate.get("t2_status", ""),
                "data_source": data_sources.get(cand_epic, "none"),
                "bars_fetched_this_run": _bars_fetched(cand_epic),
                "total_bars_in_master": _bars_in_master(cand_epic),
                "broker_data_available": broker_available.get(cand_epic, False),
                "yh_data_available": yh_available.get(cand_epic, False),
                "validation_passed": "",  # filled in by StrategyEval
            })

        # Include any active-universe instruments not present in candidates.json.
        for instrument_id in instruments:
            if instrument_id in seen_epics:
                continue
            rows.append({
                "epic": instrument_id,
                "name": "",
                "t1_status": "",
                "t2_status": "",
                "data_source": data_sources.get(instrument_id, "none"),
                "bars_fetched_this_run": _bars_fetched(instrument_id),
                "total_bars_in_master": _bars_in_master(instrument_id),
                "broker_data_available": broker_available.get(instrument_id, False),
                "yh_data_available": yh_available.get(instrument_id, False),
                "validation_passed": "",
            })

        fieldnames = [
            "epic",
            "name",
            "t1_status",
            "t2_status",
            "data_source",
            "bars_fetched_this_run",
            "total_bars_in_master",
            "broker_data_available",
            "yh_data_available",
            "validation_passed",
        ]
        with open(report_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    # ------------------------------------------------------------------
    # Incremental fetch helpers
    # ------------------------------------------------------------------

    def _get_candidate_t2_status(self, instrument_id: str) -> str:
        """Return the last known T2 status for instrument_id from universe_candidates.json.

        Used in P7-log to surface the previous T2 status alongside a skip warning,
        distinguishing a first-time failure (previously PENDING_T2) from a regression
        (previously YES).  Returns ``"UNKNOWN"`` when the file is absent or unreadable.
        """
        if not os.path.isfile(self.CANDIDATES_PATH):
            return "UNKNOWN"
        try:
            with open(self.CANDIDATES_PATH) as f:
                data = json.load(f)
            for candidate in data.get("candidates", []):
                if candidate.get("epic") == instrument_id:
                    return candidate.get("t2_status", "UNKNOWN")
        except Exception:
            pass
        return "UNKNOWN"

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
        if close_sheet is None or close_sheet.empty:
            return None
        if instrument_id not in close_sheet.columns:
            return None
        close_series = close_sheet[instrument_id].dropna()
        if close_series.empty:
            return None
        timestamps = [ts.strftime("%Y-%m-%dT%H:%M:%S") for ts in close_series.index]
        return {
            "close": close_series.tolist(),
            "high": [None] * len(timestamps),
            "low": [None] * len(timestamps),
            "volume": [None] * len(timestamps),
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

        close, high, low, volume = [], [], [], []
        timestamps: list = []

        for bar in bars:
            close.append(bar.get("close"))
            high.append(bar.get("high"))
            low.append(bar.get("low"))
            volume.append(bar.get("volume"))
            timestamps.append(bar.get("timestamp"))

        return {
            "close": close,
            "high": high,
            "low": low,
            "volume": volume,
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
        """Convert per-instrument price lists into dict of one DataFrame (mid_close)."""
        series_dict = {}
        for instrument_id, fields in prices.items():
            ts = fields.get("timestamps", [])
            vals = fields.get("close", [])
            index = pd.to_datetime(ts, utc=True, errors="coerce")
            series_dict[instrument_id] = pd.Series(vals, index=index, dtype=float)
        return {"mid_close": pd.DataFrame(series_dict) if series_dict else pd.DataFrame()}

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

        # (1) All expected sheet names present
        for name in self.SHEET_NAMES:
            if name not in series:
                print(f"[DataPipeline] VALIDATION: missing sheet '{name}'.")
                valid = False
        if not valid:
            return False

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

            # (4) Index is sorted ascending
            if not df.index.is_monotonic_increasing:
                print(f"[DataPipeline] VALIDATION: sheet '{name}' index is not sorted ascending.")
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
        for name in self.SHEET_NAMES:
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
