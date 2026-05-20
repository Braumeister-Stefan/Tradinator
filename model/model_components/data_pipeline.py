"""
Tradinator — Data Pipeline.

Downloads historical market data for each instrument via the broker adapter
and performs basic cleaning (forward-fill).  Persists consolidated series to
a master csv file as a side effect.

Two-tier validation integration
--------------------------------
T2 (data availability) validation status for each instrument is continuously
maintained in ``universe_candidates.json``:

- On a **cold-start** (no stored series for the instrument): broker-only with
  2 retries (2s backoff) via the fixed-count API.  If all attempts return
  zero bars or raise, the instrument is marked T2=NO and removed from the
  universe.
- On a **subsequent run** (stored series exists): fetches only bars after the
  last stored timestamp using the date-range API, so the master file grows
  incrementally — one or more new bars per run rather than re-fetching the
  same window repeatedly.

A per-run ``candidates_report.csv`` is written to ``data/output/`` recording
the data-source outcome for every universe instrument.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""


import csv
import datetime
import json
import os
import time

import pandas as pd

from data.input import registry_io


def load_universe(path: str) -> list[str]:
    """Load the instrument universe from a CSV file.

    Returns a deduplicated list of broker-agnostic conId strings.
    Skips entries where valid=False.
    """
    if not os.path.isfile(path):
        print(f"ERROR: Universe file not found: {path}")
        print(
            "Populate data/input/universe.csv manually with IBKR canonical symbols "
            "(e.g. 'DAX', 'EURUSD')."
        )
        raise SystemExit(1) from None

    rows = registry_io.load_universe_rows(path)
    if not rows:
        print(
            f"WARNING: No instruments found in {path} — pipeline will run with an empty universe. "
            "Populate data/input/universe.csv manually with IBKR canonical symbol strings."
        )

    seen: set[str] = set()
    symbols: list[str] = []
    n_excluded = 0
    n_invalid = 0
    for inst in rows:
        iid = inst.get("conId", "")
        if not iid:
            continue
        if inst.get("overwrite_exclusion", False):
            n_excluded += 1
            continue
        if not inst.get("valid", True):
            n_invalid += 1
            continue
        if iid in seen:
            continue
        seen.add(iid)
        symbols.append(iid)

    total_rows = len(rows)
    print(f"[Universe] Loaded: {total_rows} instrument(s) from universe.csv.")
    if n_excluded > 0:
        print(f"[Universe] Manually excluded: {n_excluded} (overwrite_exclusion=True).")
    if n_invalid > 0:
        print(f"[Universe] Invalid rows skipped: {n_invalid} (valid=False).")
    return symbols


def filter_by_history(universe: list[str], config: dict) -> list[str]:
    """Drop conIds whose stored history is shorter than ``min_history_years``.

    Reads ``oldest_bar_date`` per conId from ``candidates_report.csv``
    (written by DataPipeline and the backfill diagnostic tool). conIds with
    missing/empty ``oldest_bar_date`` are kept (no information to filter on).
    The underlying ``universe.csv`` is NOT modified — only the in-memory
    active list is filtered.
    """
    min_years = float(config.get("min_history_years", 0) or 0)
    if min_years <= 0:
        return list(universe)

    output_dir = config.get("output_dir", "data/output")
    report_path = os.path.join(output_dir, "candidates_report.csv")
    if not os.path.isfile(report_path):
        print(
            f"[Universe] WARNING: {report_path} not found — "
            "cannot enforce min_history_years; keeping full universe."
        )
        return list(universe)

    oldest_by_id: dict[str, str] = {}
    with open(report_path, "r", newline="") as fh:
        for row in csv.DictReader(fh):
            cid = row.get("conId", "")
            if cid:
                oldest_by_id[cid] = (row.get("oldest_bar_date") or "").strip()

    threshold = datetime.date.today() - datetime.timedelta(days=int(min_years * 365))
    kept: list[str] = []
    n_dropped = 0
    n_missing = 0
    for cid in universe:
        raw = oldest_by_id.get(cid, "")
        if not raw:
            n_missing += 1
            kept.append(cid)
            continue
        try:
            oldest = datetime.date.fromisoformat(raw[:10])
        except ValueError:
            n_missing += 1
            kept.append(cid)
            continue
        if oldest > threshold:
            n_dropped += 1
            continue
        kept.append(cid)

    if n_missing == len(universe) and len(universe) > 0:
        print(
            "[Universe] WARNING: oldest_bar_date is missing for every active"
            " conId — history filter is a no-op. Run the pipeline once or run"
            " diagnostic_tools/backfill_universe before relying on this filter."
        )
    print(
        f"[Universe] History filter (>={min_years}y):"
        f" dropped {n_dropped}, kept {len(kept)} of {len(universe)}"
        f" (no-info-kept {n_missing})."
    )
    return kept


def filter_by_gaps(universe: list[str], config: dict) -> list[str]:
    """Drop conIds whose timeseries contains a gap larger than ``gap_tolerance``.

    A gap is a consecutive run of NaN values in the stored master series.
    Only the active in-memory list is filtered; ``universe.csv`` is not modified.

    Parameters
    ----------
    gap_resolution:
        ``"drop_gap"`` (default) removes assets that exceed the tolerance.
        ``"flat_fill"`` is a placeholder; not yet implemented; returns full list.
    gap_tolerance:
        Maximum acceptable consecutive-NaN run length.  Default ``0`` means any
        single missing bar causes the asset to be dropped.
    """
    gap_resolution = config.get("gap_resolution", "drop_gap")
    if gap_resolution != "drop_gap":
        print(
            f"[Universe] gap_resolution='{gap_resolution}' is not yet "
            "implemented — skipping gap filter."
        )
        return list(universe)

    gap_tolerance = int(config.get("gap_tolerance", 0) or 0)

    series_path = "data/input/universe_series.csv"
    if not os.path.isfile(series_path):
        print(
            f"[Universe] WARNING: {series_path} not found — "
            "cannot enforce gap_tolerance; keeping full universe."
        )
        return list(universe)

    try:
        df = pd.read_csv(series_path, index_col=0)
    except Exception as exc:  # noqa: BLE001
        print(
            f"[Universe] WARNING: could not read {series_path} ({exc}) — "
            "keeping full universe."
        )
        return list(universe)

    kept: list[str] = []
    n_dropped = 0
    n_missing = 0
    for cid in universe:
        if cid not in df.columns:
            n_missing += 1
            kept.append(cid)
            continue
        col = df[cid]
        nan_mask = col.isna()
        if not nan_mask.any():
            kept.append(cid)
            continue
        # Longest consecutive NaN run via cumsum-group trick.
        groups = nan_mask.ne(nan_mask.shift()).cumsum()
        max_gap = int(nan_mask.groupby(groups).sum().max())
        if max_gap > gap_tolerance:
            n_dropped += 1
        else:
            kept.append(cid)

    print(
        f"[Universe] Gap filter (max consecutive NaN={gap_tolerance}):"
        f" dropped {n_dropped}, kept {len(kept)} of {len(universe)}"
        f" (no-series-kept {n_missing})."
    )
    return kept


class DataPipeline:
    """Fetch and clean historical price data for every instrument in the universe."""

    DEFAULT_RESOLUTION = "DAY"
    DEFAULT_LOOKBACK = 50
    FILL_METHOD = "ffill"  # forward-fill for missing values
    RATE_LIMIT_DELAY = 1.0  # seconds between API calls
    SERIES_FILE = "data/input/universe_series.csv"  # master file path
    HISTORIC_DIR = "data/input/historic_series"  # historic ingest folder
    SHEET_NAMES = ("mid_close",)  # only mid-price close is stored
    CANDIDATES_PATH = "data/input/universe_candidates.csv"
    UNIVERSE_PATH = "data/input/universe.csv"
    CANDIDATES_REPORT_FILENAME = "candidates_report.csv"  # written to output_dir
    METADATA_CACHE_FILE = "data/input/instrument_metadata_cache.json"
    CHECKPOINT_INTERVAL = 50  # flush master series to disk every N instruments

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, broker_state: dict, revalidate: bool = False) -> dict:
        """Download prices for each instrument, clean them, and return market_data.

        Parameters
        ----------
        broker_state:
            Output dict from BrokerConnector / Reconciliation.
        revalidate:
            When ``False`` (default) only instruments that already have stored
            series data are fetched (incremental update only).  Instruments
            with no stored data are silently skipped — they are NOT removed
            from the universe and will be cold-started on the next run where
            ``revalidate=True`` is passed.
            When ``True`` the full cold-start + T2-validation path runs for
            every instrument without stored data.

        Fetch strategy
        --------------
        1. Load the existing master series file **before** the fetch loop so the
           last stored timestamp can be determined per instrument.
        2. For instruments with no stored data (cold start): if ``revalidate``
           is ``True``, call the fixed-count API (``fetch_historical_prices``)
           with the configured ``lookback``, retrying up to 2 additional times
           with 2-second backoff if the broker returns zero bars or raises.
           If all attempts fail, the instrument is marked T2=NO and removed
           from the universe.  If ``revalidate`` is ``False``, the instrument
           is skipped.
        3. For instruments with existing stored data: call the date-range API
           (``fetch_historical_prices_by_date_range``) from the last stored
           timestamp + 1 second, retrieving only genuinely new bars.
        4. After each fetch, update the T2 status in ``universe_candidates.json``
           and — if the fetch returned zero bars — remove the instrument from
           ``universe.json``.
        5. Every ``CHECKPOINT_INTERVAL`` instruments the accumulated prices are
           merged into master and flushed to disk so partial progress survives
           a process interruption during long revalidation runs.
        """
        adapter = broker_state["adapter"]
        instruments = broker_state["instruments"]
        resolution = self.config.get("resolution", self.DEFAULT_RESOLUTION)
        lookback = self.config.get("lookback", self.DEFAULT_LOOKBACK)

        # Load master series before the fetch loop (needed for last-date lookup).
        master = self._load_master_series(self.SERIES_FILE)

        # P1-log/P2: load all candidates once for reporting and logging.
        all_candidates: list = []
        candidates_total = 0
        try:
            if os.path.isfile(self.CANDIDATES_PATH):
                all_candidates = registry_io.load_candidate_rows(self.CANDIDATES_PATH)
                candidates_total = len(all_candidates)
        except Exception as _exc:
            print(f"[DataPipeline] WARNING: could not load candidates file — {_exc}")

        prices = {}
        # Track the data source used for each instrument.
        data_sources: dict[str, str] = {}
        # Track whether broker succeeded per instrument for the report.
        broker_available: dict[str, bool] = {}
        # P10: track instruments removed from universe this run (for series cleanup).
        removed_instruments: list[str] = []
        # Count instruments served from cache (weekend / already-current).
        cache_served = 0

        today_utc = datetime.datetime.now(datetime.timezone.utc).date()

        n_instruments = len(instruments)
        _last_pct_milestone = -1
        did_fetch = False
        for i, conId in enumerate(instruments):
            if n_instruments > 0:
                pct = int((i + 1) / n_instruments * 100)
                milestone = pct - (pct % 5)
                if milestone > _last_pct_milestone:
                    _last_pct_milestone = milestone
                    print(
                        f"\r[DataPipeline] Retrieving recent prices: {milestone}%"
                        f" ({i + 1}/{n_instruments})",
                        end="",
                        flush=True,
                    )
            if did_fetch:
                time.sleep(self.RATE_LIMIT_DELAY)
            did_fetch = False

            # Checkpoint: flush accumulated prices to disk every N instruments
            # so partial progress is preserved during long revalidation runs.
            if i > 0 and i % self.CHECKPOINT_INTERVAL == 0:
                master = self._flush_to_master(prices, master)

            last_date = self._get_last_stored_date(conId, master)

            # Intra-day skip guard (DAY resolution only):
            # Skip the broker call and serve the cached series when either:
            #   (a) today's bar is already stored (days_since == 0), or
            #   (b) today is a weekend day and the last bar is from this week
            #       (days_since <= 3) — no new bar can have closed on a Saturday
            #       or Sunday regardless of how many times the loop runs.
            if resolution == "DAY" and last_date is not None:
                last_ts = self._get_last_stored_ts(conId, master)
                if last_ts is not None:
                    days_since = (today_utc - last_ts.date()).days
                    is_today = days_since == 0
                    is_weekend_no_new_bar = today_utc.weekday() >= 5 and days_since <= 3
                    if is_today or is_weekend_no_new_bar:
                        parsed = self._reconstruct_from_master(conId, master)
                        if parsed is not None:
                            if is_today:
                                reason = f"last bar is today ({today_utc})"
                            else:
                                reason = (
                                    f"weekend — last bar {last_ts.date()},"
                                    f" no new DAY bar on {today_utc}"
                                )
                            prices[conId] = parsed
                            data_sources[conId] = "master_cache"
                            broker_available[conId] = True
                            cache_served += 1
                            continue

            if last_date is not None:
                # Incremental fetch — retrieve only bars after the last stored bar.
                did_fetch = True
                print(
                    f"[DataPipeline] Incremental fetch {conId}"
                    f" from {last_date} ({resolution})…"
                )
                try:
                    bars = adapter.fetch_historical_prices_by_date_range(
                        conId, resolution, last_date
                    )
                except Exception as exc:
                    print(
                        f"[DataPipeline] WARNING: incremental fetch failed for"
                        f" {conId} — {exc}. Falling back to fixed-count fetch."
                    )
                    try:
                        bars = adapter.fetch_historical_prices(
                            conId, resolution, lookback
                        )
                    except Exception as exc2:
                        last_t2 = self._get_candidate_t2_status(conId)  # P7-log
                        print(
                            f"[DataPipeline] WARNING: skipping {conId} — {exc2} (last known T2={last_t2})"
                        )
                        if revalidate:
                            self._update_t2_status(conId, "NO", f"fetch exception: {exc2}")
                            self._remove_from_universe(conId)
                            removed_instruments.append(conId)
                        broker_available[conId] = False
                        continue

                parsed = self._bars_to_columns(bars)
                if parsed is None:
                    # Zero bars on an incremental fetch is normal on weekends and
                    # public holidays — it means no new bar has closed since the last
                    # stored bar.  This is NOT a T2 failure; do not remove the
                    # instrument from the universe.  Serve the existing stored series
                    # for this pipeline run instead of dropping the instrument.
                    print(
                        f"[DataPipeline] {conId}: 0 new bars since {last_date}"
                        " (non-trading day or no new data) — retaining stored series."
                    )
                    parsed = self._reconstruct_from_master(conId, master)
                    if parsed is None:
                        print(
                            f"[DataPipeline] WARNING: no stored series for {conId},"
                            " skipping this run."
                        )
                        broker_available[conId] = False
                        continue
                elif revalidate:
                    self._update_t2_status(
                        conId, "YES", f"{len(bars)} new bar(s) fetched"
                    )

                prices[conId] = parsed
                data_sources[conId] = "broker"
                broker_available[conId] = True
            else:
                # No stored data for this instrument.
                if not revalidate:
                    # No stored data on a non-revalidation run: remove from universe.
                    # The instrument will be cold-started if re-added via revalidation.
                    self._remove_from_universe(conId)
                    removed_instruments.append(conId)
                    continue

                # Cold start — fetch the configured lookback window with up to
                # 2 retries (2s backoff) before declaring T2=NO.
                did_fetch = True
                print(
                    f"[DataPipeline] Cold-start fetch {conId}"
                    f" ({resolution}, {lookback} bars)…"
                )
                broker_ok = False
                parsed = None
                bars: list = []
                attempts = 3  # initial + 2 retries
                for attempt in range(1, attempts + 1):
                    try:
                        bars = adapter.fetch_historical_prices(
                            conId, resolution, lookback
                        )
                        parsed = self._bars_to_columns(bars)
                        if parsed is not None and not self._all_none(parsed):
                            broker_ok = True
                            break
                        print(
                            f"[DataPipeline] Cold-start {conId}: attempt {attempt}/{attempts}"
                            " returned no usable bars."
                        )
                    except Exception as exc:
                        print(
                            f"[DataPipeline] Cold-start {conId}: attempt {attempt}/{attempts}"
                            f" raised — {exc}"
                        )
                    if attempt < attempts:
                        time.sleep(2.0)

                broker_available[conId] = broker_ok

                if broker_ok:
                    self._update_t2_status(
                        conId, "YES", f"{len(bars)} bar(s) fetched (cold-start)"
                    )
                    prices[conId] = parsed
                    data_sources[conId] = "broker"
                    continue

                # All cold-start attempts failed — remove from universe.
                print(
                    f"[DataPipeline] WARNING: no usable data for {conId}"
                    " (cold-start, broker returned nothing after 3 attempts)"
                    " — removing from universe."
                )
                self._update_t2_status(
                    conId, "NO",
                    "cold-start fetch returned no usable bars from broker after 3 attempts"
                )
                self._remove_from_universe(conId)
                removed_instruments.append(conId)

        prices = self._clean_prices(prices)

        # Finalise the progress line before any post-loop output.
        if n_instruments > 0:
            print()

        # --- Cache-served summary (printed once per run) ---
        universe_size = len(instruments)
        if cache_served > 0 and universe_size > 0:
            pct_cache = cache_served / universe_size * 100
            print(
                f"[DataPipeline] {cache_served}/{universe_size} instruments"
                f" ({pct_cache:.1f}%) served from cache"
                " (no new DAY bar — weekend or already current)."
            )

        # --- Active universe log (P1) ---
        universe_size = len(instruments)
        active_size = len(prices)
        pct_active = (active_size / universe_size * 100) if universe_size > 0 else 0.0
        log_msg = (
            f"[DataPipeline] Active universe: {active_size}/{universe_size} priced"
            f" ({pct_active:.1f}%)"
        )
        if candidates_total > 0:
            pct_total = active_size / candidates_total * 100
            log_msg += f"; {active_size}/{candidates_total} of all candidates ({pct_total:.1f}%)"
        log_msg += "."
        print(log_msg)

        # --- Candidates report (non-blocking side effect) ---
        try:
            self._write_candidates_report(
                instruments, prices, data_sources, broker_available,
                master=master, all_candidates=all_candidates,
            )
        except Exception as exc:
            print(f"[DataPipeline] WARNING: candidates report write failed — {exc}")

        # --- Fetch instrument metadata with dealing rules ---
        metadata_cache = self._load_metadata_cache()
        instrument_metadata: dict = {}
        did_metadata_fetch = False
        skipped_metadata = 0
        fetched_ok = 0
        metadata_failed_ids: list[str] = []
        total_metadata = len(prices)
        # Snapshot keys: under revalidate=True the loop may flag instruments
        # for removal, which mutates ``prices`` after the loop. Iterating a
        # snapshot avoids "dictionary changed size during iteration".
        for conId in list(prices):
            # Metadata skip guard.
            # revalidate=False  → use any existing cache entry regardless of age;
            #                     fetch only for instruments not yet in cache.
            # revalidate=True   → always fetch fresh data (cache is bypassed).
            cached = metadata_cache.get(conId)
            if not revalidate and cached:
                try:
                    if "fetched_date" in cached:
                        instrument_metadata[conId] = {
                            k: v for k, v in cached.items() if k != "fetched_date"
                        }
                        skipped_metadata += 1
                        continue
                except (TypeError, AttributeError):
                    pass  # malformed cache entry — fall through to live fetch

            # Live fetch with retries. Under revalidate=True a persistent
            # failure causes exclusion from the universe (no fallback dict);
            # under revalidate=False the existing fallback dict is used so a
            # transient broker hiccup does not silently shrink the universe
            # on a routine run.
            attempts = 3 if revalidate else 1
            info = None
            last_exc: "Exception | None" = None
            for attempt in range(1, attempts + 1):
                if did_metadata_fetch:
                    time.sleep(self.RATE_LIMIT_DELAY)
                did_metadata_fetch = True
                try:
                    info = adapter.fetch_instrument_info(conId)
                    break
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    if attempt < attempts:
                        print(
                            f"[DataPipeline] Metadata fetch {conId}:"
                            f" attempt {attempt}/{attempts} failed — {exc}"
                        )
                        time.sleep(2.0)

            if info is not None:
                instrument_metadata[conId] = info
                metadata_cache[conId] = {**info, "fetched_date": today_utc.isoformat()}
                fetched_ok += 1
                continue

            # All attempts failed.
            if revalidate:
                print(
                    f"[DataPipeline] WARNING: metadata fetch failed for {conId}"
                    f" after {attempts} attempt(s) — {last_exc}."
                    " Removing from universe (revalidate run)."
                )
                metadata_failed_ids.append(conId)
            else:
                print(
                    f"[DataPipeline] WARNING: metadata fetch failed for {conId}"
                    f" — {last_exc}. Using fallback metadata."
                )
                instrument_metadata[conId] = {
                    "instrument_name": conId,
                    "conId": conId,
                    "currency": "Unknown",
                    "min_deal_size": 0.01,
                    "max_deal_size": None,
                    "min_size_increment": 1.0,
                    "scaling_factor": 1,
                    "dealing_enabled": True,
                    "buy_allowed": True,
                    "sell_allowed": True,
                }

        # --- Apply batched exclusions from metadata failures (revalidate only) ---
        # Persist the cache first so any successfully fetched entries are not
        # lost if a subsequent disk write fails mid-cleanup.
        try:
            self._save_metadata_cache(metadata_cache)
        except Exception as exc:
            print(f"[DataPipeline] WARNING: metadata cache save failed — {exc}")

        for conId in metadata_failed_ids:
            prices.pop(conId, None)
            data_sources.pop(conId, None)
            broker_available.pop(conId, None)
            instrument_metadata.pop(conId, None)
            self._remove_from_universe(conId)
            removed_instruments.append(conId)

        # --- Summary log ---
        print(
            f"[DataPipeline] Metadata: fetched {fetched_ok},"
            f" cached {skipped_metadata},"
            f" excluded {len(metadata_failed_ids)} of {total_metadata} instrument(s)."
        )

        # --- Persistence side effect ---
        try:
            live_frames = self._build_dataframes(prices)
            if master is None:
                master = {name: pd.DataFrame() for name in self.SHEET_NAMES}
            master = self._ingest_historic_files(master, self.HISTORIC_DIR)
            master = self._merge_series(live_frames, master)
            # P10: drop series columns for instruments removed from universe this run
            # so the scoper's in_series_not_in_universe list does not grow unbounded.
            # Collect all columns to drop first, then drop per sheet in one call.
            if removed_instruments:
                for sheet_name, df in master.items():
                    cols_to_drop = [e for e in removed_instruments if e in df.columns]
                    if cols_to_drop:
                        master[sheet_name] = df.drop(columns=cols_to_drop)
                        for conId in cols_to_drop:
                            print(f"[DataPipeline] Dropped stale series column for {conId} (T2=NO).")
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

    # ------------------------------------------------------------------
    # Candidates report
    # ------------------------------------------------------------------

    def _write_candidates_report(
        self,
        instruments: list[str],
        prices: dict,
        data_sources: dict,
        broker_available: dict,
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
        master series for each conId, making the discrepancy explicit.

        The ``validation_passed`` column is left blank here and filled in
        later by ``StrategyEval``.
        """
        output_dir = self.config.get("output_dir", "data/output")
        os.makedirs(output_dir, exist_ok=True)
        report_path = os.path.join(output_dir, self.CANDIDATES_REPORT_FILENAME)

        def _bars_fetched(conId: str) -> int:
            """Count non-zero close values fetched this run for conId."""
            close_vals = prices.get(conId, {}).get("close", [])
            return sum(1 for v in close_vals if v is not None and v != 0)

        def _bars_in_master(conId: str) -> int:
            """Count non-NaN rows in the master series for conId."""
            if master is None:
                return 0
            ref_sheet = master.get(self.SHEET_NAMES[0])
            if ref_sheet is None or ref_sheet.empty:
                return 0
            if conId not in ref_sheet.columns:
                return 0
            return int(ref_sheet[conId].notna().sum())

        def _oldest_bar_date(conId: str) -> str:
            """Return ISO date of the oldest non-NaN bar in master, or ''."""
            if master is None:
                return ""
            ref_sheet = master.get(self.SHEET_NAMES[0])
            if ref_sheet is None or ref_sheet.empty or conId not in ref_sheet.columns:
                return ""
            col = ref_sheet[conId].dropna()
            if col.empty:
                return ""
            return col.index.min().date().isoformat()

        rows = []
        seen_instruments: set[str] = set()

        # Preserve three backfill-related columns from the existing on-disk
        # report (written by diagnostic_tools/backfill_universe), so the live
        # pipeline does not blow them away.  Best effort: missing/unreadable
        # file is ignored silently.
        preserved_by_id: dict[str, dict] = {}
        _preserved_cols = (
            "total_bars_in_master_backfill",
            "oldest_bar_date",
            "most_recent_bar_date",
        )
        if os.path.isfile(report_path):
            try:
                with open(report_path, "r", newline="") as _existing:
                    _reader = csv.DictReader(_existing)
                    for _row in _reader:
                        _cid = _row.get("conId", "")
                        if not _cid:
                            continue
                        preserved_by_id[_cid] = {
                            _k: _row.get(_k, "") for _k in _preserved_cols if _k in _row
                        }
            except Exception:
                preserved_by_id = {}

        def _merge_preserved(row: dict) -> dict:
            extras = preserved_by_id.get(row["conId"], {})
            for k in _preserved_cols:
                if k in extras:
                    row[k] = extras[k]
            # Live oldest_bar_date from master overrides preserved value when
            # master actually has data for this conId; otherwise the preserved
            # value (written by backfill) wins.
            live_oldest = _oldest_bar_date(row["conId"])
            if live_oldest:
                row["oldest_bar_date"] = live_oldest
            return row

        # P2: iterate ALL candidates from universe_candidates.json first,
        # so failures are not silently dropped from the output sheet.
        for candidate in (all_candidates or []):
            cand_id = candidate.get("conId", "")
            if not cand_id:
                continue
            seen_instruments.add(cand_id)
            t1 = candidate.get("t1_status", "")
            t2 = candidate.get("t2_status", "")
            pre_passed = "" if (t1 == "PASS" and t2 == "YES") else "false"
            rows.append(_merge_preserved({
                "conId": cand_id,
                "name": candidate.get("name", ""),
                "t1_status": t1,
                "t2_status": t2,
                "data_source": data_sources.get(cand_id, "none"),
                "bars_fetched_this_run": _bars_fetched(cand_id),
                "total_bars_in_master": _bars_in_master(cand_id),
                "broker_data_available": broker_available.get(cand_id, False),
                "validation_passed": pre_passed,
            }))

        # Include any active-universe instruments not present in candidates.json.
        for conId in instruments:
            if conId in seen_instruments:
                continue
            rows.append(_merge_preserved({
                "conId": conId,
                "name": "",
                "t1_status": "",
                "t2_status": "",
                "data_source": data_sources.get(conId, "none"),
                "bars_fetched_this_run": _bars_fetched(conId),
                "total_bars_in_master": _bars_in_master(conId),
                "broker_data_available": broker_available.get(conId, False),
                "validation_passed": "",
            }))

        fieldnames = [
            "conId",
            "name",
            "t1_status",
            "t2_status",
            "data_source",
            "bars_fetched_this_run",
            "total_bars_in_master",
            "broker_data_available",
            "validation_passed",
            "total_bars_in_master_backfill",
            "oldest_bar_date",
            "most_recent_bar_date",
        ]
        with open(report_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    # ------------------------------------------------------------------
    # Incremental fetch helpers
    # ------------------------------------------------------------------

    def _load_metadata_cache(self) -> dict:
        """Load the persisted instrument metadata cache from disk."""
        if not os.path.isfile(self.METADATA_CACHE_FILE):
            print(
                f"[DataPipeline] Metadata cache not found ({self.METADATA_CACHE_FILE})"
                " — regenerating from broker. This takes ~1s per instrument."
            )
            return {}
        try:
            with open(self.METADATA_CACHE_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            return {}

    def _save_metadata_cache(self, cache: dict) -> None:
        """Persist the instrument metadata cache to disk atomically."""
        os.makedirs(os.path.dirname(self.METADATA_CACHE_FILE) or ".", exist_ok=True)
        tmp = self.METADATA_CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(cache, fh, indent=2)
        os.replace(tmp, self.METADATA_CACHE_FILE)

    def _get_candidate_t2_status(self, conId: str) -> str:
        """Return the last known T2 status for conId from universe_candidates.json.

        Used in P7-log to surface the previous T2 status alongside a skip warning,
        distinguishing a first-time failure (previously PENDING_T2) from a regression
        (previously YES).  Returns ``"UNKNOWN"`` when the file is absent or unreadable.
        """
        if not os.path.isfile(self.CANDIDATES_PATH):
            return "UNKNOWN"
        try:
            rows = registry_io.load_candidate_rows(self.CANDIDATES_PATH)
            for candidate in rows:
                if candidate.get("conId") == conId:
                    return candidate.get("t2_status", "UNKNOWN")
        except Exception:
            pass
        return "UNKNOWN"

    def _get_last_stored_date(
        self, conId: str, master: "dict | None"
    ) -> "str | None":
        """Return ISO-8601 UTC timestamp (last stored + 1 second) for incremental fetch.

        Uses the ``mid_close`` sheet as the reference.  Returns ``None`` when
        no stored data exists for *conId*, indicating a cold start.
        """
        if master is None:
            return None
        ref_sheet = master.get(self.SHEET_NAMES[0])  # mid_close
        if ref_sheet is None or ref_sheet.empty:
            return None
        if conId not in ref_sheet.columns:
            return None
        series = ref_sheet[conId].dropna()
        if series.empty:
            return None
        last_ts = series.index.max()
        # Add 1 second so the last stored bar is not re-fetched.
        next_ts = last_ts + pd.Timedelta(seconds=1)
        return next_ts.strftime("%Y-%m-%dT%H:%M:%S")

    def _get_last_stored_ts(
        self, conId: str, master: "dict | None"
    ) -> "pd.Timestamp | None":
        """Return the raw last stored UTC Timestamp for *conId* (no +1s offset).

        Used by the intra-day skip guard to compare bar dates without the
        offset that ``_get_last_stored_date`` applies for broker fetch calls.
        Returns ``None`` when no stored data exists for *conId*.
        """
        if master is None:
            return None
        ref_sheet = master.get(self.SHEET_NAMES[0])  # mid_close
        if ref_sheet is None or ref_sheet.empty:
            return None
        if conId not in ref_sheet.columns:
            return None
        series = ref_sheet[conId].dropna()
        if series.empty:
            return None
        return series.index.max()

    def _reconstruct_from_master(
        self, conId: str, master: "dict | None"
    ) -> "dict | None":
        """Rebuild a bars-as-columns dict from stored master series for one instrument.

        Used when an incremental fetch returns zero bars (e.g., on weekends) so
        the pipeline can still provide the previously-stored series to downstream
        components without re-fetching.

        Returns ``None`` if no stored series exists for *conId*.
        """
        if master is None:
            return None
        close_sheet = master.get("mid_close")
        if close_sheet is None or close_sheet.empty:
            return None
        if conId not in close_sheet.columns:
            return None
        close_series = close_sheet[conId].dropna()
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
        self, conId: str, t2_status: str, t2_reason: str
    ) -> None:
        """Update T2 status for an instrument in universe_candidates.json.

        Silently skips if the candidates file does not exist.  Failure to
        update is non-fatal — a warning is printed but the pipeline continues.
        """
        if not os.path.isfile(self.CANDIDATES_PATH):
            return
        try:
            rows = registry_io.load_candidate_rows(self.CANDIDATES_PATH)

            now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            updated = False
            for candidate in rows:
                if candidate.get("conId") == conId:
                    candidate["t2_status"] = t2_status
                    candidate["valid"] = (
                        candidate.get("t1_status") == "PASS" and t2_status == "YES"
                    )
                    candidate["last_validated"] = now_utc
                    updated = True
                    break

            if updated:
                print(f"[DataPipeline] T2 {t2_status} {conId} — {t2_reason}")
                registry_io.save_candidate_rows(rows, self.CANDIDATES_PATH)
        except Exception as exc:
            print(
                f"[DataPipeline] WARNING: could not update T2 status"
                f" for {conId} — {exc}"
            )

    def _remove_from_universe(self, conId: str) -> None:
        """Remove an instrument from universe.json when T2 fails (zero bars returned).

        Mutates ONLY ``UNIVERSE_PATH`` — ``universe_candidates.json`` is left
        untouched (candidates persist across runs; their status fields are
        updated by ``_update_t2_status``).

        Instruments are removed from the machine-read universe so the pipeline
        does not continue trying to trade them.  Re-run the stock scoper to
        re-add an instrument if data becomes available again.

        Silently skips if universe.json does not exist.  Failure is non-fatal.
        """
        if not os.path.isfile(self.UNIVERSE_PATH):
            return
        try:
            rows = registry_io.load_universe_rows(self.UNIVERSE_PATH)

            original_count = len(rows)
            rows = [inst for inst in rows if inst.get("conId") != conId]
            removed = original_count - len(rows)
            if removed > 0:
                print(
                    f"[DataPipeline] Removed {conId} from universe.csv"
                    " (T2=NO, zero bars in last fetch)."
                )
                registry_io.save_universe_rows(rows, self.UNIVERSE_PATH)
        except Exception as exc:
            print(
                f"[DataPipeline] WARNING: could not remove {conId}"
                f" from universe.csv — {exc}"
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
        for conId, fields in prices.items():
            if self._all_none(fields):
                print(f"[DataPipeline] Dropping {conId} — all values are None.")
                continue
            cleaned[conId] = {
                key: (values if key == "timestamps" else self._forward_fill(values))
                for key, values in fields.items()
            }
        return cleaned

    # ------------------------------------------------------------------
    # DataFrame / csv methods
    # ------------------------------------------------------------------

    def _build_dataframes(self, prices: dict) -> dict:
        """Convert per-instrument price lists into dict of one DataFrame (mid_close)."""
        series_dict = {}
        for conId, fields in prices.items():
            ts = fields.get("timestamps", [])
            vals = fields.get("close", [])
            index = pd.to_datetime(ts, utc=True, errors="coerce")
            series_dict[conId] = pd.Series(vals, index=index, dtype=float)
        return {"mid_close": pd.DataFrame(series_dict) if series_dict else pd.DataFrame()}

    @staticmethod
    def _read_csv_series(path: str) -> "pd.DataFrame":
        """Read a single-column-index csv into a UTC-indexed DataFrame.

        Raises pandas' parser exceptions on a malformed file; callers decide
        whether to quarantine or merely warn.
        """
        df = pd.read_csv(path, index_col=0)
        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
        return df

    def _load_master_series(self, path: str) -> "dict | None":
        """Read the master series csv into ``{"mid_close": DataFrame}``.

        On a corrupt or unreadable file the file is quarantined to
        ``<path>.corrupt`` and ``None`` is returned so the caller rebuilds a
        fresh master.  The next save replaces the broken file atomically.
        """
        if not os.path.isfile(path):
            return None
        try:
            df = self._read_csv_series(path)
            return {"mid_close": df}
        except Exception as exc:  # noqa: BLE001 — quarantine on any read failure
            quarantine = path + ".corrupt"
            try:
                if os.path.exists(quarantine):
                    os.remove(quarantine)
                os.rename(path, quarantine)
            except OSError as rename_exc:
                print(
                    f"[DataPipeline] WARNING: master series file {path} is corrupt ({exc}); "
                    f"failed to quarantine ({rename_exc})."
                )
            else:
                print(
                    f"[DataPipeline] WARNING: master series file {path} is corrupt ({exc}); "
                    f"moved to {quarantine}. A fresh master will be rebuilt this run."
                )
            return None

    def _load_historic_series(self, path: str) -> "dict | None":
        """Read a user-supplied historic csv.  Warn on error; never quarantine."""
        if not os.path.isfile(path):
            return None
        try:
            df = self._read_csv_series(path)
            return {"mid_close": df}
        except Exception as exc:  # noqa: BLE001 — user file, just warn
            print(
                f"[DataPipeline] WARNING: could not load historic file {path}"
                f" ({exc}); skipping."
            )
            return None

    # Backwards-compatible shim — kept for any external callers; uses the
    # master loader (quarantines on failure).
    def _load_series_file(self, path: str) -> "dict | None":
        return self._load_master_series(path)

    def _flush_to_master(self, prices: dict, master: "dict | None") -> "dict":
        """Merge current prices into master and save to disk.

        Called periodically during the fetch loop to checkpoint progress.
        Returns the updated master dict so subsequent loop iterations can
        use it for ``_get_last_stored_date`` lookups.
        """
        if not prices:
            return master or {name: pd.DataFrame() for name in self.SHEET_NAMES}
        frames = self._build_dataframes(prices)
        if master is None:
            master = {name: pd.DataFrame() for name in self.SHEET_NAMES}
        master = self._merge_series(frames, master)
        try:
            self._save_series_file(master, self.SERIES_FILE)
        except Exception as exc:
            print(f"[DataPipeline] WARNING: checkpoint save failed — {exc}")
        return master

    def _save_series_file(self, series: dict, path: str) -> None:
        """Write the mid_close DataFrame to csv, sorted ascending.

        Crash-safe: writes to ``<path>.tmp`` first, then atomically replaces
        the target via ``os.replace``.  Skips persistence entirely when the
        series is empty (e.g. first run before any prices fetched).
        """
        if not series:
            return
        df = series.get("mid_close")
        if df is None or df.empty:
            return
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        tmp_path = path + ".tmp"
        try:
            sorted_df = df.sort_index(ascending=True)
            if sorted_df.index.tz is None:
                sorted_df.index = pd.to_datetime(sorted_df.index, utc=True)
            sorted_df.to_csv(tmp_path, index=True)
            os.replace(tmp_path, path)
        except Exception:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            raise

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
        """Scan folder for csv files and merge each into master."""
        if not os.path.isdir(folder_path):
            return master
        for filename in sorted(os.listdir(folder_path)):
            if not filename.endswith(".csv"):
                continue
            filepath = os.path.join(folder_path, filename)
            print(f"[DataPipeline] Ingesting historic file: {filename}")
            loaded = self._load_historic_series(filepath)
            if loaded is None:
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
