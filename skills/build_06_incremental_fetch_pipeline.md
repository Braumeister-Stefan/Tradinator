# BUILD PROMPT 06 — Incremental Fetch + T2 Status Update in DataPipeline

**Prepared by:** FUNCTIONALIST  
**Scope:** Modify `DataPipeline.run()` in `model/model_components/data_pipeline.py` to:
1. Fetch only new bars (incremental) instead of always fetching the last `lookback` bars.
2. Update `universe_candidates.json` with the T2 status for each instrument after fetching.
3. Update `universe.json` to remove instruments whose most recent fetch returned zero bars.

**Must run AFTER:** Build 01 (universe_candidates.json exists), Build 02 (universe.json uses valid flag), Build 05 (date-range fetch method in adapter)

---

## Context

The repository is at `/home/runner/work/Tradinator/Tradinator`.

**Current behaviour:**
- Always fetches the last `lookback` (default: 5) bars, regardless of what is already stored.
- No inspection of `universe_series.xlsx` before fetching.
- No update to `universe_candidates.json` after fetching.
- If an instrument returns zero bars, it is silently skipped for that run — it remains in `universe.json`.

**New behaviour:**
- Before fetching, check `universe_series.xlsx` for the last stored timestamp for each instrument.
- If bars are stored (last_date known): use `fetch_historical_prices_by_date_range(from_date=last_date)` to fetch only new bars.
- If no bars stored (cold start): use `fetch_historical_prices(lookback)` as today (fixed count).
- After each fetch:
  - If bars returned: update `universe_candidates.json` → `t2_status: "YES"`, `valid: true`.
  - If zero bars: update `universe_candidates.json` → `t2_status: "NO"`, `valid: false`, and remove from `universe.json`.

---

## Detailed Implementation

### New private method: `_get_last_stored_date()`

Add to `DataPipeline`:

```python
def _get_last_stored_date(self, instrument_id: str, master: dict | None) -> str | None:
    """Return the last stored UTC timestamp string for an instrument, or None if no data."""
    if master is None:
        return None
    ref_sheet = master.get(self.SHEET_NAMES[0])  # use mid_close as reference
    if ref_sheet is None or ref_sheet.empty:
        return None
    if instrument_id not in ref_sheet.columns:
        return None
    series = ref_sheet[instrument_id].dropna()
    if series.empty:
        return None
    last_ts = series.index.max()
    # Return as ISO string for the adapter; add 1 second to exclude the last bar itself
    import pandas as pd
    next_ts = last_ts + pd.Timedelta(seconds=1)
    return next_ts.strftime("%Y-%m-%dT%H:%M:%S")
```

### Updated `run()` method

Modify `DataPipeline.run()` to implement incremental fetching. The key changes:

**Step 1:** Load the existing master series file BEFORE the fetch loop (not after):
```python
master = self._load_series_file(self.SERIES_FILE)
```

**Step 2:** In the fetch loop, check for last stored date per instrument:
```python
for i, instrument_id in enumerate(instruments):
    if i > 0:
        time.sleep(self.RATE_LIMIT_DELAY)

    last_date = self._get_last_stored_date(instrument_id, master)

    if last_date is not None:
        print(f"[DataPipeline] Incremental fetch {instrument_id} from {last_date}…")
        try:
            bars = adapter.fetch_historical_prices_by_date_range(
                instrument_id, resolution, last_date
            )
        except Exception as exc:
            print(f"[DataPipeline] WARNING: incremental fetch failed for {instrument_id} — {exc}. "
                  "Falling back to fixed-count fetch.")
            try:
                bars = adapter.fetch_historical_prices(instrument_id, resolution, lookback)
            except Exception as exc2:
                print(f"[DataPipeline] WARNING: skipping {instrument_id} — {exc2}")
                self._update_t2_status(instrument_id, "NO", f"fetch exception: {exc2}")
                continue
    else:
        print(f"[DataPipeline] Cold-start fetch {instrument_id} ({resolution}, {lookback} bars)…")
        try:
            bars = adapter.fetch_historical_prices(instrument_id, resolution, lookback)
        except Exception as exc:
            print(f"[DataPipeline] WARNING: skipping {instrument_id} — {exc}")
            self._update_t2_status(instrument_id, "NO", f"fetch exception: {exc}")
            continue

    parsed = self._bars_to_columns(bars)
    if parsed is None:
        print(f"[DataPipeline] WARNING: no usable data for {instrument_id}, skipping.")
        self._update_t2_status(instrument_id, "NO", "fetch returned no usable bars")
        self._remove_from_universe(instrument_id)
        continue

    prices[instrument_id] = parsed
    self._update_t2_status(instrument_id, "YES", f"{len(bars)} bar(s) fetched")
```

**Step 3:** In the persistence block, pass the already-loaded `master` instead of re-loading:
```python
try:
    live_frames = self._build_dataframes(prices)
    if master is None:
        master = {name: pd.DataFrame() for name in self.SHEET_NAMES}
    master = self._ingest_historic_files(master, self.HISTORIC_DIR)
    master = self._merge_series(live_frames, master)
    ...
```

### New private method: `_update_t2_status()`

Add to `DataPipeline`:

```python
CANDIDATES_PATH = "data/input/universe_candidates.json"

def _update_t2_status(self, instrument_id: str, t2_status: str, t2_reason: str) -> None:
    """Update T2 status for an instrument in universe_candidates.json."""
    import json
    import time as _time

    if not os.path.isfile(self.CANDIDATES_PATH):
        return  # candidates file doesn't exist yet; skip silently

    try:
        with open(self.CANDIDATES_PATH) as f:
            data = json.load(f)

        now_utc = _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime())
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
        print(f"[DataPipeline] WARNING: could not update T2 status for {instrument_id} — {exc}")
```

### New private method: `_remove_from_universe()`

Add to `DataPipeline`:

```python
UNIVERSE_PATH = "data/input/universe.json"

def _remove_from_universe(self, instrument_id: str) -> None:
    """Remove an instrument from universe.json when T2 fails (zero bars)."""
    import json

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
            print(f"[DataPipeline] Removed {instrument_id} from universe.json (T2=NO, zero bars).")
            with open(self.UNIVERSE_PATH, "w") as f:
                json.dump(data, f, indent=2)
                f.write("\n")
    except Exception as exc:
        print(f"[DataPipeline] WARNING: could not remove {instrument_id} from universe.json — {exc}")
```

---

## T2 Status Logic Summary

| Fetch outcome | T2 status | valid | universe.json |
|---|---|---|---|
| ≥1 bar returned | YES | true (if T1=PASS) | unchanged |
| 0 bars returned | NO | false | instrument removed |
| Exception during fetch | NO | false | instrument removed |

Note: Gaps in stored bars do NOT disqualify an instrument. Only the MOST RECENT fetch returning zero bars causes removal. If next run returns bars again, `discover_universe.py` must be run to re-add the instrument to `universe.json`.

---

## Acceptance Criteria

1. For instruments with existing stored data, `DataPipeline` calls `fetch_historical_prices_by_date_range()` instead of `fetch_historical_prices()`.
2. For instruments with no stored data, `DataPipeline` calls `fetch_historical_prices()` (cold start).
3. After a successful fetch (≥1 bar), `universe_candidates.json` reflects `t2_status: "YES"`.
4. After a failed fetch (0 bars), `universe_candidates.json` reflects `t2_status: "NO"` and the instrument is removed from `universe.json`.
5. All existing DataPipeline tests pass (run `python -m pytest tests/ -q` if tests exist).
6. If `universe_candidates.json` does not exist, `_update_t2_status` silently skips (no crash).
7. If `universe.json` does not exist, `_remove_from_universe` silently skips (no crash).

---

## Files to Touch

| File | Action |
|---|---|
| `model/model_components/data_pipeline.py` | MODIFY — incremental fetch + T2/universe update |
