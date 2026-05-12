# Conversation Index: Incremental Fetch Investigation & Fix

**Purpose:** Structured index of the debugging session that identified and resolved the IG API `JSONDecodeError` caused by incorrect date string formatting in the incremental fetch path of `ig_adapter.py`.

**Session date:** 2026-05-13  
**Files touched:** `model/model_components/data_pipeline.py`, `model/model_components/ig_adapter.py`

---

## Master Turn Table

| Turn | Speaker | Intent | Method/Agent Used | Subject | Key Inputs | Key Outputs | Files Touched | Findings / Decisions |
|------|---------|--------|-------------------|---------|------------|-------------|---------------|----------------------|
| 1 | User | Understand incremental fetch logic | Read + explain | `_get_last_stored_date` and incremental fetch branch in `data_pipeline.py` | Source code of `data_pipeline.py` | Explanation of cold-start vs incremental path, fallback chain, zero-bar handling, T2 status updates | `model/model_components/data_pipeline.py` | Branch decision: `last_date is not None` → incremental; else → cold start. Zero bars ≠ T2 failure. |
| 2 | User | Validate hidden assumptions and risk | VALIDATOR subagent on full `data_pipeline.py` | Incremental fetch logic | Full source of `data_pipeline.py` | 10 findings with severity ratings; priority order: #8, #3, #4, #6 (High); #1a, #2, #7, #10a, #10b (Medium) | `model/model_components/data_pipeline.py` | See Findings Registry below. |
| 3 | User | Map live failure to validator finding | Log analysis | `JSONDecodeError: Expecting value: line 1 column 1 (char 0)` on every instrument | Live log output showing all instruments failing incremental fetch with empty API response body | Attribution to finding #1a (timestamp format rejected by broker); from-date `2026-05-12T00:00:01` lacks `Z` / timezone | None | Decision: format of from-date string is the likely cause; investigate adapter date normalisation. |
| 4 | User | Build and deploy fix | FUNCTIONALIST → BUILDER → OPTIMIZER → VALIDATOR | Date string normalisation in `ig_adapter.py` | `ig_adapter.py` source; `trading_ig` library v2 URL path format | True root cause identified (slashes fragmented URL path); fix applied; VALIDATOR returned PASS | `model/model_components/ig_adapter.py` | See Changes Log and Root Cause Chain below. |

---

## Findings Registry (Turn 2 VALIDATOR Output)

| # | Assumption | Severity | Description |
|---|-----------|---------|-------------|
| 1a | `strftime` no Z suffix → broker treats as UTC | Medium | `_get_last_stored_date` produces ISO-8601 without `Z` or UTC offset; broker may reject or misinterpret as local time. |
| 1b | xlsx round-trip lossless for all timestamp formats | Minor | Timestamps written to and read from `.xlsx` may lose sub-second precision or timezone info depending on pandas/openpyxl handling. |
| 2 | Broker never returns duplicate timestamps | Medium | No deduplication step before appending new bars; duplicate timestamps would silently corrupt the stored series. |
| 3 | `_load_series_file` always succeeds | High | `run()` calls `_load_series_file` with no surrounding `try/except`; a corrupt or missing file raises an uncaught exception that aborts the pipeline. |
| 4 | Zero bars = weekend/holiday only | High | A zero-bar response is treated as a non-trading day and handled via `_reconstruct_from_master`; quota exhaustion or a silent API error would produce the same response and go undetected. |
| 5 | T2 reason field accurate | Minor | T2 status reasons are set by string literals in the code; if branching logic changes, the reason strings may become stale without a test catching it. |
| 6 | Both fetch failures = durable problem, not transient | High | If both the date-range fetch and the fixed-count fallback fail, the instrument is removed from the active list; a transient network or API error would permanently drop it. |
| 7 | `bars == []` exclusively means non-trading day | Medium | An empty list is also returned on certain API errors (rate limiting, bad parameters); the code does not distinguish between these cases. |
| 8 | `pd.ExcelWriter` write is crash-safe | High | Data is written to `.xlsx` inside a `with pd.ExcelWriter(...)` block; a crash mid-write produces a corrupt file with no recovery path. |
| 9 | `high/low/volume = None` from reconstruction handled downstream | Low | `_reconstruct_from_master` returns `None` for `high`, `low`, and `volume` fields; downstream consumers must tolerate `None` without explicit contract. |
| 10a | 1-second delay governs all request rate | Medium | A fixed 1-second sleep is used between requests; this may be insufficient for burst scenarios or if the broker enforces a stricter per-minute cap. |
| 10b | Inter-instrument delay covers incremental→fallback pair | Medium | When incremental fetch fails and falls back to fixed-count, two requests are made for the same instrument; the single inter-instrument delay may not provide adequate spacing for the pair. |

**Priority order:** #8, #3, #4, #6 (High) → #1a, #2, #7, #10a, #10b (Medium) → #1b, #5, #9 (Low/Minor)

---

## Changes Log (Turn 4)

All changes applied to `model/model_components/ig_adapter.py`.

| # | Agent | Change Type | Location | Before | After | Rationale |
|---|-------|-------------|----------|--------|-------|-----------|
| 1 | BUILDER | Bug fix | `fetch_historical_prices_by_date_range` — `normalised` assignment | `base.replace("T", " ").replace("-", "/") + ":000"` | `base.replace("T", " ")` | Removed slash substitution and `:000` suffix; slashes in the date string fragmented the v2 URL path into extra path segments, causing the IG server to return an empty body. |
| 2 | BUILDER | Bug fix | `fetch_historical_prices_by_date_range` — `end_str` assignment | `time.strftime("%Y/%m/%d %H:%M:%S", time.gmtime()) + ":000"` | `time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())` | Same fix applied to the end-date string: removed slashes and `:000` suffix to match v2 required format `"YYYY-MM-DD HH:MM:SS"`. |
| 3 | OPTIMIZER | Comment update | `fetch_historical_prices_by_date_range` — inline comment | Referenced v1 date format with slashes | Updated to reference v2 format `"YYYY-MM-DD HH:MM:SS"` | Stale comment referenced the old format; updated to reflect the corrected format. |
| 4 | OPTIMIZER | Dead code removal | `fetch_historical_prices_by_date_range` — `close_price` extraction | Redundant `bar.get("closePrice")` call with intermediate `bid_close_val` variable | Consolidated to single `bar.get("closePrice")` assignment | Removed duplicate `bar.get("closePrice")` call and unused intermediate variable. |

---

## Root Cause Chain

```
Live symptom
└── JSONDecodeError: Expecting value: line 1 column 1 (char 0)
    └── IG API returned empty response body for ALL instruments
        └── HTTP request reached a malformed URL endpoint
            └── Date strings contained forward slashes
                └── v2 URL path format embeds dates verbatim:
                    /prices/{epic}/{resolution}/{startDate}/{endDate}
                    Slashes in date fragmented the path into extra segments
                        └── Root cause: ig_adapter.py applied v1 date
                            normalisation (.replace("-", "/") + ":000")
                            to a library that uses v2 URL path embedding
                                └── Fix: remove slash substitution and
                                    :000 suffix; use "YYYY-MM-DD HH:MM:SS"
```

### Intermediate investigation steps

| Step | Finding | Outcome |
|------|---------|---------|
| Initial attribution | Finding #1a — missing `Z` suffix | Hypothesis: broker rejects timestamp without UTC marker |
| FUNCTIONALIST review | Adapter already strips `Z` via `rstrip("Z")` | #1a fix would be a no-op on the wire; deeper investigation required |
| Read `ig_adapter.py` | `normalised` produces `"2026/05/12 00:00:01:000"` with slashes | Slashes identified as URL path separator conflict |
| Read `trading_ig` library | v2 endpoint embeds dates in URL path via `.format()` | Confirmed: slashes fragment the path; library docstring specifies `"YYYY-MM-DD HH:MM:SS"` |
| True root cause confirmed | v1 format applied to v2 library | Fix scoped to two assignments in `ig_adapter.py` |
| Post-fix VALIDATOR | PASS — two minor pre-existing findings | No `Z`/timezone guard on `from_date` (low risk, pre-existing); `DATE_FORMATS[2]` in library shows slashes (misleading for maintainers, no code change needed) |
