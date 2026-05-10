# DataSource Scoper — Technical Requirements

**Skill:** `DataSource_Scoper`
**Prepared by:** FUNCTIONALIST
**Scope:** Read-only inspection utility. Not part of the trading pipeline.
**Purpose:** Produce a Scope Report describing (1) the investible universe, (2) the stored price series, and (3) the discrepancies between them.

---

## 1. Purpose and Boundaries

The DataSource Scoper reads two data sources — `universe.json` and `universe_series.xlsx` — and emits a structured Scope Report for human or agent consumption. It performs no writes, no broker calls, and no pipeline operations. It is a standalone diagnostic tool.

**What it does:**
- Reads and summarises the instrument universe (`universe.json`)
- Reads and summarises all candidates from the inspection registry (`universe_candidates.json`)
- Reads and summarises the stored price series (`universe_series.xlsx`)
- Computes the union and identifies mismatches between them

---

## 2. Two-Tier Validation Model

The Tradinator universe is managed through two distinct validation tiers:

### Tier 1 — Broker Recognition (run by `discover_universe.py` / `--discover`)

Tests whether the broker's API recognises the epic and whether dealing is enabled.

| T1 status | Meaning |
|---|---|
| `PASS` | Epic recognised by IG API, `dealingEnabled=true` |
| `EPIC_NOT_RECOGNIZED` | Broker explicitly unknown (404 / empty response) |
| `DEALING_DISABLED` | Epic found but `dealingEnabled=false` in snapshot |
| `API_ERROR` | Transient fault (network, auth, rate-limit) — epic not blacklisted |
| `UNTESTED` | Candidate has never had T1 run against it |

Instruments that pass T1 are written to `universe.json` (with `valid: true`) and marked `t2_status: "PENDING_T2"` in `universe_candidates.json`.

### Tier 2 — Data Availability (run by `DataPipeline` / main pipeline)

Tests whether live price bars can be retrieved from the broker's API. Performed as part of every pipeline run.

| T2 status | Meaning |
|---|---|
| `YES` | Pipeline fetched ≥1 valid bar in the last run |
| `NO` | Cold-start fetch returned zero usable bars — instrument removed from `universe.json` |
| `PENDING_T2` | T1 passed but pipeline has not yet run for this instrument |
| `NEVER_TRIED` | T1 failed or was never run; T2 not applicable |

**Gaps do not disqualify.** Incremental fetches returning zero bars (e.g., on weekends / public holidays) are treated as gaps and do not trigger T2 demotion. Only a **cold-start** fetch (no stored series) returning zero bars triggers `T2=NO` and removal from `universe.json`.

An instrument has `valid: true` in `universe_candidates.json` if and only if `t1_status=PASS` **and** `t2_status=YES` (confirmed by pipeline).

---

## 3. Files in Scope

### 3.1 Permitted reads (allowlist)

The scoper MAY read only these files. Everything not on this list must not be read, called, or modified.

| File | Role |
|---|---|
| `data/input/universe.json` | Machine-read universe — T1-pass instruments (pipeline gate) |
| `data/input/universe_candidates.json` | Human-readable registry — all 30+ candidates with T1/T2 metadata |
| `data/input/universe_series.xlsx` | Stored price series — three sheets of price history |
| `data/input/historic_series/*.xlsx` | Historic ingest folder — scanned for file names only |

### 3.2 Path constants

| Constant | Value |
|---|---|
| `UNIVERSE_PATH` | `"data/input/universe.json"` |
| `CANDIDATES_PATH` | `"data/input/universe_candidates.json"` |
| `SERIES_FILE` | `"data/input/universe_series.xlsx"` |
| `HISTORIC_DIR` | `"data/input/historic_series"` |
| `SHEET_NAMES` | `("mid_close", "bid_close", "mid_open")` |

---

## 4. Input / Output Specifications

### 4.1 Inputs

#### `data/input/universe.json`

```json
{
  "description": "str — auto-updated by discover_universe.py",
  "instruments": [
    {
      "epic":        "str",
      "name":        "str",
      "asset_class": "str",
      "region":      "str",
      "valid":       true
    }
  ]
}
```

Contains only T1-PASS instruments (`valid: true`). Previously contained a `status` field (`"verified"` / `"candidate"`) — this was replaced by `valid: bool` in May 2026.

#### `data/input/universe_candidates.json`

```json
{
  "description": "str",
  "last_discover_run": "ISO-8601 UTC | null",
  "candidates": [
    {
      "epic":         "str",
      "name":         "str",
      "asset_class":  "str",
      "region":       "str",
      "t1_status":    "PASS | EPIC_NOT_RECOGNIZED | DEALING_DISABLED | API_ERROR | UNTESTED",
      "t1_reason":    "str | null",
      "t2_status":    "YES | NO | PENDING_T2 | NEVER_TRIED",
      "t2_reason":    "str | null",
      "valid":        "bool",
      "last_validated": "ISO-8601 UTC | null"
    }
  ]
}
```

Contains all candidates (pass and fail). `valid: true` means T1=PASS AND T2=YES confirmed by pipeline.

#### `data/input/universe_series.xlsx`

Multi-sheet Excel file. All three sheets share the same layout:

| Element | Detail |
|---|---|
| **Sheets** | `mid_close`, `bid_close`, `mid_open` |
| **Column A** | Datetime index (row 1 cell is `None` / blank) |
| **Columns B onwards** | One column per stored epic, header = IG epic string |
| **Cell values** | `float` price, or `None` for missing/unfetched bars |
| **Index type** | Stored as naive datetime in file; loaded as UTC-aware `pandas.Timestamp` via `pd.to_datetime(..., utc=True)` |

#### `data/input/historic_series/*.xlsx`

Same schema as `universe_series.xlsx`. The folder may be empty. The scoper reports file count and names only.

---

### 4.2 Output — Scope Report

The Scope Report is a structured data object (or equivalent formatted text) with four sections. It is emitted to stdout and/or returned as a dict for agent consumption. It must not be written to any file in `data/` or `secrets/`.

**Section A — Universe Scope (from universe.json)**

| Field | Type | Description |
|---|---|---|
| `total_valid` | int | Count of T1-pass instruments in `universe.json` |
| `valid_epics` | list[str] | All epics in `universe.json` |
| `malformed_entries` | int | Instruments excluded due to missing or empty `epic` field |

**Section B — Candidates Scope (from universe_candidates.json)**

| Field | Type | Description |
|---|---|---|
| `total_candidates` | int | Total candidates (all tiers) |
| `t1_pass_count` | int | Count with `t1_status == "PASS"` |
| `t1_fail_count` | int | Count with T1 failures (any non-PASS non-UNTESTED) |
| `t1_untested_count` | int | Count with `t1_status == "UNTESTED"` |
| `pending_t2_count` | int | Count with `t2_status == "PENDING_T2"` |
| `t2_yes_count` | int | Count with `t2_status == "YES"` |
| `t2_no_count` | int | Count with `t2_status == "NO"` |
| `fully_valid_count` | int | Count with `valid == true` (T1=PASS AND T2=YES) |
| `epic_not_recognized` | list[str] | Epics where `t1_status == "EPIC_NOT_RECOGNIZED"` |
| `dealing_disabled` | list[str] | Epics where `t1_status == "DEALING_DISABLED"` |
| `api_error` | list[str] | Epics where `t1_status == "API_ERROR"` |
| `last_discover_run` | str \| None | ISO-8601 timestamp of last discover run |

**Section C — Series Scope (from universe_series.xlsx)**

| Field | Type | Description |
|---|---|---|
| `series_epics` | list[str] | All epic column headers found in `universe_series.xlsx` (any sheet) |
| `series_epic_count` | int | Count of unique epics across all sheets |
| `date_range` | dict[str, dict] | Per-sheet: `{"first": datetime, "last": datetime}` |
| `sheets_have_consistent_date_range` | bool | True if all three sheets have identical first/last datetime |
| `sheets_fully_consistent` | bool | True if all three sheets contain exactly the same epic columns |
| `historic_file_count` | int | Number of files found in `data/input/historic_series/` (excluding `.gitkeep`) |
| `historic_files_present` | list[str] | Filenames in `data/input/historic_series/` (excluding `.gitkeep`) |

**Section D — Discrepancy Analysis**

| Field | Type | Description |
|---|---|---|
| `in_universe_not_in_series` | list[str] | Epics in `universe.json` but absent as series columns |
| `in_series_not_in_universe` | list[str] | Epics in series but absent from `universe.json` (orphaned) |
| `pending_not_in_series` | list[str] | PENDING_T2 epics with no series column yet (expected) |
| `valid_not_in_series` | list[str] | Fully valid epics (`valid=true` in candidates) with no series column — highest-priority gaps |
| `same_base_variant_orphans` | list[dict] | Series epics whose 3-segment base matches a universe epic but the exact epic differs |

---

## 5. Scope Definitions

### Scope 1 — Universe Scope

The universe scope answers: *what T1-validated instruments are currently active in the trading pipeline?*

- Source: `data/input/universe.json`
- Contains ONLY instruments where T1 validation passed.
- Does NOT contain candidates that failed T1 or are PENDING_T2.
- The `valid: true` field means "currently in universe (T1 confirmed)".
- For the full validation picture, see `universe_candidates.json`.

### Scope 2 — Candidates Scope

The candidates scope answers: *what is the validation state of all registered candidates, including failures?*

- Source: `data/input/universe_candidates.json`
- Contains ALL registered candidates (pass and fail).
- Key field distinctions: `t1_status` tells you WHY T1 failed (non-recognition vs. dealing disabled vs. transient error). `t2_status` tells you whether data is available.
- `valid: true` means fully confirmed by both tiers.

### Scope 3 — Series Scope

The series scope answers: *what price data is actually stored, and for which instruments and date ranges?*

- Source: `data/input/universe_series.xlsx` and `data/input/historic_series/*.xlsx`
- A column may exist but contain only `None` values (gaps); this is different from the column being absent.

### Scope 4 — Discrepancy Analysis

The discrepancy analysis answers: *where are the gaps between what the system has validated and what data is stored?*

---

## 6. Error Handling Contract

| Condition | Required behaviour |
|---|---|
| `universe.json` missing | Report as error in Section A; continue to other sections |
| `universe_candidates.json` missing | Report as error in Section B; continue to other sections |
| `universe_series.xlsx` missing | Report absence in Section C; Section A and B proceed normally |
| File present but corrupt / unreadable | Catch exception; report error for that file; continue |
| `historic_series/` absent or empty | Report `historic_file_count: 0`; no error |

The scoper must never raise an unhandled exception due to missing or malformed input files.

---

## 7. Dependencies

| Dependency | Source | Usage |
|---|---|---|
| `json` | Python stdlib | Parse `universe.json` and `universe_candidates.json` |
| `pandas` | Already in `requirements.txt` | Read `universe_series.xlsx`; datetime loading via `pd.to_datetime(..., utc=True)` |
| `os` / `pathlib` | Python stdlib | Directory scan of `historic_series/` |

No broker dependencies. No network calls. No credentials required.

---

## 8. Guardrails and Out of Scope

The scoper MAY read only the files listed in Section 3.1. Everything else must not be read, called, or modified.

### 8.1 Assets that must not be touched

| Asset | Type | Reason |
|---|---|---|
| `data/input/universe.json` | Data file | Machine-read universe; written only by `discover_universe.py` / `DataPipeline` |
| `data/input/universe_candidates.json` | Data file | Candidate registry; written only by `discover_universe.py` / `DataPipeline` |
| `data/input/universe_series.xlsx` | Data file | Master price series; written only by `DataPipeline` |
| `data/input/historic_series/` | Directory | Ingest source; contents written externally |
| `data/input/discover_universe.py` | Script | Makes live broker API calls |
| `main.py` | Code file | Entry point and config definition |
| `model/model.py` | Code file | Pipeline orchestration |
| `model/run_loop.py` | Code file | Pipeline orchestration |
| `model/handoff.py` | Code file | Pipeline orchestration |
| `model/__init__.py` | Code file | Package init imports all pipeline component classes and IG adapter code (`trading_ig`, `dotenv`); do not import |
| `model/model_components/data_pipeline.py` | Code file | Pipeline component |
| `model/model_components/broker_connector.py` | Code file | Broker session management |
| `model/model_components/broker_adapter.py` | Code file | Protocol definition |
| `model/model_components/ig_adapter.py` | Code file | Live broker adapter |
| `model/model_components/ibkr_adapter.py` | Code file | Live broker adapter |
| `secrets/` | Directory | Credentials; must never be accessed |
| `data/output/` | Directory | Pipeline outputs; not in scope |

### 8.2 Explicitly out of scope

- Making broker API calls of any kind
- Writing, updating, or deleting any file in `data/` or `secrets/`
- Running the trading pipeline
- Verifying whether a candidate instrument is currently tradeable
- Fetching or ingesting new price bars
- Modifying `universe.json` or `universe_candidates.json`
- Computing trading signals or portfolio metrics
- Importing or invoking `discover_universe.py`, `broker_connector.py`, `ig_adapter.py`, `ibkr_adapter.py`, `Model`, `RunLoop`, or `Handoff`

---

## 9. Scope Report — Structural Sketch

```
=== DataSource Scope Report ===

--- A. Universe Scope (universe.json) ---
{ total_valid, valid_epics, malformed_entries }

--- B. Candidates Scope (universe_candidates.json) ---
{ total_candidates, t1_pass_count, t1_fail_count, t1_untested_count,
  pending_t2_count, t2_yes_count, t2_no_count, fully_valid_count,
  epic_not_recognized, dealing_disabled, api_error, last_discover_run }

--- C. Series Scope (universe_series.xlsx) ---
{ series_epics, series_epic_count, date_range, sheets_have_consistent_date_range,
  sheets_fully_consistent, historic_file_count, historic_files_present }

--- D. Discrepancy Analysis ---
{ in_universe_not_in_series, in_series_not_in_universe,
  pending_not_in_series, valid_not_in_series, same_base_variant_orphans }
```

*(The exact formatting is implementation detail — BUILDER territory.)*
