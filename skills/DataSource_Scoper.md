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
- Reads and summarises the stored price series (`universe_series.xlsx`)
- Computes the union and identifies mismatches between them

**What it does not do:**
- Connect to any broker
- Modify any file
- Ingest historic series
- Trigger or call any pipeline component

---

## 2. Files in Scope

### 2.1 Read targets (data files)

| File | Role |
|---|---|
| `data/input/universe.json` | Instrument registry — source of truth for the investible universe |
| `data/input/universe_series.xlsx` | Stored price series — three sheets of price history |
| `data/input/historic_series/*.xlsx` | Historic ingest folder — scanned for presence only |

### 2.2 Reference files (read for constants and logic — do NOT modify)

| File | Relevant elements |
|---|---|
| `main.py` | `_load_universe()` deduplication logic, `UNIVERSE_PATH` constant, `config["resolution"]`, `config["lookback"]` |
| `model/model_components/data_pipeline.py` | `DataPipeline` class constants (see Section 4) |

### 2.3 Files outside scope (must NOT be read, called, or modified)

| File | Reason |
|---|---|
| `data/input/discover_universe.py` | Makes live broker API calls — excluded by design |
| `model/model_components/broker_connector.py` | Initiates broker session — excluded by design |
| `model/model_components/broker_adapter.py` | Protocol interface — not needed for inspection |
| `model/model_components/ig_adapter.py` | Live broker adapter — excluded by design |
| `data/output/` | Pipeline outputs — not in scope |
| `secrets/.env` | Credentials — must never be read or referenced |

---

## 3. Input / Output Specifications

### 3.1 Inputs

#### `data/input/universe.json`

```
{
  "description": str,
  "instruments": [
    {
      "epic":        str,            // IG epic, e.g. "IX.D.FTSE.DAILY.IP"
      "name":        str,            // human-readable instrument name
      "asset_class": str,            // "index" | "forex" | "commodity" | ...
      "region":      str,            // "UK" | "US" | "EU" | "global" | ...
      "status":      "verified" | "candidate",
      "note":        str (optional)  // warning notes, e.g. alternate epics
    },
    ...
  ]
}
```

Current state: 30 instruments — 5 `verified`, 25 `candidate`.

#### `data/input/universe_series.xlsx`

Multi-sheet Excel file. All three sheets share the same layout:

| Element | Detail |
|---|---|
| **Sheets** | `mid_close`, `bid_close`, `mid_open` |
| **Column A** | Datetime index (row 1 cell is `None` / blank) |
| **Columns B…N** | One column per instrument, header = IG epic string |
| **Cell values** | `float` price, or `None` for missing/unfetched bars |
| **Index type** | `datetime` (Python `datetime.datetime` objects) |

Current state: 13 epics across columns. Epics in the file do not fully align with `universe.json` (see Section 5).

#### `data/input/historic_series/*.xlsx`

Same schema as `universe_series.xlsx`. The folder is currently empty (contains only `.gitkeep`). The scoper must report presence/absence of files but need not parse them for the core report. If files are present, their column headers (epic names) must be extracted to identify what additional series exist.

---

### 3.2 Output — Scope Report

The Scope Report is a structured data object (or equivalent formatted text) with three sections. It is emitted to stdout and/or returned as a dict for agent consumption. It must not be written to any file in `data/` or `secrets/`.

**Section A — Universe Scope**

| Field | Type | Description |
|---|---|---|
| `total_instruments` | int | Total instruments in `universe.json` |
| `verified_count` | int | Count with `status == "verified"` |
| `candidate_count` | int | Count with `status == "candidate"` |
| `verified_epics` | list[str] | Epics with `status == "verified"` |
| `candidate_epics` | list[str] | Epics with `status == "candidate"` |
| `deduplicated_epics` | list[str] | Output of `_load_universe()` deduplication — the list that the pipeline actually uses |
| `deduplication_collisions` | list[tuple[str, str]] | Pairs of (dropped epic, retained epic) where two epics share the same 3-segment base |
| `instruments_with_notes` | list[dict] | Instruments carrying a `note` field, with epic + note text |

**Section B — Series Scope**

| Field | Type | Description |
|---|---|---|
| `series_epics` | list[str] | All epic column headers found in `universe_series.xlsx` (any sheet) |
| `series_epic_count` | int | Count of unique epics across all sheets |
| `date_range` | dict[str, dict] | Per-sheet: `{"first": datetime, "last": datetime, "row_count": int}` for each of `mid_close`, `bid_close`, `mid_open` |
| `coverage_per_epic` | dict[str, dict] | Per epic: `{"non_null_count": int, "null_count": int}` per sheet — summarises data density |
| `historic_files_present` | list[str] | Filenames found in `data/input/historic_series/` (excluding `.gitkeep`) |
| `historic_epics` | list[str] | Union of epic column headers from all historic ingest files (empty list if none present) |

**Section C — Discrepancy Analysis**

| Field | Type | Description |
|---|---|---|
| `in_universe_not_in_series` | list[str] | Epics present in `deduplicated_epics` but absent as columns in `universe_series.xlsx` |
| `in_series_not_in_universe` | list[str] | Epics present as columns in `universe_series.xlsx` but absent from `universe.json` entirely (orphaned series) |
| `verified_not_in_series` | list[str] | Subset of `verified_epics` that have no column in the series file — highest-priority gaps |
| `candidates_never_fetched` | list[str] | Candidate epics absent from both series file and historic ingest — have never been fetched |
| `cross_sheet_column_mismatch` | list[str] | Epics present in some sheets of `universe_series.xlsx` but not all three |

---

## 4. Relevant Constants

These constants define the configuration context for interpreting the data sources. They are read for context only — the scoper must not alter them.

### From `model/model_components/data_pipeline.py` — `DataPipeline` class

| Constant | Value | Relevance |
|---|---|---|
| `SERIES_FILE` | `"data/input/universe_series.xlsx"` | Canonical path for the master series file |
| `HISTORIC_DIR` | `"data/input/historic_series"` | Canonical path for the historic ingest folder |
| `SHEET_NAMES` | `("mid_close", "bid_close", "mid_open")` | Authoritative list of expected sheet names |
| `DEFAULT_RESOLUTION` | `"DAY"` | Resolution the pipeline fetches at (informational context for report) |
| `DEFAULT_LOOKBACK` | `50` | Default bar count if config does not override (informational context) |

### From `main.py`

| Constant / config key | Value | Relevance |
|---|---|---|
| `UNIVERSE_PATH` | `"data/input/universe.json"` | Canonical path for the universe file |
| `config["resolution"]` | `"DAY"` | Active resolution for the running config |
| `config["lookback"]` | `5` | Active lookback for the running config |

### Deduplication logic in `main.py::_load_universe()`

The pipeline deduplicates epics by comparing the first three dot-segments (e.g. `IX.D.FTSE` from `IX.D.FTSE.DAILY.IP`). Only the first encountered variant per base is retained. The scoper must replicate this logic when computing `deduplicated_epics` and `deduplication_collisions` — it must not call `_load_universe()` directly (to avoid importing `main.py`), but must implement the same base-extraction rule inline.

---

## 5. Three Scopes — Detailed Definitions

### Scope 1 — Universe Scope

The universe scope answers: *what instruments does the system know about, and which are cleared for trading?*

- Source: `data/input/universe.json`
- A `verified` instrument has been tested against the IG Demo API by `discover_universe.py` and confirmed accessible.
- A `candidate` instrument is registered but unverified — it may or may not be tradeable.
- The deduplicated list is the effective input to the pipeline (`broker_state["instruments"]`); only this list matters operationally.

### Scope 2 — Series Scope

The series scope answers: *what price data is actually stored, and for which instruments and date ranges?*

- Source: `data/input/universe_series.xlsx` (primary) and `data/input/historic_series/*.xlsx` (supplementary)
- Each column in the xlsx file represents one instrument's price history for one price type.
- Columns in the series file are IG epic strings — the same identifier space as `universe.json`.
- A column may exist but contain only `None` values; this is meaningfully different from the column being absent.
- The `historic_series/` folder supplements the master file via `DataPipeline._ingest_historic_files()`. The scoper must check whether any files are present and, if so, which epics they contain.

### Scope 3 — Discrepancy Analysis

The discrepancy analysis answers: *where are the gaps and anomalies between what the system knows and what it has stored?*

Priority classification for gaps:

| Priority | Condition | Meaning |
|---|---|---|
| **P1** | Verified epic absent from series file | Pipeline will attempt to fetch live but has no historical buffer |
| **P2** | Series column absent for any deduplicated epic | Pipeline fetches live but cannot extend from a stored baseline |
| **P3** | Series column present for an epic not in universe.json | Orphaned data — instrument removed from universe, data not cleaned up |
| **P4** | Candidate epic absent from both series and historic files | Never been fetched; no data at all |

---

## 6. Dependencies

| Dependency | Source | Usage |
|---|---|---|
| `json` | Python stdlib | Parse `universe.json` |
| `openpyxl` or `pandas` | Already in `requirements.txt` | Read `universe_series.xlsx` and historic files |
| `os` / `pathlib` | Python stdlib | Directory scan of `historic_series/` |
| `datetime` | Python stdlib | Date range computation |

No broker dependencies. No network calls. No credentials required.

---

## 7. Guardrails — Files and Functions That Must Not Be Touched

Any future agent or task operating under this skill's scope is prohibited from modifying the following:

| Asset | Type | Reason |
|---|---|---|
| `data/input/universe.json` | Data file | Registry of record; modifications require `discover_universe.py` |
| `data/input/universe_series.xlsx` | Data file | Master price series; written only by `DataPipeline` |
| `data/input/historic_series/` | Directory | Ingest source; contents written externally |
| `main.py` | Code file | Entry point and config definition |
| `model/model_components/data_pipeline.py` | Code file | Pipeline component |
| `model/model_components/broker_connector.py` | Code file | Broker session management |
| `model/model_components/broker_adapter.py` | Code file | Protocol definition |
| `model/model_components/ig_adapter.py` | Code file | Live broker adapter |
| `data/input/discover_universe.py` | Script | Broker validation script |
| `secrets/` | Directory | Credentials; must never be accessed |
| `data/output/` | Directory | Pipeline outputs; not in scope |

---

## 8. Out of Scope

The following are explicitly outside the scope of any task operating under this skill:

- Making broker API calls of any kind
- Writing, updating, or deleting any file in `data/` or `secrets/`
- Running the trading pipeline
- Verifying whether a candidate instrument is currently tradeable
- Fetching or ingesting new price bars
- Modifying `universe.json` to add, remove, or promote instruments
- Computing trading signals or portfolio metrics
- Importing or invoking `discover_universe.py`, `broker_connector.py`, `ig_adapter.py`, or `Model`

---

## 9. Scope Report — Example Structure

```
=== DataSource Scope Report ===
Generated: 2026-06-01T12:00:00

--- A. Universe Scope ---
Total instruments:     30
  Verified:            5
  Candidates:          25
Deduplicated (active): 29  (1 deduplication collision)
  Collision: IX.D.DAX.IFD.IP dropped in favour of IX.D.DAX.DAILY.IP

Verified epics:
  IX.D.FTSE.DAILY.IP, IX.D.SPTRD.DAILY.IP, CS.D.EURUSD.MINI.IP,
  CS.D.GBPUSD.MINI.IP, CC.D.CL.UMP.IP

Instruments with notes: 3
  CC.D.GC.UMP.IP — "WARN: verify on Demo; spot alt CS.D.GOLD.MINI.IP"
  ...

--- B. Series Scope ---
Epics in universe_series.xlsx:  13
Sheets present: mid_close, bid_close, mid_open

Date ranges:
  mid_close:  2026-02-18 → 2026-06-01  (450 rows)
  bid_close:  2026-02-18 → 2026-06-01  (450 rows)
  mid_open:   2026-02-18 → 2026-06-01  (450 rows)

Coverage (non-null / total rows per epic per sheet):
  CC.D.CL.UMP.IP   — mid_close: 200/450, bid_close: 200/450, mid_open: 200/450
  ...

Historic ingest files: none

--- C. Discrepancy Analysis ---
[P1] Verified epics with NO series data: 0

[P2] Active (deduplicated) epics with no series column: 16
  IX.D.DAX.DAILY.IP, IX.D.DOW.DAILY.IP, CS.D.USDJPY.MINI.IP, ...

[P3] Series orphans (in file, not in universe.json): 0

[P4] Candidate epics never fetched: 22
  IX.D.DAX.DAILY.IP, ...

Cross-sheet column mismatches: 0
```

*(The exact formatting is implementation detail — BUILDER territory. The above illustrates the required information, not the required format.)*
