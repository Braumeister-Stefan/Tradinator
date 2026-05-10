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

---

## 2. Files in Scope

### 2.1 Permitted reads (allowlist)

The scoper MAY read only these files. Everything not on this list must not be read, called, or modified.

| File | Role |
|---|---|
| `data/input/universe.json` | Instrument registry — source of truth for the investible universe |
| `data/input/universe_series.xlsx` | Stored price series — three sheets of price history |
| `data/input/historic_series/*.xlsx` | Historic ingest folder — scanned for file names only |

### 2.2 Path constants

These literal paths are defined directly in the skill:

| Constant | Value |
|---|---|
| `UNIVERSE_PATH` | `"data/input/universe.json"` |
| `SERIES_FILE` | `"data/input/universe_series.xlsx"` |
| `HISTORIC_DIR` | `"data/input/historic_series"` |
| `SHEET_NAMES` | `("mid_close", "bid_close", "mid_open")` |

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

Instruments with a missing or empty `epic` field are excluded from all counts and reported as `malformed_entries`. Instruments where `status` is any value other than `"verified"` or `"candidate"` are reported under `unknown_status`.

#### `data/input/universe_series.xlsx`

Multi-sheet Excel file. All three sheets share the same layout:

| Element | Detail |
|---|---|
| **Sheets** | `mid_close`, `bid_close`, `mid_open` |
| **Column A** | Datetime index (row 1 cell is `None` / blank) |
| **Columns B onwards** | One column per stored epic, header = IG epic string |
| **Cell values** | `float` price, or `None` for missing/unfetched bars |
| **Index type** | Stored as naive datetime in file; loaded as UTC-aware `pandas.Timestamp` via `pd.to_datetime(..., utc=True)` |

Current state: 13 epics across columns. Epics in the file do not fully align with `universe.json` (see Section 4).

#### `data/input/historic_series/*.xlsx`

Same schema as `universe_series.xlsx`. The folder is currently empty (contains only `.gitkeep`). The scoper reports file count and names only. If files are present, their filenames are listed in `historic_files_present`; no parsing of their contents is required.

---

### 3.2 Output — Scope Report

The Scope Report is a structured data object (or equivalent formatted text) with three sections. It is emitted to stdout and/or returned as a dict for agent consumption. It must not be written to any file in `data/` or `secrets/`.

**Section A — Universe Scope**

| Field | Type | Description |
|---|---|---|
| `total_instruments` | int | Total instruments in `universe.json` (excludes malformed entries) |
| `verified_count` | int | Count with `status == "verified"` |
| `candidate_count` | int | Count with `status == "candidate"` |
| `verified_epics` | list[str] | Epics with `status == "verified"` |
| `candidate_epics` | list[str] | Epics with `status == "candidate"` |
| `unknown_status` | list[str] | Epics where `status` is neither `"verified"` nor `"candidate"` |
| `malformed_entries` | int | Count of instruments excluded due to missing or empty `epic` field |

**Section B — Series Scope**

| Field | Type | Description |
|---|---|---|
| `series_epics` | list[str] | All epic column headers found in `universe_series.xlsx` (any sheet) |
| `series_epic_count` | int | Count of unique epics across all sheets |
| `date_range` | dict[str, dict] | Per-sheet: `{"first": datetime, "last": datetime}` for each of `mid_close`, `bid_close`, `mid_open`; absent if sheet is missing |
| `sheets_have_consistent_date_range` | bool | True if all three sheets have identical first and last datetime values |
| `sheets_fully_consistent` | bool | True if all three sheets contain exactly the same set of epic columns (column set only — ordering is not checked) |
| `historic_file_count` | int | Number of files found in `data/input/historic_series/` (excluding `.gitkeep`) |
| `historic_files_present` | list[str] | Filenames found in `data/input/historic_series/` (excluding `.gitkeep`) |

**Section C — Discrepancy Analysis**

| Field | Type | Description |
|---|---|---|
| `in_universe_not_in_series` | list[str] | Epics present in `universe.json` but absent as columns in `universe_series.xlsx` |
| `in_series_not_in_universe` | list[str] | Epics present as columns in `universe_series.xlsx` but absent from `universe.json` entirely (orphaned series) |
| `verified_not_in_series` | list[str] | Subset of `verified_epics` that have no column in the series file — highest-priority gaps |
| `same_base_variant_orphans` | list[dict] | Epics in the series file whose 3-segment base (e.g. `IX.D.DAX`) matches a universe epic but the exact identifier differs. Format: `[{"series_epic": str, "universe_epic": str}]` |

---

## 4. Scope Definitions

### Scope 1 — Universe Scope

The universe scope answers: *what instruments does the system know about, and which are cleared for trading?*

- Source: `data/input/universe.json`
- A `verified` instrument has been tested against the IG Demo API by `discover_universe.py` and confirmed accessible.
- A `candidate` instrument is registered but unverified — it may or may not be tradeable.
- The scoper does NOT make live broker calls. The `verified` status reflects the last run of `discover_universe.py`. For reference, `main.py` currently configures `config["broker"] = "ig"` — the scoper does not read `main.py`; this is background context only.
- All `verified_epics` entries reflect IG Demo API validation only. If `config["broker"]` is not `"ig"`, the `verified_epics` list has no accessibility guarantee for the current broker.

### Scope 2 — Series Scope

The series scope answers: *what price data is actually stored, and for which instruments and date ranges?*

- Source: `data/input/universe_series.xlsx` (primary) and `data/input/historic_series/*.xlsx` (supplementary)
- Each column in the xlsx file represents one instrument's price history for one price type.
- Columns in the series file are IG epic strings — the same identifier space as `universe.json`.
- A column may exist but contain only `None` values; this is meaningfully different from the column being absent.
- The `historic_series/` folder supplements the master file. The scoper reports file count and names only; it does not parse the contents of historic files.

### Scope 3 — Discrepancy Analysis

The discrepancy analysis answers: *where are the gaps and anomalies between what the system knows and what it has stored?*

Three discrepancy categories are reported:

| Field | Meaning |
|---|---|
| `in_universe_not_in_series` | Universe entry has no stored series column |
| `in_series_not_in_universe` | Series column has no corresponding universe entry |
| `verified_not_in_series` | Verified (cleared-for-trading) epic has no series column — highest-priority gap |

`same_base_variant_orphans` is a sub-category of `in_series_not_in_universe`: series epics that share a 3-segment base with a universe epic but differ in the full identifier. These are distinct from true orphans where no base match exists in the universe at all.

---

## 5. Error Handling Contract

| Condition | Required behaviour |
|---|---|
| `universe.json` missing | Report as error in Section A output; do not abort — continue to Section B |
| `universe_series.xlsx` missing | Report absence in Section B; Section A proceeds normally |
| File present but corrupt / unreadable | Catch exception; report error for that file; continue with remaining sections |
| `historic_series/` absent or empty | Report `historic_file_count: 0`; no error |

The scoper must never raise an unhandled exception due to missing or malformed input files.

---

## 6. Dependencies

| Dependency | Source | Usage |
|---|---|---|
| `json` | Python stdlib | Parse `universe.json` |
| `pandas` | Already in `requirements.txt` | Read `universe_series.xlsx` and historic files; datetime loading via `pd.to_datetime(..., utc=True)` |
| `os` / `pathlib` | Python stdlib | Directory scan of `historic_series/` |

No broker dependencies. No network calls. No credentials required.

---

## 7. Guardrails and Out of Scope

The scoper MAY read only the files listed in Section 2.1. Everything else must not be read, called, or modified.

### 7.1 Assets that must not be touched

| Asset | Type | Reason |
|---|---|---|
| `data/input/universe.json` | Data file | Registry of record; modifications require `discover_universe.py` |
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

### 7.2 Explicitly out of scope

- Making broker API calls of any kind
- Writing, updating, or deleting any file in `data/` or `secrets/`
- Running the trading pipeline
- Verifying whether a candidate instrument is currently tradeable
- Fetching or ingesting new price bars
- Modifying `universe.json` to add, remove, or promote instruments
- Computing trading signals or portfolio metrics
- Importing or invoking `discover_universe.py`, `broker_connector.py`, `ig_adapter.py`, `ibkr_adapter.py`, `Model`, `RunLoop`, or `Handoff`

---

## 8. Scope Report — Structural Sketch

```
=== DataSource Scope Report ===

--- A. Universe Scope ---
{ total_instruments, verified_count, candidate_count, verified_epics,
  candidate_epics, unknown_status, malformed_entries }

--- B. Series Scope ---
{ series_epics, series_epic_count, date_range, sheets_have_consistent_date_range,
  sheets_fully_consistent, historic_file_count, historic_files_present }

--- C. Discrepancy Analysis ---
{ in_universe_not_in_series, in_series_not_in_universe, verified_not_in_series,
  same_base_variant_orphans }
```

*(The exact formatting is implementation detail — BUILDER territory.)*
