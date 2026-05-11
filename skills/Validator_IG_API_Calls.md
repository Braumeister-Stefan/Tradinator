# Validator — IG API Calls

**Skill:** `Validator_IG_API_Calls`
**Prepared by:** FUNCTIONALIST
**Input:** `skills/ig_api_calls.csv` (produced by `DataInput_Scoper_IG_API`)
**Output:** `skills/ig_api_calls_validated.csv`
**Purpose:** Adversarially challenge the scoper's inventory for completeness,
correctness, and retry-coverage gaps. Emit a validated CSV that adds
`validation_status` and `remediation_needed` columns to each row and appends a
summary section.

---

## 1. Validation Rules

### Rule 1 — Expected methods present

The following IG API methods MUST appear at least once in the inventory.
Any absent method is a FAIL:

| Method | Expected in file |
|---|---|
| `create_session` | `ig_adapter.py` AND `discover_universe.py` |
| `fetch_accounts` | `ig_adapter.py` |
| `fetch_open_positions` | `ig_adapter.py` |
| `fetch_market_by_epic` | `ig_adapter.py` AND `discover_universe.py` |
| `fetch_historical_prices_by_epic_and_num_points` | `ig_adapter.py` AND `discover_universe.py` |
| `fetch_historical_prices_by_epic_and_date_range` | `ig_adapter.py` |
| `search_markets` | `discover_universe.py` |
| `create_open_position` | `ig_adapter.py` |
| `close_open_position` | `ig_adapter.py` |
| `fetch_deal_by_deal_reference` | `ig_adapter.py` |

### Rule 2 — No duplicate call sites

Two rows with the same `(file, line)` pair constitute a duplicate and are
flagged `DUPLICATE`.

### Rule 3 — Retry coverage

Any call where `has_retry == False` AND `category` is one of
`market_data`, `account`, or `order` is flagged as
`NEEDS_RETRY` — it is exposed to `ApiExceededException` without protection.

`session` category calls are exempt: session creation has dedicated retry logic
built in and a different failure mode.

### Rule 4 — Category consistency

Every `ig_method` value must appear in the known taxonomy (see
`DataInput_Scoper_IG_API.md` Section 4). Unknown methods are flagged `UNKNOWN_CATEGORY`.

### Rule 5 — Line number sanity

`line` must be a positive integer.  Zero or negative values are flagged `INVALID_LINE`.

---

## 2. Output — `skills/ig_api_calls_validated.csv`

Adds three columns to the scoper's schema:

| Column | Type | Description |
|---|---|---|
| `validation_status` | str | `OK`, `NEEDS_RETRY`, `DUPLICATE`, `UNKNOWN_CATEGORY`, `INVALID_LINE` |
| `remediation_needed` | bool | `True` when `validation_status != OK` |
| `validator_notes` | str | Human-readable explanation of the status |

Rows are sorted by `(file, line)`.

A summary block is appended after the last data row as a comment section
(lines prefixed with `#`) containing:
- Total rows
- Count of `NEEDS_RETRY` rows
- Count of `DUPLICATE` rows
- Count of `OK` rows
- Missing expected methods (if any)
- Overall verdict: `PASS` or `FAIL`

---

## 3. Guardrails

The validator reads only `skills/ig_api_calls.csv`.
It writes only `skills/ig_api_calls_validated.csv`.
It makes no broker calls, no pipeline imports, and no writes to `data/` or
`secrets/`.

The validator must never raise an unhandled exception.  If the input CSV is
absent or unreadable, it writes an error-only output CSV and exits cleanly.

---

## 4. Verdict Logic

| Condition | Overall verdict |
|---|---|
| Any missing expected method | `FAIL` |
| Any `NEEDS_RETRY` row | `FAIL` (after remediation, re-run to verify) |
| Any `DUPLICATE` row | `FAIL` |
| All rows `OK` | `PASS` |
