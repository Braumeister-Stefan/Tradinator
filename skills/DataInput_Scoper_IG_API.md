# DataInput Scoper — IG API Calls

**Skill:** `DataInput_Scoper_IG_API`
**Prepared by:** FUNCTIONALIST
**Scope:** Read-only static-analysis utility. Not part of the trading pipeline.
**Purpose:** Produce an exhaustive, machine-readable inventory of every call made
to the IG broker API across the entire codebase. The inventory records the source
file, enclosing function, and line number for each call so that rate-limit and
retry logic can be applied consistently to every call site.

---

## 1. Purpose and Boundaries

The IG API Scoper parses the codebase with Python's `ast` module and emits a
structured CSV listing every call to an `IGService` method.  It performs no
writes to `data/`, no broker calls, and no pipeline operations.  It is a
standalone static-analysis tool.

**What it does:**
- Walks all `.py` files under the repository root (excluding `skills/`,
  `.git/`, and virtual-environment directories)
- Identifies direct calls to `trading_ig.IGService` methods, i.e. attribute
  calls on any variable whose assignment traces back to `IGService(...)` or
  that is typed as `IGService`
- Records, for each call site:
  - `file` — relative path from repository root
  - `function` — enclosing function name (or `<module>` for top-level code)
  - `line` — 1-based line number of the call expression
  - `ig_method` — the attribute name called (e.g. `fetch_market_by_epic`)
  - `category` — logical category: `session`, `market_data`, `order`, `account`
- Writes one row per call site to `skills/ig_api_calls.csv`

---

## 2. Files in Scope

### 2.1 Permitted reads (allowlist)

The scoper MAY read only `.py` source files in the repository.  Data files,
secrets, and Excel files must not be read.

### 2.2 Scan roots

| Root | Notes |
|---|---|
| `model/model_components/ig_adapter.py` | Primary adapter — all normalised calls live here |
| `data/input/discover_universe.py` | Discovery script — direct IGService calls |
| All other `.py` files under project root | Checked for any out-of-band IG calls |

### 2.3 Exclusion list

The following directories are excluded from the walk:

- `skills/` (tooling, not production code)
- `.git/`
- `__pycache__/`
- `venv/`, `.venv/`, `env/`

---

## 3. Output Specification

### `skills/ig_api_calls.csv`

| Column | Type | Description |
|---|---|---|
| `file` | str | Relative file path from repository root |
| `function` | str | Enclosing function name, or `<module>` |
| `line` | int | 1-based line number of the call |
| `ig_method` | str | IG API method called (e.g. `fetch_market_by_epic`) |
| `category` | str | Logical grouping: `session`, `account`, `market_data`, `order` |
| `has_retry` | bool | Whether the call site is already wrapped in explicit retry logic |
| `notes` | str | Free-text annotation from the scoper |

Rows are sorted by `file` then `line`.

---

## 4. Category Taxonomy

| IG Method | Category |
|---|---|
| `create_session` | `session` |
| `fetch_accounts` | `account` |
| `fetch_open_positions` | `account` |
| `fetch_market_by_epic` | `market_data` |
| `fetch_historical_prices_by_epic_and_num_points` | `market_data` |
| `fetch_historical_prices_by_epic_and_date_range` | `market_data` |
| `search_markets` | `market_data` |
| `create_open_position` | `order` |
| `close_open_position` | `order` |
| `fetch_deal_by_deal_reference` | `order` |

Any method not in this table is categorised as `other`.

---

## 5. `has_retry` Detection Heuristic

The scoper inspects the enclosing function body (up to 30 lines surrounding the
call) for any of the following patterns.  If at least one is present, it sets
`has_retry=True`:

- A `for` loop with the word `attempt` or `retry` in the loop variable
- A `while` loop body that contains `time.sleep` and a variable with `wait` or
  `backoff` in its name
- An explicit `except` clause that catches an exception class whose name
  contains `Exceeded` or `RateLimit`

---

## 6. Error Handling Contract

| Condition | Required behaviour |
|---|---|
| A `.py` file cannot be parsed (syntax error) | Skip file; log warning; continue |
| No IG API calls found in a file | Omit file from CSV; no warning needed |
| Output directory (`skills/`) absent | Create it; continue |

The scoper must never raise an unhandled exception.

---

## 7. Guardrails

### 7.1 Assets that must not be touched

| Asset | Reason |
|---|---|
| `data/input/universe.json` | Registry of record |
| `data/input/universe_series.xlsx` | Master price series |
| `secrets/` | Credentials |
| `data/output/` | Pipeline outputs |

### 7.2 Explicitly out of scope

- Making any broker API calls
- Importing or invoking `ig_adapter.py`, `broker_connector.py`, or `IGService`
- Running the trading pipeline
- Modifying any production source file
