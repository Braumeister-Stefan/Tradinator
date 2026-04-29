# Dashboard Creation & Publishing — Technical Requirements Report

**Component under review:** `PerformanceMonitoring` and `dashboard.html`
**Files:** `model/model_components/performance_monitoring.py`, `model/model_components/templates/dashboard.html`
**Prepared by:** FUNCTIONALIST
**Purpose:** Handoff document for an engineer implementing a new dashboard delivery mechanism.

---

## 1. What the Code Does — Full Pipeline Description

### Pipeline position

`PerformanceMonitoring` is the final stage of the execution pipeline. It is called by `Model.run_execution()` as step 4 (Record & Report), after `PortfolioAnalytics` has computed metrics:

```
PortfolioLedger.run()   →   PortfolioAnalytics.run()   →   PerformanceMonitoring.run()
      (ledger_snapshot)           (analytics dict)               (side effects only)
```

`PerformanceMonitoring.run()` has no return value. It produces four side effects in sequence:

### Step 1 — Build text report (`_build_report`)
Converts the `analytics` dict into a fixed-width ASCII report string. Uses dot-leader alignment (`_format_metric`). Drawdown is negated before display via `_negate_drawdown` so it always renders as a negative number. Integer values (position count, snapshot count) are rendered without decimal places via `_int_or_none`.

### Step 2 — Print to stdout (`_print_report`)
Passes the string directly to `print()`. No filtering, truncation, or log-level gating.

### Step 3 — Save text report (`_save_report`)
Writes the string to `{output_dir}/performance_report.txt`. Controlled by the class-level constant `SAVE_REPORT = True`. `output_dir` is read from `self.config["output_dir"]`, defaulting to `"."`. Directory is created if absent. `OSError` is caught and logged to stdout; no re-raise.

### Step 4 — Render and publish HTML dashboard (`_save_html_report`)
This method performs three distinct sub-steps:

1. **Template rendering** — Loads `dashboard.html` from `model/model_components/templates/` using Jinja2's `FileSystemLoader`. The template path is resolved relative to `__file__`. `autoescape=True` is set. The `analytics` dict is merged with a `defaults` dict to ensure all expected template variables are present, then unpacked as keyword arguments into `template.render()`.

2. **File write** — Writes rendered HTML to `{output_dir}/performance_dashboard.html`. Directory is created if absent.

3. **Browser launch** — Calls `webbrowser.open(f"file:///{abs_path}")` using the absolute path of the written file. Prints a confirmation line to stdout.

All exceptions in `_save_html_report` are caught broadly (`except Exception`) and logged to stdout; no re-raise.

### Template rendering detail (`dashboard.html`)
The template is a standalone, static HTML page. It has no JavaScript. All logic is Jinja2. It renders a dark-themed 2×2 CSS Grid layout where:
- **Quadrant 1 (top-left):** All live performance metrics.
- **Quadrants 2–4:** Placeholder panels (dashed border, reserved icons, no data).

Metric rows apply semantic colour classes (`positive`, `negative`, `caution`, `na`) using Jinja2 conditionals. Drawdown sign is handled entirely within the template via `| abs` — the raw positive value from `analytics` is used (not the negated value `_build_report` applies via `_negate_drawdown`).

---

## 2. Encapsulation Assessment

### What is well-separated
- `_build_report`, `_print_report`, `_save_report`, and `_save_html_report` are distinct private methods, making the four output channels independently readable.
- Static helper methods (`_negate_drawdown`, `_int_or_none`, `_format_metric`, `_print_report`) have no side effects and are trivially testable in isolation.
- `PerformanceMonitoring` takes an opaque `analytics` dict, creating a clean interface boundary with `PortfolioAnalytics`.
- The class has a single public entry point (`run()`), which keeps the external contract narrow.

### Where responsibility is concentrated
`_save_html_report` conflates three concerns that would each change for independent reasons:
1. Template rendering (Jinja2 environment setup, context merging, `template.render()`).
2. File I/O (path construction, `os.makedirs`, file write).
3. Delivery (browser launch via `webbrowser.open()`).

If the delivery mechanism changes (the stated goal), it requires editing a method that also owns rendering and file-write logic.

---

## 3. Limits to Encapsulation

### 3.1 Delivery is fused with file-write
`webbrowser.open()` is called immediately after the file write with no intervening abstraction. There is no method boundary between "the HTML is ready" and "do something with it". Replacing `webbrowser.open()` requires editing `_save_html_report` directly, which risks disturbing the rendering and I/O logic alongside it.

### 3.2 Template path is hardcoded
```python
template_dir = os.path.join(os.path.dirname(__file__), "templates")
```
This resolves correctly only when the file is on disk in its expected location. It cannot be overridden via config and is invisible to callers. A test that patches the template directory has no seam to inject an alternative path.

### 3.3 Output filename is a hardcoded string, not a class constant
`"performance_report.txt"` is declared as `REPORT_FILENAME` (class constant), but `"performance_dashboard.html"` is a bare string literal inside `_save_html_report`. These are inconsistent. The HTML filename cannot be overridden without subclassing or monkey-patching.

### 3.4 Broad exception catch masks failures silently
```python
except Exception as exc:
    print(f"[PerformanceMonitoring] Could not save HTML report: {exc}")
```
Template not found, Jinja2 syntax error, permission denied, and browser-not-found all produce the same silent log line. The caller (`Model.run_execution`) receives no indication that the dashboard was not delivered.

### 3.5 Drawdown sign treatment is inconsistent between outputs
- Text report: `_negate_drawdown()` converts the value to negative before passing to `_format_metric`.
- HTML template: receives the raw positive value and applies `| abs` then prefixes a literal `-`.

Both outputs are visually correct, but the data passed to the template for `max_drawdown_pct` is the raw positive value from `PortfolioAnalytics` — this must be documented for any template or downstream consumer.

### 3.6 `SAVE_REPORT` is a class-level constant, not config-driven
All other behaviour is config-driven. `SAVE_REPORT = True` cannot be controlled from `main.py`'s `config` dict without subclassing. The HTML dashboard has no equivalent control at all.

### 3.7 `PortfolioAnalytics._print_summary` duplicates output concerns
`PortfolioAnalytics` prints its own inline summary to stdout before passing `analytics` to `PerformanceMonitoring`. This means the console receives two separate stdout outputs for the same run: one from analytics computation, one from the monitoring report. The print concern is split across two classes.

---

## 4. Key Extension Points — Migrating from `webbrowser.open()`

### What would change

To replace `webbrowser.open()` with another delivery mechanism (e.g. email attachment, HTTP push, file copy to a remote share, Slack upload), the minimal change is to extract the delivery call from `_save_html_report` into a dedicated method with a defined signature.

**Proposed seam:**
```python
def _deliver_dashboard(self, html: str, local_path: str) -> None:
    """Deliver the rendered dashboard. Override to change delivery mechanism."""
    webbrowser.open(f"file:///{local_path}")
```

`_save_html_report` would call `self._deliver_dashboard(html, abs_path)` after the file write. The `html` string is passed so delivery mechanisms that do not need a local file (e.g. sending via API) can operate without touching the filesystem.

### What must remain unchanged
- The Jinja2 rendering step — it is self-contained and produces a plain string.
- The `analytics` dict structure — the template contract is defined by it (see Section 5).
- `output_dir` config key — the file write step depends on it.

### What must also change for a complete delivery refactor
| Concern | Current location | Recommended location |
|---|---|---|
| Template directory resolution | Hardcoded in `_save_html_report` | Config key `template_dir` or class constant `TEMPLATE_DIR` |
| HTML output filename | Hardcoded string in `_save_html_report` | Class constant `DASHBOARD_FILENAME` (mirrors `REPORT_FILENAME`) |
| Delivery toggle | Does not exist | Config key `deliver_dashboard` (bool) analogous to `SAVE_REPORT` |
| Exception handling | Broad `except Exception` | Separate try/except around rendering vs. delivery so each failure is attributable |

### What does NOT need to change
- The `analytics` dict contract (Section 5 below).
- `_build_report` and the text report path — they are independent of the HTML pipeline.
- `dashboard.html` — the template is already a self-contained, portable HTML string once rendered.

---

## 5. Data Contract — `analytics` Dict Schema

This is the schema produced by `PortfolioAnalytics.run()` and consumed by `PerformanceMonitoring.run()` and `dashboard.html`. All keys are always present in the dict returned by `PortfolioAnalytics`; `None` signals insufficient data (fewer than 2 history snapshots).

### Top-level keys

| Key | Type | Nullable | Description |
|---|---|---|---|
| `timestamp` | `str` | No (empty string fallback) | ISO 8601 UTC timestamp of the ledger snapshot that produced this analytics run. Format: `YYYY-MM-DDTHH:MM:SS.ffffff` |
| `total_return_pct` | `float` | Yes | `(last_balance - first_balance) / first_balance * 100`. `None` if history has fewer than 2 snapshots or first balance is zero. |
| `period_return_pct` | `float` | Yes | Return from second-to-last snapshot to last snapshot, as a percentage. `None` if fewer than 2 snapshots or previous balance is zero. |
| `max_drawdown_pct` | `float` | Yes | Maximum peak-to-trough decline as a **positive** percentage (e.g. `12.5` means 12.5% drawdown). `None` if fewer than 2 snapshots. The negative sign is applied at the display layer. |
| `sharpe_ratio` | `float` | Yes | Annualised Sharpe ratio using population std, 252-day annualisation, 4% risk-free rate. `None` if fewer than 2 return periods or std is zero. |
| `volatility_annual_pct` | `float` | Yes | Annualised return volatility as a positive percentage. `None` if fewer than 2 return periods. |
| `current_exposure` | `dict` | No (always a dict) | See sub-schema below. |
| `history_length` | `int` | No | Number of ledger snapshots in the full history. Minimum value: 1 (current snapshot always included). |

### `current_exposure` sub-dict

Always present as a dict. If `balance == 0.0`, all percentage values are `0.0`.

| Key | Type | Nullable | Description |
|---|---|---|---|
| `invested_pct` | `float` | No | `(balance - cash) / balance * 100`. Percentage of portfolio in open positions. |
| `cash_pct` | `float` | No | `cash / balance * 100`. Percentage of portfolio in cash. |
| `position_count` | `int` | No | Number of open positions at the time of the snapshot. |

### Drawdown sign convention — important

`max_drawdown_pct` is stored as a **positive** number in the dict. The display sign is applied independently by each consumer:
- `_build_report` calls `_negate_drawdown()` to convert it to negative before the text formatter.
- `dashboard.html` applies `| abs` and prepends a literal `-` character.

Any new consumer of `max_drawdown_pct` must apply its own sign treatment. Do not assume the dict value is negative.

### Template variable injection

`_save_html_report` merges `analytics` with a `defaults` dict before calling `template.render(**context)`. The defaults ensure template variables are never undefined:

```python
defaults = {
    "timestamp": "",
    "total_return_pct": None,
    "period_return_pct": None,
    "max_drawdown_pct": None,
    "sharpe_ratio": None,
    "volatility_annual_pct": None,
    "current_exposure": None,
    "history_length": None,
}
```

`current_exposure` may therefore be `None` at the template layer. The template guards against this:
```jinja
{% set exposure = current_exposure or {} %}
```

All `exposure.get(...)` calls in the template are safe against a `None` value because of this guard.

---

## 6. Summary of Issues Relevant to a Delivery Refactor

The following items are blockers or risks for a clean delivery mechanism migration, ranked by impact:

1. **No delivery abstraction point.** `webbrowser.open()` is inlined in `_save_html_report` with no extractable seam. Must be refactored before an alternative delivery mechanism can be injected cleanly.

2. **Broad exception catch.** Rendering failure and delivery failure produce identical, undifferentiated output. Split before implementing any delivery that has its own failure modes (e.g. network errors).

3. **HTML filename is not a class constant.** The filename `"performance_dashboard.html"` is a bare string literal. Elevate to a class constant (`DASHBOARD_FILENAME`) to match the pattern of `REPORT_FILENAME` and allow subclass override.

4. **Template path is not injectable.** The template directory is resolved from `__file__`. Add a config key or class constant (`TEMPLATE_DIR`) to make it overridable.

5. **Drawdown sign inconsistency.** Document at the point of use (inline comment) that `max_drawdown_pct` is always positive in the dict and that callers must negate for display. Currently undocumented.

6. **`SAVE_REPORT` is not in config.** Low priority but inconsistent with the config-driven pattern used everywhere else in the codebase.
