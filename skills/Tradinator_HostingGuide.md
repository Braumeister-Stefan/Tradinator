# Tradinator Hosting Guide

**Skill:** `Tradinator_HostingGuide`
**Produced by:** DOCUMENTER → VALIDATOR → FUNCTIONALIST → OPTIMIZER pipeline
**Repository:** Braumeister-Stefan/Tradinator
**Target deployment:** https://wilmars.one (GoDaddy hosting)

---

## How the Dashboard Currently Works

The dashboard is the final output of a six-stage pipeline that runs at the end of every execution cycle.

### Stage 1 — Ledger snapshot

`run_execution` in `model/model.py` re-fetches live broker state after all orders are submitted, then calls `reconciliation.run()` on the re-fetched state (adjusting positions and balances to match broker reality), then passes the reconciled state to `portfolio_ledger.run(execution_log, broker_state)`. The ledger returns a `ledger_snapshot` dict:

| Field | Content |
|---|---|
| `history` | List of `{balance, timestamp}` dicts, one per completed run |
| `balance` | Current total portfolio value |
| `cash` | Uninvested cash |
| `positions` | List of open position records (`instrument_id`, `size`, `level`) |
| `timestamp` | ISO-8601 string of the current run |

### Stage 2 — Analytics computation

`PortfolioAnalytics.run(ledger_snapshot)` computes nine values from the ledger:

- `total_return_pct`, `period_return_pct` (require ≥ 2 history snapshots)
- `max_drawdown_pct`, `sharpe_ratio` (4 % risk-free rate, 252-day annualisation), `volatility_annual_pct`
- `current_exposure`: `invested_pct`, `cash_pct`, `position_count`
- `history_length`

Returns the `analytics` dict and prints a one-line stdout summary.

### Stage 3 — Formatting and persistence

`PerformanceMonitoring.run(analytics)` executes four steps:

1. Builds a 60-char dot-leader text report → stdout + `data/output/performance_report.txt`
2. Resolves metrics via `METRICS_CONFIG` (Returns / Risk / Exposure / History groups)
3. Computes pie chart slices: top-3 positions by notional + "Others"; labels from the third dot-segment of the epic string
4. Renders the Jinja2 template → `data/output/performance_dashboard.html`; writes `data/output/dashboard_data.json`

### Stage 4 — Delivery

`_deliver_dashboard()` (called from inside `_save_html_report`) starts a `SimpleHTTPRequestHandler` on `127.0.0.1:8742` serving `data/output/`. Opens the browser on first run (sentinel file `.dashboard_opened`). Blocks `thread.join()` indefinitely — `PerformanceMonitoring.run()` never returns until Ctrl+C. This is the default `"localhost"` delivery mode.

### Stage 5 — Browser rendering

Dark-themed 2×2 CSS grid (max-width 1100 px):
- **Q1** — performance metrics driven by `METRICS_CONFIG`; semantic colour coding (green/red for signed returns, amber for caution, red for drawdown)
- **Q2** — HTML5 canvas pie chart of position weights; legend alongside
- **Q3/Q4** — reserved placeholders

### Stage 6 — Live polling

An inline IIFE fetches `dashboard_data.json?t=<epoch>` (cache-busted) 2 seconds after page load, then every 60 seconds. On success, `applyData()` patches metric DOM elements in-place and redraws the pie chart from `d.pie_chart_data`. Fetch errors are silently swallowed.

---

## Problems That Prevent Web Hosting

| # | Problem | Severity |
|---|---------|----------|
| 1.1 | `thread.join()` inside `_deliver_dashboard` blocks the process indefinitely — scheduled/decoupled mode never runs a second cycle | **Primary blocker** |
| 1.2 | `data/output/*` is gitignored — generated files have no path to GoDaddy | **Primary blocker** |
| 1.3 | `dashboard_data.json` fetched via bare relative URL — breaks if HTML and JSON are not co-located | **Primary blocker** |
| 1.4 | Pie chart baked into HTML at render time — stale between pipeline runs on a hosted page | **Functional gap** |
| 1.5 | `webbrowser.open()` called on the pipeline host — fails silently on headless servers | **Secondary** |
| 1.6 | HTTP server bound to `0.0.0.0` — unintended network exposure of the entire `data/output/` directory | **Security** |
| 1.7 | `dashboard_data.json` exposes position composition publicly once deployed | **Disclosure** |

---

## Proposed Hosting Architecture

**Static file deployment via FTP.** The pipeline continues to run locally. On each run it writes `performance_dashboard.html` and `dashboard_data.json` to `data/output/`, then uploads both files to GoDaddy via FTP over TLS (FTPS). GoDaddy serves them as static files. The browser polls `dashboard_data.json` from the same host every 60 seconds.

- No server-side code runs on GoDaddy
- No new external Python dependencies (uses `ftplib` from the standard library)
- The existing localhost mode is fully preserved as the default
- `.gitignore` is unchanged — FTP bypasses Git entirely

---

## Code Changes Implemented

### 1. `model/model_components/performance_monitoring.py`

**Class constants** — replaced dead `DASHBOARD_SERVER_LINGER_SECONDS` with:
```python
DELIVER_MODE = "localhost"   # "localhost" | "file_only" | "ftp" | "netlify"
FTP_REMOTE_DIR = ""
```

**`_save_html_report()`** — added `deliver_mode` dispatch after writing both files:
- `"localhost"` → existing `_deliver_dashboard()` (default, unchanged behaviour)
- `"file_only"` → prints path and returns immediately (non-blocking, for headless/cron use)
- `"ftp"` → calls `_publish_via_ftp(output_dir)`
- `"netlify"` → calls `_publish_via_netlify(output_dir)`

Added `dashboard_data_url` to the Jinja2 context dict (replaces `dashboard_data_filename`):
```python
context["dashboard_data_url"] = self.config.get(
    "dashboard_data_url", self.DASHBOARD_DATA_FILENAME
)
```
Default is `"dashboard_data.json"` — preserves relative-URL behaviour for localhost.

**`_write_dashboard_json()`** — signature extended to accept `pie_chart_data: list`. Pre-computed pie slices are included in the JSON payload under key `pie_chart_data`, enabling the polling script to redraw the chart on each cycle.

**`_deliver_dashboard()`** — bound to `"127.0.0.1"` (loopback only) instead of `""` (all interfaces). `import webbrowser` moved inside the method body so it is never imported in FTP or file-only mode.

**`_publish_via_ftp(output_dir)`** — new method:
- Reads `ftp_host`, `ftp_user`, `ftp_password`, `ftp_remote_dir` from `self.config`
- If any key is missing, logs a descriptive error and returns without raising
- Opens `ftplib.FTP_TLS`, calls `.prot_p()` for encrypted data transfer
- Uploads **only** `performance_dashboard.html` and `dashboard_data.json` — never the full directory
- Wraps the FTP block in `try/except ftplib.all_errors`; pipeline continues on failure

**`_publish_via_netlify(output_dir)`** — new method:
- Reads `netlify_token` and `netlify_site_id` from `self.config`
- If either key is missing, logs a descriptive error and returns without raising
- Uses only stdlib (`hashlib`, `urllib.request`) — no new dependencies
- SHA-1 digests each file and POSTs a deploy manifest to `api.netlify.com`
- PUTs only the files whose digests Netlify does not already have cached (incremental deploy)
- Wraps all network calls in `try/except`; pipeline continues on failure

### 2. `model/model_components/templates/dashboard.html`

**Pie chart** — extracted drawing logic into a named `renderPie(slices)` function exposed as `window._renderPie`. Initial render uses Jinja2-baked data on page load. The polling `applyData()` function calls `window._renderPie(d.pie_chart_data)` on each successful poll, keeping the chart current.

**Polling URL** — replaced:
```javascript
var DATA_URL = "{{ dashboard_data_filename }}";
```
with:
```javascript
var DATA_URL = "{{ dashboard_data_url }}";
```
When `dashboard_data_url` is set to an absolute URL in config (e.g. `https://wilmars.one/dashboard_data.json`), polling works from any page location.

### 3. `main.py`

Added `dotenv_values` import. Config dict reads FTP and Netlify keys from `secrets/.env` at startup:
```python
"deliver_mode":       _env.get("DELIVER_MODE", "localhost"),
"dashboard_data_url": _env.get("DASHBOARD_DATA_URL", "dashboard_data.json"),
"ftp_host":           _env.get("FTP_HOST", ""),
"ftp_user":           _env.get("FTP_USER", ""),
"ftp_password":       _env.get("FTP_PASSWORD", ""),
"ftp_remote_dir":     _env.get("FTP_REMOTE_DIR", ""),
"netlify_token":      _env.get("NETLIFY_TOKEN", ""),
"netlify_site_id":    _env.get("NETLIFY_SITE_ID", ""),
```
All default to safe empty/localhost values — existing users are unaffected.

### 4. `secrets/.env.example`

Added three new commented-out sections:
```
# --- Dashboard delivery (optional) ---
# DELIVER_MODE=localhost          # localhost | file_only | ftp | netlify
# DASHBOARD_DATA_URL=dashboard_data.json

# --- GoDaddy FTP (required when DELIVER_MODE=ftp) ---
# FTP_HOST=ftp.yourdomain.com
# FTP_USER=your_ftp_username
# FTP_PASSWORD=your_ftp_password
# FTP_REMOTE_DIR=/public_html/tradinator

# --- Netlify (required when DELIVER_MODE=netlify) ---
# NETLIFY_TOKEN=your_netlify_personal_access_token
# NETLIFY_SITE_ID=your_netlify_site_id
```

---

## Deployment Workflow — Netlify (recommended when no cPanel hosting exists)

Use this workflow when the GoDaddy account holds only a domain registration with no hosting plan (e.g. the account shows only **Domains** and **Websites + Marketing**, not **Web Hosting / cPanel**).

### Why Netlify

- Free tier (100 GB bandwidth / month, unlimited deploys)
- No server to manage — Netlify serves the static files
- Custom domain (`wilmars.one`) supported via a free CNAME record in GoDaddy DNS
- No FTP, no cPanel, no hosting purchase required
- The pipeline publishes via Netlify's REST API using only Python stdlib (`hashlib`, `urllib.request`)

### One-time setup

**A. Create a Netlify site**

1. Go to [app.netlify.com](https://app.netlify.com) → **Add new site** → **Deploy manually**.
2. Drop any placeholder file when prompted (the first real deploy will overwrite it).
3. Note the **Site ID** from **Site configuration → General → Site details** — it looks like `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.
4. Go to **User settings → Applications → Personal access tokens** → **New access token**.  Give it a name (e.g. `tradinator`) and copy the token — you will not see it again.

**B. Point your custom domain at Netlify**

1. In Netlify: **Domain management → Add custom domain** → enter `wilmars.one`.  Follow the prompts.
2. In GoDaddy: **My Products → Domains → wilmars.one → DNS → Add** a CNAME record:
   - **Host:** `@` (or `www` if you prefer `www.wilmars.one`)
   - **Points to:** `<your-netlify-site-name>.netlify.app`
   - **TTL:** 1 hour
3. DNS propagation takes up to 24 hours.  Netlify provisions an SSL certificate automatically once the DNS resolves.

**C. Configure `secrets/.env`**

Add these lines (copy from `secrets/.env.example`):

```
DELIVER_MODE=netlify
NETLIFY_TOKEN=your_netlify_personal_access_token
NETLIFY_SITE_ID=your_netlify_site_id
DASHBOARD_DATA_URL=https://wilmars.one/dashboard_data.json
```

> `DASHBOARD_DATA_URL` tells the in-page JavaScript where to poll for updates.
> Use your custom domain once DNS is live, or `https://<site-name>.netlify.app/dashboard_data.json` until then.

**D. Run the pipeline**

```
python main.py
```

Confirm both files are live:
- `https://wilmars.one/` (or `https://<site-name>.netlify.app/`) → dashboard HTML
- `https://wilmars.one/dashboard_data.json` → JSON sidecar (should be valid JSON)

### Per-run workflow (after setup)

Every pipeline execution automatically:
1. Computes analytics and renders the dashboard
2. Writes `data/output/performance_dashboard.html` and `data/output/dashboard_data.json`
3. Deploys both files to Netlify via the Files Deploy API (only uploads what changed)
4. Returns — no blocking, scheduled/decoupled mode continues normally

### Latency

- JSON is updated at pipeline run frequency (e.g. every 3600 seconds in `scheduled` mode)
- An open browser tab reflects the latest run within ≤ 60 seconds of the deploy completing
- The pie chart updates on the next poll cycle after each deploy

---

## Deployment Workflow — GoDaddy FTP (requires a cPanel hosting plan)

### One-time setup

1. In GoDaddy cPanel → **FTP Accounts**, create an account for Tradinator. Note the FTP hostname (usually `ftp.yourdomain.com`), username, and password.
2. In cPanel → **File Manager**, create the directory `/public_html/tradinator/`.
3. Add to `secrets/.env`:
   ```
   DELIVER_MODE=ftp
   FTP_HOST=ftp.wilmars.one
   FTP_USER=<your ftp username>
   FTP_PASSWORD=<your ftp password>
   FTP_REMOTE_DIR=/public_html/tradinator
   DASHBOARD_DATA_URL=https://wilmars.one/tradinator/dashboard_data.json
   ```
4. Run the pipeline once: `python main.py`
5. Confirm both files appear at `https://wilmars.one/tradinator/performance_dashboard.html` and `https://wilmars.one/tradinator/dashboard_data.json`.
6. Confirm the timestamp element updates approximately 62 seconds after opening the page.

### Per-run workflow (after setup)

Every pipeline execution automatically:
1. Computes analytics and renders the dashboard
2. Writes `data/output/performance_dashboard.html` and `data/output/dashboard_data.json`
3. Uploads both files to GoDaddy via FTPS
4. Returns — no blocking, scheduled/decoupled mode continues normally

### Latency

- JSON is updated at pipeline run frequency (e.g. every 3600 seconds in `scheduled` mode)
- An open browser tab reflects the latest run within ≤ 60 seconds of the upload completing
- The pie chart updates on the next poll cycle after each JSON upload

---

## Data and Security Considerations

| Concern | Detail |
|---|---|
| **Loopback binding** | `HTTPServer` now binds to `127.0.0.1` — localhost server is no longer reachable from the network |
| **Broker credentials** | Neither `_publish_via_ftp()` nor `_publish_via_netlify()` touches `secrets/.env`, ledger files, or handoff data — only the two dashboard files are ever uploaded |
| **FTP credentials** | Stored in `secrets/.env` (gitignored). Uses `FTP_TLS` with `.prot_p()` — credentials and data transfer are encrypted |
| **Netlify token** | Stored in `secrets/.env` (gitignored). Transmitted only over HTTPS to `api.netlify.com` |
| **Public portfolio data** | `dashboard_data.json` on Netlify/GoDaddy is publicly readable. It exposes position counts, weights, labels (e.g. "FTSE"), and return metrics. This is paper trading data — no real monetary exposure — but the operator should confirm this disclosure is acceptable |
| **Access control** | If access control is required for the FTP workflow, add a `.htaccess` file to `/public_html/tradinator/` via FTP enabling HTTP Basic Auth. For Netlify, use **Site configuration → Access control → Basic password protection** (Pro plan) or a Netlify Edge Function. The polling script would need `credentials: "include"` added to the `fetch()` call |
| **CORS** | HTML and JSON are co-located on the same origin — no CORS headers required |

---

## CORS Constraint

Both files **must** be served from the same origin. If they are on different origins (e.g. HTML on Netlify, JSON on GitHub raw), the browser blocks the `fetch()` call. Co-location is a hard constraint of the polling architecture.

---

## Alternative: GitHub Actions + FTP Deploy

Instead of uploading from within the Python process, the pipeline can commit the two files to a deploy branch and trigger a GitHub Actions workflow that uses an FTP deploy action (e.g. `SamKirkland/FTP-Deploy-Action`) to push to GoDaddy. GoDaddy FTP credentials are stored as GitHub Actions secrets. This keeps deployment logic out of the Python code but requires a Git commit per deploy, which pollutes repository history with generated data. The in-process `ftplib` approach is simpler and preferred when cPanel hosting is available.
