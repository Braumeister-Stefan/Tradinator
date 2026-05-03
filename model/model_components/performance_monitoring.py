"""
Tradinator — Performance Monitoring.

Presentation layer that takes analytics output and formats it for human
consumption.  Phase 1: prints a formatted summary table to stdout and
optionally writes the report to a text file.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import base64
import ftplib
import hashlib
import json
import os
import posixpath
import threading
import urllib.error
import urllib.request
from http.server import HTTPServer, SimpleHTTPRequestHandler

from jinja2 import Environment, FileSystemLoader


# ---------------------------------------------------------------------------
# Metrics configuration
# ---------------------------------------------------------------------------
# Each group key is the section heading shown in Panel 1 of the dashboard.
# Within each group, add/remove metric dicts freely.
# Set "enabled" to True to display the metric, False to hide it.
# "key"     – matches the key in the analytics dict (or current_exposure sub-dict
#             when "source" is set to "exposure").
# "label"   – human-readable name shown in the dashboard.
# "suffix"  – appended to the value (e.g. "%" or "").
# "color"   – rendering hint: "signed" | "drawdown" | "sharpe" | "caution" | "integer" | "neutral"
# ---------------------------------------------------------------------------
METRICS_CONFIG = {
    "Returns": [
        {"key": "total_return_pct",     "label": "Total Return",        "enabled": True,  "suffix": "%", "color": "signed"},
        {"key": "period_return_pct",    "label": "Period Return",        "enabled": True,  "suffix": "%", "color": "signed"},
    ],
    "Risk": [
        {"key": "max_drawdown_pct",     "label": "Max Drawdown",         "enabled": True,  "suffix": "%", "color": "drawdown"},
        {"key": "sharpe_ratio",         "label": "Sharpe Ratio",         "enabled": True,  "suffix": "",  "color": "sharpe"},
        {"key": "volatility_annual_pct","label": "Annual Volatility",    "enabled": True,  "suffix": "%", "color": "caution"},
    ],
    "Exposure": [
        {"key": "invested_pct",         "label": "Invested",             "enabled": True,  "suffix": "%", "color": "neutral", "source": "exposure"},
        {"key": "cash_pct",             "label": "Cash",                 "enabled": True,  "suffix": "%", "color": "neutral", "source": "exposure"},
        {"key": "position_count",       "label": "Open Positions",       "enabled": True,  "suffix": "",  "color": "integer", "source": "exposure"},
    ],
    "History": [
        {"key": "history_length",       "label": "Snapshots Available",  "enabled": True,  "suffix": "",  "color": "integer"},
    ],
}


class PerformanceMonitoring:
    """Format and display a human-readable performance report."""

    DISPLAY_WIDTH = 60
    SAVE_REPORT = True
    REPORT_FILENAME = "performance_report.txt"
    DASHBOARD_FILENAME = "performance_dashboard.html"
    DASHBOARD_DATA_FILENAME = "dashboard_data.json"
    DASHBOARD_SENTINEL_FILENAME = ".dashboard_opened"
    DASHBOARD_HTTP_PORT = 8742
    # Delivery mode: "localhost" (default), "file_only", "ftp", or "netlify"
    DELIVER_MODE = "localhost"
    FTP_REMOTE_DIR = ""

    # Color palette used for the positions pie chart slices (top-3 + others).
    PIE_COLORS = ["#4a90d9", "#34d399", "#fbbf24", "#6b7a8d"]

    def __init__(self, config: dict):
        """Store config for later use by run()."""
        self.config = config

    def run(self, analytics: dict) -> None:
        """Build, print, and optionally save the performance report."""
        report = self._build_report(analytics)
        self._print_report(report)
        self._save_report(report)
        self._save_html_report(analytics)

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _format_metric(self, label: str, value, suffix: str = "") -> str:
        """Format a single metric line with dot-leader alignment."""
        if value is None:
            formatted_value = "N/A"
        elif isinstance(value, str):
            formatted_value = f"{value}{suffix}"
        else:
            formatted_value = f"{value:.2f}{suffix}"

        # "  Label " + dots + " Value"
        prefix = f"  {label} "
        # Reserve space for the value on the right side
        available = self.DISPLAY_WIDTH - len(prefix) - len(formatted_value)
        dots = "." * max(available, 1)
        return f"{prefix}{dots} {formatted_value}"

    def _build_report(self, analytics: dict) -> str:
        """Construct the full report string."""
        separator = "=" * self.DISPLAY_WIDTH
        timestamp = analytics.get("timestamp", "")

        lines = [
            separator,
            "  TRADINATOR — Performance Report",
            f"  Generated: {timestamp}",
            separator,
            "",
            "  RETURNS",
            self._format_metric(
                "Total Return",
                analytics.get("total_return_pct"),
                "%",
            ),
            self._format_metric(
                "Period Return",
                analytics.get("period_return_pct"),
                "%",
            ),
            "",
            "  RISK",
            self._format_metric(
                "Max Drawdown",
                self._negate_drawdown(analytics.get("max_drawdown_pct")),
                "%",
            ),
            self._format_metric(
                "Sharpe Ratio",
                analytics.get("sharpe_ratio"),
            ),
            self._format_metric(
                "Annual Volatility",
                analytics.get("volatility_annual_pct"),
                "%",
            ),
            "",
            "  EXPOSURE",
        ]

        exposure = analytics.get("current_exposure") or {}
        lines += [
            self._format_metric(
                "Invested", exposure.get("invested_pct"), "%"
            ),
            self._format_metric("Cash", exposure.get("cash_pct"), "%"),
            self._format_metric(
                "Open Positions",
                self._int_or_none(exposure.get("position_count")),
            ),
            "",
            "  HISTORY",
            self._format_metric(
                "Snapshots Available",
                self._int_or_none(analytics.get("history_length")),
            ),
            "",
            separator,
            "  DISCLAIMER: This is not trading advice. Paper trading only.",
            separator,
        ]

        return "\n".join(lines)

    @staticmethod
    def _print_report(report: str) -> None:
        """Print the report to stdout."""
        print(report)

    def _save_report(self, report: str) -> None:
        """Write the report to a text file if SAVE_REPORT is enabled."""
        if not self.SAVE_REPORT:
            return
        try:
            output_dir = self.config.get("output_dir", ".")
            path = os.path.join(output_dir, self.REPORT_FILENAME)
            os.makedirs(output_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(report)
                fh.write("\n")
        except OSError as exc:
            print(f"[PerformanceMonitoring] Could not save report: {exc}")

    def _build_pie_chart_data(self, analytics: dict) -> list[dict]:
        """Compute pie chart slices for the positions panel.

        Returns a list of dicts with keys: label, pct, color.
        Cash is excluded.  The top-3 positions by notional value are shown
        individually; all remaining positions are combined as ``Others``.

        Multiple broker position tickets for the same instrument (e.g. two
        separate orders filled at different times) are aggregated by
        instrument_id so each underlying market appears only once.
        """
        positions = analytics.get("positions") or []

        # Aggregate notional value by instrument_id across all position tickets.
        aggregated: dict[str, float] = {}
        for pos in positions:
            level = float(pos.get("level") or 0)
            size = abs(float(pos.get("size") or 0))
            value = size * level
            if value > 0:
                raw_id = pos.get("instrument_id") or "Unknown"
                aggregated[raw_id] = aggregated.get(raw_id, 0.0) + value

        valued = []
        for raw_id, value in aggregated.items():
            # Use the third dot-segment of the epic as a compact label
            # (e.g. "FTSE" from "IX.D.FTSE.DAILY.IP"), falling back to
            # the full instrument_id when the format does not match.
            parts = raw_id.split(".")
            label = parts[2] if len(parts) >= 3 else raw_id
            valued.append({"label": label, "value": value})

        if not valued:
            return []

        valued.sort(key=lambda x: x["value"], reverse=True)
        total = sum(p["value"] for p in valued)
        top = valued[:3]
        others_value = sum(p["value"] for p in valued[3:])

        slices = []
        for i, pos in enumerate(top):
            pct = round(pos["value"] / total * 100, 1)
            slices.append({"label": pos["label"], "pct": pct, "color": self.PIE_COLORS[i]})

        if others_value > 0:
            pct = round(others_value / total * 100, 1)
            slices.append({"label": "Others", "pct": pct, "color": self.PIE_COLORS[3]})

        return slices

    def _build_rendered_groups(self, analytics: dict) -> list[dict]:
        """Resolve metric values from analytics and return template-ready groups.

        Iterates METRICS_CONFIG, skips disabled metrics, resolves each value
        from the analytics dict (or the current_exposure sub-dict for metrics
        with ``"source": "exposure"``), and returns a list of group dicts.
        """
        exposure = analytics.get("current_exposure") or {}
        rendered_groups = []
        for group_name, metrics in METRICS_CONFIG.items():
            rendered_metrics = []
            for m in metrics:
                if not m.get("enabled", True):
                    continue
                if m.get("source") == "exposure":
                    value = exposure.get(m["key"])
                else:
                    value = analytics.get(m["key"])
                rendered_metrics.append({
                    "label": m["label"],
                    "value": value,
                    "suffix": m.get("suffix", ""),
                    "color": m.get("color", "neutral"),
                    "dash_id": "dash-" + m["key"].replace("_", "-"),
                })
            if rendered_metrics:
                rendered_groups.append({"name": group_name, "metrics": rendered_metrics})
        return rendered_groups

    def _save_html_report(self, analytics: dict) -> None:
        """Render the Jinja2 HTML dashboard and write it to output_dir."""
        try:
            template_dir = os.path.join(os.path.dirname(__file__), "templates")
            env = Environment(
                loader=FileSystemLoader(template_dir),
                autoescape=True,
            )
            template = env.get_template("dashboard.html")

            pie_chart_data = self._build_pie_chart_data(analytics)
            deliver_mode = self.config.get("deliver_mode", self.DELIVER_MODE)
            if deliver_mode in ("ftp", "netlify"):
                data_url = self.config.get("dashboard_data_url", self.DASHBOARD_DATA_FILENAME)
            else:
                data_url = self.DASHBOARD_DATA_FILENAME
            defaults = {
                "timestamp": "",
                "total_return_pct": None,
                "period_return_pct": None,
                "max_drawdown_pct": None,
                "sharpe_ratio": None,
                "volatility_annual_pct": None,
                "current_exposure": None,
                "history_length": None,
                "positions": [],
                "dashboard_data_url": data_url,
            }
            context = {**defaults, **analytics}
            context["rendered_groups"] = self._build_rendered_groups(analytics)
            context["pie_chart_data_json"] = json.dumps(pie_chart_data)
            # NOTE: max_drawdown_pct is a positive float in the analytics dict.
            # The template applies |abs and prepends a literal '-' for display.
            # Any new consumer of this dict must apply its own sign treatment.
            html = template.render(**context)

            output_dir = self.config.get("output_dir", ".")
            os.makedirs(output_dir, exist_ok=True)
            path = os.path.join(output_dir, self.DASHBOARD_FILENAME)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(html)
            abs_path = os.path.abspath(path)
        except Exception as exc:
            print(f"[PerformanceMonitoring] Could not render or write HTML report: {exc}")
            return

        errors: list[str] = []
        try:
            self._write_dashboard_json(analytics, pie_chart_data, output_dir)
        except Exception as exc:
            errors.append(f"JSON write: {exc}")

        if deliver_mode == "ftp":
            try:
                self._publish_via_ftp(output_dir, skip_json=bool(errors))
            except Exception as exc:
                print(f"[PerformanceMonitoring] Could not publish dashboard via FTP: {exc}")
        elif deliver_mode == "netlify":
            try:
                self._publish_via_netlify(output_dir, skip_json=bool(errors))
            except Exception as exc:
                print(f"[PerformanceMonitoring] Could not publish dashboard via Netlify: {exc}")
        elif deliver_mode == "github_pages":
            try:
                self._publish_via_github_pages(output_dir, skip_json=bool(errors))
            except Exception as exc:
                print(f"[PerformanceMonitoring] Could not publish dashboard_data.json via GitHub Pages: {exc}")
        elif deliver_mode == "file_only":
            print(f"[PerformanceMonitoring] Dashboard written to {abs_path}")
        else:
            try:
                self._deliver_dashboard(html, abs_path, output_dir)
            except Exception as exc:
                print(f"[PerformanceMonitoring] Could not deliver dashboard: {exc}")

        if errors:
            print(f"[PerformanceMonitoring] Completed with warnings: {'; '.join(errors)}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _write_dashboard_json(self, analytics: dict, pie_chart_data: list, output_dir: str) -> None:
        """Write the analytics payload as a JSON sidecar for the JS polling layer."""
        path = os.path.join(output_dir, self.DASHBOARD_DATA_FILENAME)
        os.makedirs(output_dir, exist_ok=True)
        payload = dict(analytics)
        payload["pie_chart_data"] = pie_chart_data
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, default=str)

    def _deliver_dashboard(self, html: str, local_path: str, output_dir: str) -> None:
        """Start an ephemeral HTTP server for this run and open the dashboard on first run.

        ``HTTPServer.allow_reuse_address = 1`` (set by the stdlib) ensures SO_REUSEADDR
        is applied before bind(), so the port is reusable across back-to-back scheduler runs.
        Binds to 127.0.0.1 (loopback only) to prevent unintended network exposure.
        """
        import webbrowser

        serve_dir = os.path.abspath(output_dir)

        class _Handler(SimpleHTTPRequestHandler):
            def __init__(self_inner, *args, **kwargs):
                super().__init__(*args, directory=serve_dir, **kwargs)

            def log_message(self_inner, format, *args):  # noqa: A002
                pass  # suppress per-request logging

        server = HTTPServer(("127.0.0.1", self.DASHBOARD_HTTP_PORT), _Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        url = f"http://localhost:{self.DASHBOARD_HTTP_PORT}/{self.DASHBOARD_FILENAME}"
        sentinel_path = os.path.join(output_dir, self.DASHBOARD_SENTINEL_FILENAME)
        try:
            if not os.path.exists(sentinel_path):
                webbrowser.open(url)
                with open(sentinel_path, "w", encoding="utf-8") as fh:
                    fh.write("")
            print(f"[PerformanceMonitoring] Dashboard at {url} — press Ctrl+C to stop")
            thread.join()
        except KeyboardInterrupt:
            pass
        finally:
            server.shutdown()

    def _publish_via_ftp(self, output_dir: str, *, skip_json: bool = False) -> None:
        """Upload the dashboard HTML and JSON sidecar to a remote host via FTP over TLS.

        Reads connection parameters from config: ``ftp_host``, ``ftp_user``,
        ``ftp_password``, ``ftp_remote_dir``.  An optional ``ftp_json_remote_dir``
        redirects the JSON sidecar to a different remote path; falls back to
        ``ftp_remote_dir`` when absent (backward-compatible).
        If any required key is absent, logs an error and returns without raising
        so the pipeline continues.
        Uploads only the two dashboard files — never the full output directory.
        When ``skip_json`` is True the JSON upload step is skipped.
        """
        required_keys = ("ftp_host", "ftp_user", "ftp_password", "ftp_remote_dir")
        missing = [k for k in required_keys if not self.config.get(k)]
        if missing:
            print(
                f"[PerformanceMonitoring] FTP publish skipped — missing config keys: "
                f"{', '.join(missing)}"
            )
            return

        host = self.config["ftp_host"]
        user = self.config["ftp_user"]
        password = self.config["ftp_password"]
        remote_dir = self.config["ftp_remote_dir"]
        # Fall back to remote_dir when ftp_json_remote_dir is absent or empty.
        json_remote_dir = self.config.get("ftp_json_remote_dir") or remote_dir

        html_local = os.path.join(output_dir, self.DASHBOARD_FILENAME)
        json_local = os.path.join(output_dir, self.DASHBOARD_DATA_FILENAME)

        html_ok = False
        json_ok = False
        try:
            with ftplib.FTP_TLS(host) as ftp:
                ftp.login(user, password)
                ftp.prot_p()  # enable encrypted data channel
                try:
                    self._ftp_upload(ftp, html_local, remote_dir)
                    html_ok = True
                except ftplib.all_errors as exc:
                    print(
                        f"[PerformanceMonitoring] FTP upload failed for HTML "
                        f"({remote_dir}): {exc}"
                    )
                if not skip_json:
                    try:
                        self._ftp_upload(ftp, json_local, json_remote_dir)
                        json_ok = True
                    except ftplib.all_errors as exc:
                        print(
                            f"[PerformanceMonitoring] FTP upload failed for JSON "
                            f"({json_remote_dir}): {exc}"
                        )
        except ftplib.all_errors as exc:
            print(f"[PerformanceMonitoring] FTP connection failed: {exc}")
            return

        if html_ok or json_ok:
            parts = []
            if html_ok:
                parts.append(f"HTML → {host}/{remote_dir.lstrip('/')}")
            if json_ok:
                parts.append(f"JSON → {host}/{json_remote_dir.lstrip('/')}")
            print(f"[PerformanceMonitoring] Dashboard published — {' | '.join(parts)}")

    def _publish_via_netlify(self, output_dir: str, *, skip_json: bool = False) -> None:
        """Deploy the dashboard to Netlify via the Files Deploy API.

        Reads ``netlify_token`` and ``netlify_site_id`` from config.
        Uses only stdlib (``hashlib``, ``urllib.request``) — no extra dependencies.

        Netlify deploy flow:
          1. POST ``/api/v1/sites/{site_id}/deploys`` with a dict mapping each
             remote path to the SHA-1 digest of its content.  Netlify responds
             with a deploy ID and a list of digests it still needs.
          2. PUT each required file to ``/api/v1/deploys/{deploy_id}/files/{path}``.
             Netlify publishes the deploy automatically once all required files
             have been received.

        Wraps all network calls in try/except so the pipeline continues on failure.
        """
        token = self.config.get("netlify_token", "")
        site_id = self.config.get("netlify_site_id", "")
        if not token or not site_id:
            missing = [k for k, v in [("netlify_token", token), ("netlify_site_id", site_id)] if not v]
            print(
                f"[PerformanceMonitoring] Netlify publish skipped — missing config keys: "
                f"{', '.join(missing)}"
            )
            return

        html_local = os.path.join(output_dir, self.DASHBOARD_FILENAME)
        json_local = os.path.join(output_dir, self.DASHBOARD_DATA_FILENAME)

        def _sha1(path: str) -> str:
            h = hashlib.sha1()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    h.update(chunk)
            return h.hexdigest()

        # Map remote path -> (sha1 digest, local path)
        file_entries: dict[str, tuple[str, str]] = {}
        try:
            file_entries["/index.html"] = (_sha1(html_local), html_local)
        except OSError as exc:
            print(f"[PerformanceMonitoring] Netlify publish skipped — cannot read HTML: {exc}")
            return
        if not skip_json:
            try:
                file_entries["/dashboard_data.json"] = (_sha1(json_local), json_local)
            except OSError as exc:
                print(f"[PerformanceMonitoring] Netlify: JSON not readable, skipping: {exc}")

        auth_header = f"Bearer {token}"

        # Step 1 — create the deploy
        files_payload = {remote: digest for remote, (digest, _) in file_entries.items()}
        body = json.dumps({"files": files_payload}).encode()
        req = urllib.request.Request(
            f"https://api.netlify.com/api/v1/sites/{site_id}/deploys",
            data=body,
            headers={
                "Authorization": auth_header,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                deploy = json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            print(f"[PerformanceMonitoring] Netlify deploy creation failed ({exc.code}): {exc.reason}")
            return
        except Exception as exc:
            print(f"[PerformanceMonitoring] Netlify deploy creation failed: {exc}")
            return

        deploy_id = deploy.get("id")
        required_hashes: list[str] = deploy.get("required", [])
        if not deploy_id:
            print("[PerformanceMonitoring] Netlify deploy creation returned no deploy ID")
            return

        # Step 2 — upload files whose digest Netlify does not yet have cached
        sha_to_entry = {digest: (remote, local) for remote, (digest, local) in file_entries.items()}
        uploaded: list[str] = []
        for sha in required_hashes:
            if sha not in sha_to_entry:
                continue
            remote_path, local_path = sha_to_entry[sha]
            try:
                with open(local_path, "rb") as fh:
                    data = fh.read()
            except OSError as exc:
                print(f"[PerformanceMonitoring] Netlify: cannot read {local_path}: {exc}")
                continue
            put_req = urllib.request.Request(
                f"https://api.netlify.com/api/v1/deploys/{deploy_id}/files{remote_path}",
                data=data,
                headers={
                    "Authorization": auth_header,
                    "Content-Type": "application/octet-stream",
                },
                method="PUT",
            )
            try:
                with urllib.request.urlopen(put_req, timeout=60):
                    pass
                uploaded.append(remote_path)
            except urllib.error.HTTPError as exc:
                print(
                    f"[PerformanceMonitoring] Netlify upload failed for {remote_path} "
                    f"({exc.code}): {exc.reason}"
                )
            except Exception as exc:
                print(f"[PerformanceMonitoring] Netlify upload failed for {remote_path}: {exc}")

        deploy_url = deploy.get("deploy_ssl_url") or deploy.get("deploy_url", "")
        if uploaded or not required_hashes:
            print(
                f"[PerformanceMonitoring] Dashboard published to Netlify — "
                f"{deploy_url or site_id}"
            )

    def _publish_via_github_pages(self, output_dir: str, *, skip_json: bool = False) -> None:
        """Push dashboard_data.json to a GitHub Pages repository via the Contents API.

        Reads ``github_pat``, ``github_repo``, and optionally ``github_json_path``
        and ``github_branch`` from config.  Uses only stdlib — no extra dependencies.
        The file is created on first run and updated (with its current SHA) on all
        subsequent runs.  When ``skip_json`` is True the upload step is skipped.
        """
        if skip_json:
            print("[PerformanceMonitoring] GitHub Pages: skipping JSON upload (JSON write failed).")
            return

        token = self.config.get("github_pat", "").strip()
        repo = self.config.get("github_repo", "").strip()
        file_path = self.config.get("github_json_path", "dashboard_data.json").strip()
        branch = self.config.get("github_branch", "main").strip()
        commit_message = self.config.get(
            "github_commit_message", "chore: update dashboard data"
        )

        if not token or not repo:
            missing = [k for k, v in [("github_pat", token), ("github_repo", repo)] if not v]
            print(
                f"[PerformanceMonitoring] GitHub Pages publish skipped — "
                f"missing config keys: {', '.join(missing)}"
            )
            return

        local_json = os.path.join(output_dir, self.DASHBOARD_DATA_FILENAME)
        try:
            with open(local_json, "rb") as fh:
                content_bytes = fh.read()
        except OSError as exc:
            print(f"[PerformanceMonitoring] GitHub Pages: cannot read JSON: {exc}")
            return

        content_b64 = base64.b64encode(content_bytes).decode()
        auth_header = f"Bearer {token}"
        api_url = f"https://api.github.com/repos/{repo}/contents/{file_path}"
        common_headers = {
            "Authorization": auth_header,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        # Step 1 — fetch the current file SHA (required for updates; absent for creates).
        sha: str | None = None
        get_req = urllib.request.Request(
            f"{api_url}?ref={branch}",
            headers=common_headers,
            method="GET",
        )
        try:
            with urllib.request.urlopen(get_req, timeout=15) as resp:
                sha = json.loads(resp.read()).get("sha")
        except urllib.error.HTTPError as exc:
            if exc.code != 404:
                print(
                    f"[PerformanceMonitoring] GitHub Pages: failed to read existing file "
                    f"({exc.code}): {exc.reason}"
                )
                return
            # 404 → file does not exist yet; create it without a SHA.
        except Exception as exc:
            print(f"[PerformanceMonitoring] GitHub Pages: failed to read existing file: {exc}")
            return

        # Step 2 — PUT to create or update the file.
        put_body: dict = {
            "message": commit_message,
            "content": content_b64,
            "branch": branch,
        }
        if sha:
            put_body["sha"] = sha

        put_headers = {**common_headers, "Content-Type": "application/json"}
        put_req = urllib.request.Request(
            api_url,
            data=json.dumps(put_body).encode(),
            headers=put_headers,
            method="PUT",
        )
        try:
            with urllib.request.urlopen(put_req, timeout=30) as resp:
                result = json.loads(resp.read())
            commit_url = result.get("commit", {}).get("html_url", "")
            print(
                f"[PerformanceMonitoring] dashboard_data.json pushed to GitHub Pages"
                + (f" — {commit_url}" if commit_url else "")
            )
        except urllib.error.HTTPError as exc:
            print(
                f"[PerformanceMonitoring] GitHub Pages push failed ({exc.code}): {exc.reason}"
            )
        except Exception as exc:
            print(f"[PerformanceMonitoring] GitHub Pages push failed: {exc}")

    @staticmethod
    def _ftp_upload(ftp: ftplib.FTP_TLS, local_path: str, remote_dir: str) -> None:
        """Normalise remote_dir to an absolute POSIX path client-side before issuing CWD.

        Eliminates relative-path drift between sequential calls on a shared FTP connection
        without requiring a server round-trip to reset to '/'.  Raises ``ftplib.error_perm``
        with a descriptive message — including the normalised absolute path — on directory
        errors so callers can log exactly what path was sent to the server.
        """
        abs_dir = posixpath.normpath(posixpath.join("/", remote_dir.lstrip("/")))
        try:
            ftp.cwd(abs_dir)
        except ftplib.error_perm as exc:
            raise ftplib.error_perm(
                f"Remote directory does not exist or is inaccessible: "
                f"{abs_dir!r} ({exc})"
            ) from exc
        filename = os.path.basename(local_path)
        with open(local_path, "rb") as fh:
            ftp.storbinary(f"STOR {filename}", fh)

    @staticmethod
    def _negate_drawdown(value) -> float | None:
        """Return drawdown as a negative number for display, or None."""
        if value is None:
            return None
        return -abs(value)

    @staticmethod
    def _int_or_none(value) -> str | None:
        """Format an integer value without decimals, or return None."""
        if value is None:
            return None
        return str(int(value))

