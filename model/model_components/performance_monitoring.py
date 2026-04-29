"""
Tradinator — Performance Monitoring.

Presentation layer that takes analytics output and formats it for human
consumption.  Phase 1: prints a formatted summary table to stdout and
optionally writes the report to a text file.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import json
import os
import socket
import threading
import time
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler

from jinja2 import Environment, FileSystemLoader


class PerformanceMonitoring:
    """Format and display a human-readable performance report."""

    DISPLAY_WIDTH = 60
    SAVE_REPORT = True
    REPORT_FILENAME = "performance_report.txt"
    DASHBOARD_FILENAME = "performance_dashboard.html"
    DASHBOARD_DATA_FILENAME = "dashboard_data.json"
    DASHBOARD_SENTINEL_FILENAME = ".dashboard_opened"
    DASHBOARD_HTTP_PORT = 8742
    DASHBOARD_SERVER_LINGER_SECONDS = 4

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

    def _save_html_report(self, analytics: dict) -> None:
        """Render the Jinja2 HTML dashboard and write it to output_dir."""
        try:
            template_dir = os.path.join(os.path.dirname(__file__), "templates")
            env = Environment(
                loader=FileSystemLoader(template_dir),
                autoescape=True,
            )
            template = env.get_template("dashboard.html")

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
            context = {**defaults, **analytics}
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

        try:
            self._write_dashboard_json(analytics, output_dir)
            self._deliver_dashboard(html, abs_path, output_dir)
        except Exception as exc:
            print(f"[PerformanceMonitoring] Could not deliver dashboard: {exc}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _write_dashboard_json(self, analytics: dict, output_dir: str) -> None:
        """Write the analytics payload as a JSON sidecar for the JS polling layer."""
        path = os.path.join(output_dir, self.DASHBOARD_DATA_FILENAME)
        os.makedirs(output_dir, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(analytics, fh, default=str)

    def _deliver_dashboard(self, html: str, local_path: str, output_dir: str) -> None:
        """Start an ephemeral HTTP server for this run and open the dashboard on first run."""
        serve_dir = os.path.abspath(output_dir)

        class _Handler(SimpleHTTPRequestHandler):
            def __init__(self_inner, *args, **kwargs):
                super().__init__(*args, directory=serve_dir, **kwargs)

            def log_message(self_inner, format, *args):  # noqa: A002
                pass  # suppress per-request logging

        server = HTTPServer(("", self.DASHBOARD_HTTP_PORT), _Handler)
        server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        url = f"http://localhost:{self.DASHBOARD_HTTP_PORT}/{self.DASHBOARD_FILENAME}"
        sentinel_path = os.path.join(output_dir, self.DASHBOARD_SENTINEL_FILENAME)
        if not os.path.exists(sentinel_path):
            with open(sentinel_path, "w", encoding="utf-8") as fh:
                fh.write("")
            webbrowser.open(url)
            print(f"[PerformanceMonitoring] Dashboard opened at {url}")
        else:
            print(f"[PerformanceMonitoring] Dashboard updated at {url}")

        time.sleep(self.DASHBOARD_SERVER_LINGER_SECONDS)
        server.shutdown()

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
