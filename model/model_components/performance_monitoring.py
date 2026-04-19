"""
Tradinator — Performance Monitoring.

Presentation layer that takes analytics output and formats it for human
consumption.  Phase 1: prints a formatted summary table to stdout and
optionally writes the report to a text file.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import os
import webbrowser

from jinja2 import Environment, FileSystemLoader


class PerformanceMonitoring:
    """Format and display a human-readable performance report."""

    DISPLAY_WIDTH = 60
    SAVE_REPORT = True
    REPORT_FILENAME = "performance_report.txt"

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
            html = template.render(**context)

            output_dir = self.config.get("output_dir", ".")
            os.makedirs(output_dir, exist_ok=True)
            path = os.path.join(output_dir, "performance_dashboard.html")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(html)
            abs_path = os.path.abspath(path)
            webbrowser.open(f"file:///{abs_path}")
            print(f"[PerformanceMonitoring] Dashboard opened in browser: {abs_path}")
        except Exception as exc:
            print(f"[PerformanceMonitoring] Could not save HTML report: {exc}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

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
