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
import webbrowser

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
        """
        positions = analytics.get("positions") or []
        valued = []
        for pos in positions:
            level = float(pos.get("level") or 0)
            size = abs(float(pos.get("size") or 0))
            value = size * level
            if value > 0:
                # Use the third dot-segment of the epic as a compact label
                # (e.g. "FTSE" from "IX.D.FTSE.DAILY.IP"), falling back to
                # the full instrument_id when the format does not match.
                raw_id = pos.get("instrument_id") or "Unknown"
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
            }
            context = {**defaults, **analytics}
            context["rendered_groups"] = self._build_rendered_groups(analytics)
            context["pie_chart_data_json"] = json.dumps(self._build_pie_chart_data(analytics))

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

