"""IG API Calls Validator — challenges the scoper inventory for completeness and correctness.

Reads skills/ig_api_calls.csv (produced by ig_api_scoper.py), applies all
validation rules defined in skills/Validator_IG_API_Calls.md, and writes
skills/ig_api_calls_validated.csv with per-row status columns and a summary.

Run from the repository root:
    python skills/ig_api_validator.py

Guardrails:
    - Reads only skills/ig_api_calls.csv.
    - Writes only skills/ig_api_calls_validated.csv.
    - No broker calls, no pipeline imports.
    - Never raises an unhandled exception.
"""

import csv
import os
import sys

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SKILLS_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(SKILLS_DIR, "ig_api_calls.csv")
OUTPUT_CSV = os.path.join(SKILLS_DIR, "ig_api_calls_validated.csv")

# ---------------------------------------------------------------------------
# Validation rules
# ---------------------------------------------------------------------------

# Rule 1: expected (method, file_suffix) pairs that MUST appear.
EXPECTED_METHODS: list[tuple[str, str]] = [
    ("create_session", "ig_adapter.py"),
    ("create_session", "discover_universe.py"),
    ("fetch_accounts", "ig_adapter.py"),
    ("fetch_open_positions", "ig_adapter.py"),
    ("fetch_market_by_epic", "ig_adapter.py"),
    ("fetch_market_by_epic", "discover_universe.py"),
    ("fetch_historical_prices_by_epic_and_num_points", "ig_adapter.py"),
    ("fetch_historical_prices_by_epic_and_num_points", "discover_universe.py"),
    ("fetch_historical_prices_by_epic_and_date_range", "ig_adapter.py"),
    ("search_markets", "discover_universe.py"),
    ("create_open_position", "ig_adapter.py"),
    ("close_open_position", "ig_adapter.py"),
    ("fetch_deal_by_deal_reference", "ig_adapter.py"),
]

# Rule 3: categories that require retry coverage.
RETRY_REQUIRED_CATEGORIES = {"market_data", "account", "order"}

# Rule 4: valid known categories.
KNOWN_CATEGORIES = {"session", "account", "market_data", "order", "other"}

# Output CSV columns
INPUT_FIELDS = ["file", "function", "line", "ig_method", "category", "has_retry", "notes"]
OUTPUT_FIELDS = INPUT_FIELDS + ["validation_status", "remediation_needed", "validator_notes"]


# ---------------------------------------------------------------------------
# Validation logic
# ---------------------------------------------------------------------------

def _validate_row(row: dict, seen_positions: set[tuple]) -> tuple[str, str]:
    """Apply per-row rules.  Returns (status, validator_notes)."""
    issues: list[str] = []

    # Rule 5 — line number sanity
    try:
        line_num = int(row.get("line", 0))
        if line_num <= 0:
            issues.append("INVALID_LINE: line <= 0")
    except (ValueError, TypeError):
        issues.append("INVALID_LINE: non-integer line")

    # Rule 2 — duplicate detection
    pos_key = (row.get("file", ""), row.get("line", ""))
    if pos_key in seen_positions:
        issues.append("DUPLICATE: same (file, line)")
    else:
        seen_positions.add(pos_key)

    # Rule 4 — category consistency
    cat = row.get("category", "")
    if cat not in KNOWN_CATEGORIES:
        issues.append(f"UNKNOWN_CATEGORY: '{cat}'")

    # Rule 3 — retry coverage
    has_retry_raw = row.get("has_retry", "False")
    has_retry = has_retry_raw.strip().lower() in ("true", "1", "yes")
    if not has_retry and cat in RETRY_REQUIRED_CATEGORIES:
        issues.append(
            "NEEDS_RETRY: no retry protection for "
            f"{cat} call to {row.get('ig_method', '?')}"
        )

    if issues:
        # Return the most-important status label (priority order).
        priority = ["INVALID_LINE", "DUPLICATE", "UNKNOWN_CATEGORY", "NEEDS_RETRY"]
        status = next(
            (s for s in priority if any(s in issue for issue in issues)),
            issues[0].split(":")[0],
        )
        return status, "; ".join(issues)

    return "OK", ""


def _check_missing_methods(rows: list[dict]) -> list[str]:
    """Return list of human-readable descriptions of missing expected methods."""
    missing = []
    for method, file_suffix in EXPECTED_METHODS:
        found = any(
            r.get("ig_method") == method and r.get("file", "").endswith(file_suffix)
            for r in rows
        )
        if not found:
            missing.append(f"{method} in *{file_suffix}")
    return missing


# ---------------------------------------------------------------------------
# CSV I/O
# ---------------------------------------------------------------------------

def _load_csv(path: str) -> list[dict] | None:
    """Load the input CSV.  Returns None on failure."""
    if not os.path.isfile(path):
        print(f"[validator] ERROR: input CSV not found: {path}", file=sys.stderr)
        return None
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            return list(csv.DictReader(fh))
    except Exception as exc:
        print(f"[validator] ERROR: cannot read {path} — {exc}", file=sys.stderr)
        return None


def _write_csv(rows: list[dict], summary_lines: list[str], output_path: str) -> None:
    """Write validated rows plus a comment summary."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        # Append summary as comment lines.
        for line in summary_lines:
            fh.write(f"# {line}\n")
    print(f"[validator] Wrote {len(rows)} row(s) to {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(input_csv: str = INPUT_CSV, output_csv: str = OUTPUT_CSV) -> str:
    """Validate the scoper CSV.  Returns overall verdict ('PASS' or 'FAIL')."""
    rows = _load_csv(input_csv)
    if rows is None:
        # Write a minimal error CSV so downstream consumers get something.
        error_row = {f: "" for f in OUTPUT_FIELDS}
        error_row["validation_status"] = "ERROR"
        error_row["remediation_needed"] = "True"
        error_row["validator_notes"] = f"Input CSV not found or unreadable: {input_csv}"
        _write_csv([error_row], ["VERDICT: FAIL", "Reason: input CSV missing"], output_csv)
        return "FAIL"

    seen_positions: set[tuple] = set()
    validated_rows: list[dict] = []

    for row in rows:
        status, notes = _validate_row(row, seen_positions)
        out_row = dict(row)
        out_row["validation_status"] = status
        out_row["remediation_needed"] = str(status != "OK")
        out_row["validator_notes"] = notes
        validated_rows.append(out_row)

    # Sort by file then line.
    def _sort_key(r):
        try:
            return (r.get("file", ""), int(r.get("line", 0)))
        except ValueError:
            return (r.get("file", ""), 0)

    validated_rows.sort(key=_sort_key)

    # Rule 1 — check for missing expected methods.
    missing = _check_missing_methods(rows)

    # Build summary.
    total = len(validated_rows)
    needs_retry = sum(1 for r in validated_rows if r["validation_status"] == "NEEDS_RETRY")
    duplicates = sum(1 for r in validated_rows if r["validation_status"] == "DUPLICATE")
    ok_count = sum(1 for r in validated_rows if r["validation_status"] == "OK")

    verdict = "PASS" if (not missing and needs_retry == 0 and duplicates == 0) else "FAIL"

    summary_lines = [
        f"VERDICT: {verdict}",
        f"Total rows: {total}",
        f"OK: {ok_count}",
        f"NEEDS_RETRY: {needs_retry}",
        f"DUPLICATE: {duplicates}",
    ]
    if missing:
        summary_lines.append(f"Missing expected methods ({len(missing)}):")
        for m in missing:
            summary_lines.append(f"  - {m}")
    else:
        summary_lines.append("Missing expected methods: none")

    # Print summary to stdout.
    print("\n[validator] === Validation Summary ===")
    for line in summary_lines:
        print(f"  {line}")

    # Print rows needing remediation.
    remediation_rows = [r for r in validated_rows if r["remediation_needed"] == "True"]
    if remediation_rows:
        print("\n[validator] Rows requiring remediation:")
        for r in remediation_rows:
            print(
                f"  [{r['validation_status']}] "
                f"{r['file']}:{r['line']} "
                f"{r['function']}() → {r['ig_method']} "
                f"— {r['validator_notes']}"
            )

    _write_csv(validated_rows, summary_lines, output_csv)
    print(f"\n[validator] Verdict: {verdict}")
    return verdict


if __name__ == "__main__":
    verdict = run()
    sys.exit(0 if verdict == "PASS" else 1)
