"""Standalone read-only diagnostic tool that produces a DataSource Scope Report.

Run from the repository root:
    python data/input/datasource_scoper.py

Returns a structured dict (three sections A-C) and prints it to stdout.
Makes no writes, no broker calls, and no pipeline imports.
"""

import json
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Path constants — exact values mandated by the spec.
# ---------------------------------------------------------------------------
UNIVERSE_PATH = "data/input/universe.json"
SERIES_FILE   = "data/input/universe_series.xlsx"
HISTORIC_DIR  = "data/input/historic_series"
SHEET_NAMES   = ("mid_close",)


# ---------------------------------------------------------------------------
# Section A — Universe Scope
# ---------------------------------------------------------------------------

def _load_section_a(universe_path: str) -> dict:
    """Parse universe.json and return Section-A summary fields."""
    result = {
        "error": None,
        "total_instruments": 0,
        "verified_count": 0,
        "candidate_count": 0,
        "verified_epics": [],
        "candidate_epics": [],
        "unknown_status": [],
        "malformed_entries": 0,
    }

    try:
        with open(universe_path, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        result["error"] = f"File not found: {universe_path}"
        return result
    except Exception as exc:
        result["error"] = f"Failed to read {universe_path}: {exc}"
        return result

    instruments = data.get("instruments", [])
    for entry in instruments:
        epic = entry.get("epic", "")
        if not epic:
            result["malformed_entries"] += 1
            continue
        status = entry.get("status", "")
        if status == "verified":
            result["verified_epics"].append(epic)
            result["verified_count"] += 1
        elif status == "candidate":
            result["candidate_epics"].append(epic)
            result["candidate_count"] += 1
        else:
            result["unknown_status"].append(epic)

    result["total_instruments"] = (
        result["verified_count"] + result["candidate_count"] + len(result["unknown_status"])
    )
    return result


# ---------------------------------------------------------------------------
# Section B — Series Scope
# ---------------------------------------------------------------------------

def _load_section_b(series_file: str, historic_dir: str, sheet_names: tuple) -> dict:
    """Read universe_series.xlsx and scan historic_series/, returning Section-B fields."""
    result = {
        "error": None,
        "series_epics": [],
        "series_epic_count": 0,
        "date_range": {},
        "historic_file_count": 0,
        "historic_files_present": [],
    }

    # --- Scan historic_series directory (always attempted) ---
    historic_files = _list_historic_files(historic_dir)
    result["historic_file_count"] = len(historic_files)
    result["historic_files_present"] = historic_files

    # --- Read xlsx ---
    try:
        import pandas as pd
    except ImportError as exc:
        result["error"] = f"pandas not available: {exc}"
        return result

    if not os.path.exists(series_file):
        result["error"] = f"File not found: {series_file}"
        return result

    sheet = sheet_names[0]  # "mid_close"
    try:
        df = pd.read_excel(
            series_file,
            sheet_name=sheet,
            index_col=0,
            engine="openpyxl",
        )
        df.index = pd.to_datetime(df.index, utc=True)
        epics = [str(col) for col in df.columns]
        result["series_epics"] = sorted(epics)
        result["series_epic_count"] = len(epics)
        if len(df.index) > 0:
            result["date_range"] = {
                "first": df.index.min(),
                "last": df.index.max(),
            }
        else:
            result["date_range"] = {"first": None, "last": None}
    except Exception as exc:
        result["error"] = f"Sheet '{sheet}' error: {exc}"

    return result


def _list_historic_files(historic_dir: str) -> list:
    """Return filenames in historic_dir, excluding .gitkeep. Returns [] if absent."""
    path = Path(historic_dir)
    if not path.exists():
        return []
    return sorted(
        f.name
        for f in path.iterdir()
        if f.is_file() and f.name != ".gitkeep"
    )


# ---------------------------------------------------------------------------
# Section C — Discrepancy Analysis
# ---------------------------------------------------------------------------

def _base(epic: str) -> str:
    """Return the 3-segment dot prefix of an epic string (e.g. 'IX.D.FTSE')."""
    return ".".join(epic.split(".")[:3])


def _compute_section_c(section_a: dict, section_b: dict) -> dict:
    """Derive discrepancy fields from Sections A and B."""
    universe_epics = set(
        section_a.get("verified_epics", []) + section_a.get("candidate_epics", [])
        + section_a.get("unknown_status", [])
    )
    verified_epics = set(section_a.get("verified_epics", []))
    series_epics = set(section_b.get("series_epics", []))

    in_universe_not_in_series = sorted(universe_epics - series_epics)
    in_series_not_in_universe = sorted(series_epics - universe_epics)
    verified_not_in_series = sorted(verified_epics - series_epics)
    orphans = _find_variant_orphans(series_epics, universe_epics)

    return {
        "in_universe_not_in_series": in_universe_not_in_series,
        "in_series_not_in_universe": in_series_not_in_universe,
        "verified_not_in_series": verified_not_in_series,
        "same_base_variant_orphans": orphans,
    }


def _find_variant_orphans(series_epics: set, universe_epics: set) -> list:
    """Find series epics whose 3-segment base matches a universe epic's base but the exact epic differs."""
    universe_base_map: dict = {}
    for epic in universe_epics:
        universe_base_map[_base(epic)] = epic

    orphans = []
    for series_epic in sorted(series_epics):
        if series_epic in universe_epics:
            continue  # exact match — not an orphan
        base = _base(series_epic)
        if base in universe_base_map:
            orphans.append({
                "series_epic": series_epic,
                "universe_epic": universe_base_map[base],
            })
    return orphans


# ---------------------------------------------------------------------------
# Report assembly and printing
# ---------------------------------------------------------------------------

def _print_report(report: dict) -> None:
    """Print the Scope Report to stdout in human-readable form."""
    a = report["section_a"]
    b = report["section_b"]
    c = report["section_c"]

    print("=" * 60)
    print("=== DataSource Scope Report ===")
    print("=" * 60)

    # --- Section A ---
    print("\n--- A. Universe Scope (universe.json) ---")
    if a.get("error"):
        print(f"  ERROR: {a['error']}")
    else:
        print(f"  total_instruments : {a['total_instruments']}")
        print(f"  verified_count    : {a['verified_count']}")
        print(f"  candidate_count   : {a['candidate_count']}")
        print(f"  malformed_entries : {a['malformed_entries']}")
        print(f"  verified_epics    : {a['verified_epics']}")
        print(f"  candidate_epics   : {a['candidate_epics']}")
        if a.get("unknown_status"):
            print(f"  unknown_status    : {a['unknown_status']}")

    # --- Section B ---
    print("\n--- B. Series Scope (universe_series.xlsx) ---")
    if b.get("error"):
        print(f"  ERROR: {b['error']}")
    print(f"  series_epic_count       : {b['series_epic_count']}")
    print(f"  series_epics            : {b['series_epics']}")
    print(f"  historic_file_count     : {b['historic_file_count']}")
    print(f"  historic_files_present  : {b['historic_files_present']}")
    dr = b.get("date_range", {})
    if dr:
        print(f"  date_range.first        : {dr.get('first')}")
        print(f"  date_range.last         : {dr.get('last')}")

    # --- Section C ---
    print("\n--- C. Discrepancy Analysis ---")
    print(f"  in_universe_not_in_series : {c['in_universe_not_in_series']}")
    print(f"  in_series_not_in_universe : {c['in_series_not_in_universe']}")
    print(f"  verified_not_in_series    : {c['verified_not_in_series']}")
    print(f"  same_base_variant_orphans :")
    if c["same_base_variant_orphans"]:
        for o in c["same_base_variant_orphans"]:
            print(f"    series={o['series_epic']}  universe={o['universe_epic']}")
    else:
        print("    (none)")

    print("\n" + "=" * 60)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def build_report(
    universe_path: str = UNIVERSE_PATH,
    series_file: str = SERIES_FILE,
    historic_dir: str = HISTORIC_DIR,
    sheet_names: tuple = SHEET_NAMES,
) -> dict:
    """Build and return the full Scope Report as a dict.

    Parameters allow override for testing; defaults match the path constants.
    """
    section_a = _load_section_a(universe_path)
    section_b = _load_section_b(series_file, historic_dir, sheet_names)
    section_c = _compute_section_c(section_a, section_b)

    return {
        "section_a": section_a,
        "section_b": section_b,
        "section_c": section_c,
    }


if __name__ == "__main__":
    report = build_report()
    _print_report(report)
