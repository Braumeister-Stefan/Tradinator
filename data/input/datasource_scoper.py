"""Standalone read-only diagnostic tool that produces a DataSource Scope Report.

Run from the repository root:
    python data/input/datasource_scoper.py

Returns a structured dict (four sections A-D) and prints it to stdout.
Makes no writes, no broker calls, and no pipeline imports.
"""

import json
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Path constants — exact values mandated by the spec.
# ---------------------------------------------------------------------------
UNIVERSE_PATH   = "data/input/universe.json"
CANDIDATES_PATH = "data/input/universe_candidates.json"
SERIES_FILE     = "data/input/universe_series.xlsx"
HISTORIC_DIR    = "data/input/historic_series"
SHEET_NAMES     = ("mid_close", "bid_close", "mid_open")


# ---------------------------------------------------------------------------
# Section A — Universe Scope
# ---------------------------------------------------------------------------

def _load_section_a(universe_path: str) -> dict:
    """Parse universe.json and return Section-A summary fields."""
    result = {
        "error": None,
        "total_valid": 0,
        "valid_epics": [],
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
        else:
            result["valid_epics"].append(epic)

    result["total_valid"] = len(result["valid_epics"])
    return result


# ---------------------------------------------------------------------------
# Section B — Candidates Scope
# ---------------------------------------------------------------------------

_T1_FAIL_STATUSES = {"EPIC_NOT_RECOGNIZED", "DEALING_DISABLED", "API_ERROR"}


def _load_section_b(candidates_path: str) -> dict:
    """Parse universe_candidates.json and return Section-B summary fields."""
    result = {
        "error": None,
        "total_candidates": 0,
        "t1_pass_count": 0,
        "t1_fail_count": 0,
        "t1_untested_count": 0,
        "pending_t2_count": 0,
        "t2_yes_count": 0,
        "t2_no_count": 0,
        "fully_valid_count": 0,
        "epic_not_recognized": [],
        "dealing_disabled": [],
        "api_error": [],
        "last_discover_run": None,
    }

    try:
        with open(candidates_path, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        result["error"] = f"File not found: {candidates_path}"
        return result
    except Exception as exc:
        result["error"] = f"Failed to read {candidates_path}: {exc}"
        return result

    result["last_discover_run"] = data.get("last_discover_run")
    candidates = data.get("candidates", [])
    result["total_candidates"] = len(candidates)

    for c in candidates:
        epic = c.get("epic", "")
        t1 = c.get("t1_status", "UNTESTED")
        t2 = c.get("t2_status", "NEVER_TRIED")

        if t1 == "PASS":
            result["t1_pass_count"] += 1
        elif t1 == "UNTESTED":
            result["t1_untested_count"] += 1
        elif t1 in _T1_FAIL_STATUSES:
            result["t1_fail_count"] += 1

        if t1 == "EPIC_NOT_RECOGNIZED":
            result["epic_not_recognized"].append(epic)
        elif t1 == "DEALING_DISABLED":
            result["dealing_disabled"].append(epic)
        elif t1 == "API_ERROR":
            result["api_error"].append(epic)

        if t2 == "PENDING_T2":
            result["pending_t2_count"] += 1
        elif t2 == "YES":
            result["t2_yes_count"] += 1
        elif t2 == "NO":
            result["t2_no_count"] += 1

        if c.get("valid", False):
            result["fully_valid_count"] += 1

    return result


# ---------------------------------------------------------------------------
# Section C — Series Scope
# ---------------------------------------------------------------------------

def _load_section_c(series_file: str, historic_dir: str, sheet_names: tuple) -> dict:
    """Read universe_series.xlsx and scan historic_series/, returning Section-C fields."""
    result = {
        "error": None,
        "series_epics": [],
        "series_epic_count": 0,
        "date_range": {},
        "sheets_have_consistent_date_range": False,
        "sheets_fully_consistent": False,
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

    sheets_epics: dict[str, list[str]] = {}
    date_ranges: dict[str, dict] = {}

    for sheet in sheet_names:
        try:
            df = pd.read_excel(
                series_file,
                sheet_name=sheet,
                index_col=0,
                engine="openpyxl",
            )
            df.index = pd.to_datetime(df.index, utc=True)
            epics = [str(col) for col in df.columns]
            sheets_epics[sheet] = epics
            if len(df.index) > 0:
                date_ranges[sheet] = {
                    "first": df.index.min(),
                    "last": df.index.max(),
                }
            else:
                date_ranges[sheet] = {"first": None, "last": None}
        except Exception as exc:
            result["error"] = (result["error"] or "") + f"Sheet '{sheet}' error: {exc}; "
            sheets_epics[sheet] = []
            date_ranges[sheet] = {"first": None, "last": None}

    result["date_range"] = date_ranges

    # Union of all epics across sheets.
    all_epics: set[str] = set()
    for epics in sheets_epics.values():
        all_epics.update(epics)
    result["series_epics"] = sorted(all_epics)
    result["series_epic_count"] = len(all_epics)

    # Consistency checks.
    result["sheets_have_consistent_date_range"] = _date_ranges_consistent(date_ranges, sheet_names)
    epic_sets = [set(sheets_epics[s]) for s in sheet_names]
    result["sheets_fully_consistent"] = len(set(frozenset(s) for s in epic_sets)) == 1

    return result


def _list_historic_files(historic_dir: str) -> list[str]:
    """Return filenames in historic_dir, excluding .gitkeep. Returns [] if absent."""
    path = Path(historic_dir)
    if not path.exists():
        return []
    return sorted(
        f.name
        for f in path.iterdir()
        if f.is_file() and f.name != ".gitkeep"
    )


def _date_ranges_consistent(date_ranges: dict, sheet_names: tuple) -> bool:
    """Return True if all sheets share identical first/last datetimes."""
    firsts = [date_ranges.get(s, {}).get("first") for s in sheet_names]
    lasts  = [date_ranges.get(s, {}).get("last")  for s in sheet_names]
    return len(set(str(v) for v in firsts)) == 1 and len(set(str(v) for v in lasts)) == 1


# ---------------------------------------------------------------------------
# Section D — Discrepancy Analysis
# ---------------------------------------------------------------------------

def _base(epic: str) -> str:
    """Return the 3-segment dot prefix of an epic string (e.g. 'IX.D.FTSE')."""
    return ".".join(epic.split(".")[:3])


def _compute_section_d(
    section_a: dict,
    section_b: dict,
    section_c: dict,
) -> dict:
    """Derive discrepancy fields from Sections A, B, C."""
    universe_epics = set(section_a.get("valid_epics", []))
    series_epics   = set(section_c.get("series_epics", []))

    candidates = []  # fallback: empty if section_b failed
    if not section_b.get("error"):
        # Re-read is not required; we compute from section_b fields only.
        # We need the raw candidate list for pending/valid checks — but
        # section_b only carries counts, not the list. We therefore derive
        # from the available aggregates and the original candidate data below.
        pass

    # Load candidate records (needed for pending/valid per-epic resolution).
    pending_epics: set[str] = set()
    valid_epics_candidates: set[str] = set()
    try:
        with open(CANDIDATES_PATH, encoding="utf-8") as fh:
            raw = json.load(fh)
        for c in raw.get("candidates", []):
            epic = c.get("epic", "")
            if c.get("t2_status") == "PENDING_T2":
                pending_epics.add(epic)
            if c.get("valid", False):
                valid_epics_candidates.add(epic)
    except Exception:
        pass  # If unavailable, these sets remain empty.

    in_universe_not_in_series = sorted(universe_epics - series_epics)
    in_series_not_in_universe = sorted(series_epics - universe_epics)
    pending_not_in_series     = sorted(pending_epics - series_epics)
    valid_not_in_series       = sorted(valid_epics_candidates - series_epics)

    orphans = _find_variant_orphans(series_epics, universe_epics)

    return {
        "in_universe_not_in_series": in_universe_not_in_series,
        "in_series_not_in_universe": in_series_not_in_universe,
        "pending_not_in_series":     pending_not_in_series,
        "valid_not_in_series":       valid_not_in_series,
        "same_base_variant_orphans": orphans,
    }


def _find_variant_orphans(
    series_epics: set[str],
    universe_epics: set[str],
) -> list[dict]:
    """Find series epics whose 3-segment base matches a universe epic's base but the exact epic differs."""
    # Build base → universe epic mapping.
    universe_base_map: dict[str, str] = {}
    for epic in universe_epics:
        universe_base_map[_base(epic)] = epic

    orphans = []
    for series_epic in sorted(series_epics):
        if series_epic in universe_epics:
            continue  # exact match — not an orphan
        base = _base(series_epic)
        if base in universe_base_map:
            orphans.append({
                "series_epic":   series_epic,
                "universe_epic": universe_base_map[base],
                "base":          base,
            })
    return orphans


# ---------------------------------------------------------------------------
# Report assembly and printing
# ---------------------------------------------------------------------------

def _fmt_list(items: list, indent: int = 4) -> str:
    """Format a list as an indented multi-line string."""
    pad = " " * indent
    if not items:
        return "[]"
    return "[\n" + "".join(f"{pad}{item}\n" for item in items) + "  ]"


def _print_report(report: dict) -> None:
    """Print the Scope Report to stdout in human-readable form."""
    a = report["section_a"]
    b = report["section_b"]
    c = report["section_c"]
    d = report["section_d"]

    print("=" * 60)
    print("=== DataSource Scope Report ===")
    print("=" * 60)

    # --- Section A ---
    print("\n--- A. Universe Scope (universe.json) ---")
    if a.get("error"):
        print(f"  ERROR: {a['error']}")
    else:
        print(f"  total_valid       : {a['total_valid']}")
        print(f"  malformed_entries : {a['malformed_entries']}")
        print(f"  valid_epics       : {a['valid_epics']}")

    # --- Section B ---
    print("\n--- B. Candidates Scope (universe_candidates.json) ---")
    if b.get("error"):
        print(f"  ERROR: {b['error']}")
    else:
        print(f"  total_candidates  : {b['total_candidates']}")
        print(f"  t1_pass_count     : {b['t1_pass_count']}")
        print(f"  t1_fail_count     : {b['t1_fail_count']}")
        print(f"  t1_untested_count : {b['t1_untested_count']}")
        print(f"  pending_t2_count  : {b['pending_t2_count']}")
        print(f"  t2_yes_count      : {b['t2_yes_count']}")
        print(f"  t2_no_count       : {b['t2_no_count']}")
        print(f"  fully_valid_count : {b['fully_valid_count']}")
        print(f"  epic_not_recognized : {b['epic_not_recognized']}")
        print(f"  dealing_disabled    : {b['dealing_disabled']}")
        print(f"  api_error           : {b['api_error']}")
        print(f"  last_discover_run   : {b['last_discover_run']}")

    # --- Section C ---
    print("\n--- C. Series Scope (universe_series.xlsx) ---")
    if c.get("error"):
        print(f"  ERROR: {c['error']}")
    print(f"  series_epic_count              : {c['series_epic_count']}")
    print(f"  series_epics                   : {c['series_epics']}")
    print(f"  sheets_have_consistent_date_range : {c['sheets_have_consistent_date_range']}")
    print(f"  sheets_fully_consistent           : {c['sheets_fully_consistent']}")
    print(f"  historic_file_count               : {c['historic_file_count']}")
    print(f"  historic_files_present            : {c['historic_files_present']}")
    print("  date_range:")
    for sheet, rng in c.get("date_range", {}).items():
        print(f"    {sheet}: first={rng.get('first')}  last={rng.get('last')}")

    # --- Section D ---
    print("\n--- D. Discrepancy Analysis ---")
    print(f"  in_universe_not_in_series : {d['in_universe_not_in_series']}")
    print(f"  in_series_not_in_universe : {d['in_series_not_in_universe']}")
    print(f"  pending_not_in_series     : {d['pending_not_in_series']}")
    print(f"  valid_not_in_series       : {d['valid_not_in_series']}")
    print(f"  same_base_variant_orphans :")
    if d["same_base_variant_orphans"]:
        for o in d["same_base_variant_orphans"]:
            print(f"    series={o['series_epic']}  universe={o['universe_epic']}  base={o['base']}")
    else:
        print("    (none)")

    print("\n" + "=" * 60)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def build_report(
    universe_path:   str = UNIVERSE_PATH,
    candidates_path: str = CANDIDATES_PATH,
    series_file:     str = SERIES_FILE,
    historic_dir:    str = HISTORIC_DIR,
    sheet_names:     tuple = SHEET_NAMES,
) -> dict:
    """Build and return the full Scope Report as a dict.

    Parameters allow override for testing; defaults match the path constants.
    """
    section_a = _load_section_a(universe_path)
    section_b = _load_section_b(candidates_path)
    section_c = _load_section_c(series_file, historic_dir, sheet_names)
    section_d = _compute_section_d(section_a, section_b, section_c)

    return {
        "section_a": section_a,
        "section_b": section_b,
        "section_c": section_c,
        "section_d": section_d,
    }


if __name__ == "__main__":
    report = build_report()
    _print_report(report)
