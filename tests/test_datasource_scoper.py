"""Unit tests for datasource_scoper.py.

Tests cover normal operation, graceful handling of missing/corrupt files,
and discrepancy detection. All file I/O is mocked — no real filesystem
or network calls are made.
"""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import mock_open, patch

# ---------------------------------------------------------------------------
# Make the scoper importable without executing __main__ block.
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).parent.parent
_DATA_INPUT = str(_REPO_ROOT / "data" / "input")
if _DATA_INPUT not in sys.path:
    sys.path.insert(0, _DATA_INPUT)

import datasource_scoper as scoper  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_universe_json(verified: list = None, candidates: list = None, extra: list = None) -> str:
    """Return a universe.json string with given verified and candidate epics."""
    instruments = []
    for e in (verified or []):
        instruments.append({"epic": e, "name": e, "asset_class": "index", "region": "UK", "status": "verified"})
    for e in (candidates or []):
        instruments.append({"epic": e, "name": e, "asset_class": "index", "region": "UK", "status": "candidate"})
    for entry in (extra or []):
        instruments.append(entry)
    return json.dumps({"description": "test", "instruments": instruments})


def _make_mock_dataframe(epics: list[str], first: str = "2026-01-01", last: str = "2026-12-31"):
    """Return a pandas DataFrame mimicking a loaded series sheet."""
    import pandas as pd
    dates = pd.to_datetime([first, last], utc=True)
    df = pd.DataFrame({e: [1.0, 2.0] for e in epics}, index=dates)
    df.index = pd.to_datetime(df.index, utc=True)
    return df


# ---------------------------------------------------------------------------
# Section A tests
# ---------------------------------------------------------------------------

class TestSectionA(unittest.TestCase):
    """Tests for _load_section_a."""

    def test_normal_operation_verified_and_candidate(self):
        """Section A counts verified and candidate instruments correctly."""
        content = _make_universe_json(verified=["A.B.C.D.IP"], candidates=["E.F.G.H.IP"])
        with patch("builtins.open", mock_open(read_data=content)):
            result = scoper._load_section_a("fake/path.json")
        self.assertIsNone(result["error"])
        self.assertEqual(result["total_instruments"], 2)
        self.assertEqual(result["verified_count"], 1)
        self.assertEqual(result["candidate_count"], 1)
        self.assertIn("A.B.C.D.IP", result["verified_epics"])
        self.assertIn("E.F.G.H.IP", result["candidate_epics"])
        self.assertEqual(result["malformed_entries"], 0)

    def test_malformed_entry_missing_epic(self):
        """Instruments with empty or missing epic are counted as malformed."""
        data = json.dumps({"instruments": [
            {"epic": "", "name": "bad", "status": "verified"},
            {"epic": "A.B.C.D.IP", "name": "ok", "status": "verified"},
        ]})
        with patch("builtins.open", mock_open(read_data=data)):
            result = scoper._load_section_a("fake/path.json")
        self.assertEqual(result["malformed_entries"], 1)
        self.assertEqual(result["total_instruments"], 1)

    def test_unknown_status_reported(self):
        """Instruments with unrecognised status values are collected in unknown_status."""
        data = json.dumps({"instruments": [
            {"epic": "A.B.C.D.IP", "name": "x", "status": "pending"},
        ]})
        with patch("builtins.open", mock_open(read_data=data)):
            result = scoper._load_section_a("fake/path.json")
        self.assertIn("A.B.C.D.IP", result["unknown_status"])
        self.assertEqual(result["verified_count"], 0)
        self.assertEqual(result["candidate_count"], 0)

    def test_valid_flag_fallback_when_status_absent(self):
        """P9: instruments without a status field fall back to the valid flag.

        valid=true  → treated as verified (discover_universe.py output schema).
        valid=false → treated as candidate.
        """
        data = json.dumps({"instruments": [
            {"epic": "A.B.C.D.IP", "name": "verified one", "valid": True},
            {"epic": "E.F.G.H.IP", "name": "candidate one", "valid": False},
        ]})
        with patch("builtins.open", mock_open(read_data=data)):
            result = scoper._load_section_a("fake/path.json")
        self.assertIsNone(result["error"])
        self.assertIn("A.B.C.D.IP", result["verified_epics"])
        self.assertIn("E.F.G.H.IP", result["candidate_epics"])
        self.assertEqual(result["verified_count"], 1)
        self.assertEqual(result["candidate_count"], 1)
        self.assertEqual(result["unknown_status"], [])

    def test_missing_file_returns_error(self):
        """FileNotFoundError is caught and reported in the error field."""
        with patch("builtins.open", side_effect=FileNotFoundError("no file")):
            result = scoper._load_section_a("missing.json")
        self.assertIsNotNone(result["error"])
        self.assertIn("not found", result["error"].lower())
        self.assertEqual(result["total_instruments"], 0)

    def test_corrupt_json_returns_error(self):
        """Corrupt JSON is caught and reported in the error field."""
        with patch("builtins.open", mock_open(read_data="not valid json{")):
            result = scoper._load_section_a("corrupt.json")
        self.assertIsNotNone(result["error"])
        self.assertEqual(result["total_instruments"], 0)


# ---------------------------------------------------------------------------
# Section B tests
# ---------------------------------------------------------------------------

class TestSectionB(unittest.TestCase):
    """Tests for _load_section_b."""

    def test_missing_series_file_reports_error(self):
        """Missing universe_series.xlsx is reported; historic scan still runs."""
        with patch("os.path.exists", return_value=False):
            result = scoper._load_section_b("missing.xlsx", "fake/historic", scoper.SHEET_NAMES)
        self.assertIsNotNone(result["error"])
        self.assertEqual(result["series_epic_count"], 0)

    def test_normal_operation(self):
        """Section B extracts epics and date range from mid_close sheet."""
        import pandas as pd
        df = _make_mock_dataframe(["A.B.C.D.IP", "E.F.G.H.IP"])

        with (
            patch("os.path.exists", return_value=True),
            patch("pandas.read_excel", return_value=df),
            patch.object(scoper, "_list_historic_files", return_value=[]),
        ):
            result = scoper._load_section_b("fake.xlsx", "fake/historic", scoper.SHEET_NAMES)

        self.assertIsNone(result["error"])
        self.assertIn("A.B.C.D.IP", result["series_epics"])
        self.assertEqual(result["series_epic_count"], 2)
        self.assertIn("first", result["date_range"])
        self.assertIn("last", result["date_range"])

    def test_historic_files_listed(self):
        """Historic files are scanned and returned (excluding .gitkeep)."""
        with (
            patch("os.path.exists", return_value=False),
            patch.object(scoper, "_list_historic_files", return_value=["2025_q1.xlsx", "2025_q2.xlsx"]),
        ):
            result = scoper._load_section_b("missing.xlsx", "fake/historic", scoper.SHEET_NAMES)
        self.assertEqual(result["historic_file_count"], 2)
        self.assertIn("2025_q1.xlsx", result["historic_files_present"])

    def test_sheet_read_error_reported(self):
        """An error reading the Excel sheet is caught and reported."""
        with (
            patch("os.path.exists", return_value=True),
            patch("pandas.read_excel", side_effect=Exception("bad file")),
            patch.object(scoper, "_list_historic_files", return_value=[]),
        ):
            result = scoper._load_section_b("bad.xlsx", "fake/historic", scoper.SHEET_NAMES)
        self.assertIsNotNone(result["error"])
        self.assertEqual(result["series_epic_count"], 0)


# ---------------------------------------------------------------------------
# Section C / discrepancy tests
# ---------------------------------------------------------------------------

class TestSectionC(unittest.TestCase):
    """Tests for _compute_section_c and _find_variant_orphans."""

    def _make_sections(self, verified_epics, candidate_epics, series_epics):
        section_a = {
            "error": None,
            "verified_epics": verified_epics,
            "candidate_epics": candidate_epics,
            "unknown_status": [],
        }
        section_b = {"error": None, "series_epics": series_epics}
        return section_a, section_b

    def test_in_universe_not_in_series(self):
        """Epics in universe but absent from series are flagged."""
        a, b = self._make_sections(["A.B.C.D.IP"], ["E.F.G.H.IP"], ["A.B.C.D.IP"])
        c = scoper._compute_section_c(a, b)
        self.assertIn("E.F.G.H.IP", c["in_universe_not_in_series"])
        self.assertNotIn("A.B.C.D.IP", c["in_universe_not_in_series"])

    def test_in_series_not_in_universe(self):
        """Epics in series but absent from universe are flagged as orphans."""
        a, b = self._make_sections(["A.B.C.D.IP"], [], ["A.B.C.D.IP", "Z.Z.Z.Z.IP"])
        c = scoper._compute_section_c(a, b)
        self.assertIn("Z.Z.Z.Z.IP", c["in_series_not_in_universe"])

    def test_verified_not_in_series(self):
        """Verified epics with no series column are highest-priority gaps."""
        a, b = self._make_sections(["A.B.C.D.IP"], [], [])
        c = scoper._compute_section_c(a, b)
        self.assertIn("A.B.C.D.IP", c["verified_not_in_series"])

    def test_same_base_variant_orphans_detected(self):
        """A series epic with matching 3-segment base but different exact epic is an orphan."""
        universe_epics = {"IX.D.FTSE.DAILY.IP"}
        series_epics = {"IX.D.FTSE.CASH.IP"}  # same base IX.D.FTSE, different variant
        orphans = scoper._find_variant_orphans(series_epics, universe_epics)
        self.assertEqual(len(orphans), 1)
        self.assertEqual(orphans[0]["series_epic"], "IX.D.FTSE.CASH.IP")
        self.assertEqual(orphans[0]["universe_epic"], "IX.D.FTSE.DAILY.IP")

    def test_exact_match_not_orphan(self):
        """An epic present in both series and universe is NOT reported as an orphan."""
        universe_epics = {"IX.D.FTSE.DAILY.IP"}
        series_epics = {"IX.D.FTSE.DAILY.IP"}
        orphans = scoper._find_variant_orphans(series_epics, universe_epics)
        self.assertEqual(orphans, [])

    def test_no_base_match_not_orphan(self):
        """A series epic with no matching base in universe is not a variant orphan."""
        universe_epics = {"IX.D.FTSE.DAILY.IP"}
        series_epics = {"CS.D.GBPUSD.MINI.IP"}
        orphans = scoper._find_variant_orphans(series_epics, universe_epics)
        self.assertEqual(orphans, [])


# ---------------------------------------------------------------------------
# Integration — build_report smoke test
# ---------------------------------------------------------------------------

class TestBuildReport(unittest.TestCase):
    """Smoke tests for build_report."""

    def test_build_report_returns_three_sections(self):
        """build_report always returns a dict with sections a, b, c."""
        import pandas as pd
        df = _make_mock_dataframe(["A.B.C.D.IP"])
        universe_content = _make_universe_json(verified=["A.B.C.D.IP"])

        with (
            patch("builtins.open", mock_open(read_data=universe_content)),
            patch("os.path.exists", return_value=True),
            patch("pandas.read_excel", return_value=df),
            patch.object(scoper, "_list_historic_files", return_value=[]),
        ):
            report = scoper.build_report()

        self.assertIn("section_a", report)
        self.assertIn("section_b", report)
        self.assertIn("section_c", report)

    def test_build_report_all_missing_files(self):
        """build_report handles all files missing without raising an exception."""
        with (
            patch("builtins.open", side_effect=FileNotFoundError("no file")),
            patch("os.path.exists", return_value=False),
            patch.object(scoper, "_list_historic_files", return_value=[]),
        ):
            report = scoper.build_report()

        self.assertIsNotNone(report["section_a"]["error"])
        self.assertIsNotNone(report["section_b"]["error"])
        # section_c is derived — should still be present with empty lists
        self.assertIn("section_c", report)


if __name__ == "__main__":
    unittest.main()
