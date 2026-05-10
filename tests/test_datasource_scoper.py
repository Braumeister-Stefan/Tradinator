"""Unit tests for datasource_scoper.py.

Tests cover normal operation, graceful handling of missing/corrupt files,
and discrepancy detection. All file I/O is mocked — no real filesystem
or network calls are made.
"""

import io
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

# ---------------------------------------------------------------------------
# Make the scoper importable without executing __main__ block.
# ---------------------------------------------------------------------------

# Add data/input to sys.path so we can import datasource_scoper.
_REPO_ROOT = Path(__file__).parent.parent
_DATA_INPUT = str(_REPO_ROOT / "data" / "input")
if _DATA_INPUT not in sys.path:
    sys.path.insert(0, _DATA_INPUT)

import datasource_scoper as scoper  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_universe_json(epics: list[str]) -> str:
    """Return a universe.json string containing the given epics."""
    instruments = [{"epic": e, "name": e, "asset_class": "index", "region": "UK", "valid": True} for e in epics]
    return json.dumps({"description": "test", "instruments": instruments})


def _make_candidates_json(candidates: list[dict]) -> str:
    """Return a universe_candidates.json string for the given candidate dicts."""
    return json.dumps({
        "description": "test",
        "last_discover_run": "2026-01-01T00:00:00Z",
        "candidates": candidates,
    })


def _candidate(epic: str, t1: str = "PASS", t2: str = "PENDING_T2", valid: bool = False) -> dict:
    """Return a minimal candidate dict."""
    return {
        "epic": epic,
        "name": epic,
        "asset_class": "index",
        "region": "UK",
        "t1_status": t1,
        "t1_reason": None,
        "t2_status": t2,
        "t2_reason": None,
        "valid": valid,
        "last_validated": None,
    }


def _make_mock_dataframe(epics: list[str], first: str = "2026-01-01", last: str = "2026-12-31"):
    """Return a pandas DataFrame that mimics a loaded series sheet."""
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

    def test_normal_operation(self):
        """Section A returns correct counts for a valid universe.json."""
        content = _make_universe_json(["A.B.C.D.IP", "X.Y.Z.W.IP"])
        with patch("builtins.open", mock_open(read_data=content)):
            result = scoper._load_section_a("fake/path.json")
        self.assertIsNone(result["error"])
        self.assertEqual(result["total_valid"], 2)
        self.assertIn("A.B.C.D.IP", result["valid_epics"])
        self.assertEqual(result["malformed_entries"], 0)

    def test_malformed_entry_missing_epic(self):
        """Instruments with empty or missing epic are counted as malformed."""
        data = json.dumps({"instruments": [{"epic": "", "name": "bad"}, {"epic": "A.B.C.D.IP", "name": "ok", "valid": True}]})
        with patch("builtins.open", mock_open(read_data=data)):
            result = scoper._load_section_a("fake/path.json")
        self.assertEqual(result["malformed_entries"], 1)
        self.assertEqual(result["total_valid"], 1)

    def test_missing_file_returns_error(self):
        """FileNotFoundError is caught and reported in the error field."""
        with patch("builtins.open", side_effect=FileNotFoundError("no file")):
            result = scoper._load_section_a("missing.json")
        self.assertIsNotNone(result["error"])
        self.assertIn("not found", result["error"].lower())
        self.assertEqual(result["total_valid"], 0)

    def test_corrupt_json_returns_error(self):
        """Corrupt JSON is caught and reported in the error field."""
        with patch("builtins.open", mock_open(read_data="not valid json{")):
            result = scoper._load_section_a("corrupt.json")
        self.assertIsNotNone(result["error"])
        self.assertEqual(result["total_valid"], 0)


# ---------------------------------------------------------------------------
# Section B tests
# ---------------------------------------------------------------------------

class TestSectionB(unittest.TestCase):
    """Tests for _load_section_b."""

    def test_normal_operation(self):
        """Section B counts T1/T2 statuses correctly."""
        candidates = [
            _candidate("A.B.C.D.IP", t1="PASS",                t2="YES",       valid=True),
            _candidate("E.F.G.H.IP", t1="PASS",                t2="PENDING_T2",valid=False),
            _candidate("I.J.K.L.IP", t1="EPIC_NOT_RECOGNIZED", t2="NEVER_TRIED",valid=False),
            _candidate("M.N.O.P.IP", t1="DEALING_DISABLED",    t2="NEVER_TRIED",valid=False),
            _candidate("Q.R.S.T.IP", t1="API_ERROR",           t2="NEVER_TRIED",valid=False),
            _candidate("U.V.W.X.IP", t1="UNTESTED",            t2="NEVER_TRIED",valid=False),
        ]
        content = _make_candidates_json(candidates)
        with patch("builtins.open", mock_open(read_data=content)):
            result = scoper._load_section_b("fake/path.json")
        self.assertIsNone(result["error"])
        self.assertEqual(result["total_candidates"],  6)
        self.assertEqual(result["t1_pass_count"],     2)
        self.assertEqual(result["t1_fail_count"],     3)
        self.assertEqual(result["t1_untested_count"], 1)
        self.assertEqual(result["pending_t2_count"],  1)
        self.assertEqual(result["t2_yes_count"],      1)
        self.assertEqual(result["t2_no_count"],       0)
        self.assertEqual(result["fully_valid_count"], 1)
        self.assertIn("I.J.K.L.IP", result["epic_not_recognized"])
        self.assertIn("M.N.O.P.IP", result["dealing_disabled"])
        self.assertIn("Q.R.S.T.IP", result["api_error"])
        self.assertEqual(result["last_discover_run"], "2026-01-01T00:00:00Z")

    def test_missing_file_returns_error(self):
        """Missing universe_candidates.json reports error; other fields are defaults."""
        with patch("builtins.open", side_effect=FileNotFoundError("no file")):
            result = scoper._load_section_b("missing.json")
        self.assertIsNotNone(result["error"])
        self.assertEqual(result["total_candidates"], 0)

    def test_corrupt_json_returns_error(self):
        """Corrupt JSON is caught and reported."""
        with patch("builtins.open", mock_open(read_data="<<<bad json")):
            result = scoper._load_section_b("bad.json")
        self.assertIsNotNone(result["error"])


# ---------------------------------------------------------------------------
# Section C tests
# ---------------------------------------------------------------------------

class TestSectionC(unittest.TestCase):
    """Tests for _load_section_c."""

    def test_missing_series_file_reports_error(self):
        """Missing universe_series.xlsx is reported; historic scan still runs."""
        with patch("os.path.exists", return_value=False):
            result = scoper._load_section_c("missing.xlsx", "fake/historic", scoper.SHEET_NAMES)
        self.assertIsNotNone(result["error"])
        self.assertEqual(result["series_epic_count"], 0)

    def test_missing_series_file_reports_error_default_args(self):
        """Calling with default arg names still reports missing file gracefully."""
        with patch("os.path.exists", return_value=False):
            result = scoper._load_section_c("missing.xlsx", "fake/historic", scoper.SHEET_NAMES)
        self.assertIsNotNone(result["error"])
        self.assertEqual(result["series_epic_count"], 0)

    def test_normal_operation(self):
        """Section C extracts epics and date range from all sheets."""
        import pandas as pd
        df = _make_mock_dataframe(["A.B.C.D.IP", "E.F.G.H.IP"])

        def fake_read_excel(path, sheet_name, index_col, engine):
            """Return mock dataframe for any sheet."""
            return df.copy()

        with (
            patch("os.path.exists", return_value=True),
            patch("pandas.read_excel", side_effect=fake_read_excel),
            patch.object(scoper, "_list_historic_files", return_value=[]),
        ):
            result = scoper._load_section_c("fake.xlsx", "fake/historic", scoper.SHEET_NAMES)

        self.assertIsNone(result["error"])
        self.assertIn("A.B.C.D.IP", result["series_epics"])
        self.assertEqual(result["series_epic_count"], 2)
        self.assertTrue(result["sheets_have_consistent_date_range"])
        self.assertTrue(result["sheets_fully_consistent"])

    def test_historic_files_listed(self):
        """Historic files are scanned and returned (excluding .gitkeep)."""
        with (
            patch("os.path.exists", return_value=False),
            patch.object(scoper, "_list_historic_files", return_value=["2025_q1.xlsx", "2025_q2.xlsx"]),
        ):
            result = scoper._load_section_c("missing.xlsx", "fake/historic", scoper.SHEET_NAMES)
        self.assertEqual(result["historic_file_count"], 2)
        self.assertIn("2025_q1.xlsx", result["historic_files_present"])


# ---------------------------------------------------------------------------
# Section D / discrepancy tests
# ---------------------------------------------------------------------------

class TestSectionD(unittest.TestCase):
    """Tests for _compute_section_d and _find_variant_orphans."""

    def _make_sections(self, universe_epics, series_epics, pending_epics=None, valid_epics_cand=None):
        """Build minimal section dicts for Section D computation."""
        section_a = {"error": None, "valid_epics": universe_epics, "total_valid": len(universe_epics)}
        section_b = {"error": None}
        section_c = {"error": None, "series_epics": series_epics}
        return section_a, section_b, section_c

    def test_in_universe_not_in_series(self):
        """Epics in universe but absent from series are flagged."""
        a, b, c = self._make_sections(["A.B.C.D.IP", "E.F.G.H.IP"], ["A.B.C.D.IP"])
        candidates = [_candidate("A.B.C.D.IP", t2="PENDING_T2"), _candidate("E.F.G.H.IP", t2="PENDING_T2")]
        cand_json = _make_candidates_json(candidates)
        with patch("builtins.open", mock_open(read_data=cand_json)):
            d = scoper._compute_section_d(a, b, c)
        self.assertIn("E.F.G.H.IP", d["in_universe_not_in_series"])
        self.assertNotIn("A.B.C.D.IP", d["in_universe_not_in_series"])

    def test_in_series_not_in_universe(self):
        """Epics in series but absent from universe are flagged as orphans."""
        a, b, c = self._make_sections(["A.B.C.D.IP"], ["A.B.C.D.IP", "Z.Z.Z.Z.IP"])
        candidates = [_candidate("A.B.C.D.IP")]
        cand_json = _make_candidates_json(candidates)
        with patch("builtins.open", mock_open(read_data=cand_json)):
            d = scoper._compute_section_d(a, b, c)
        self.assertIn("Z.Z.Z.Z.IP", d["in_series_not_in_universe"])

    def test_pending_not_in_series(self):
        """PENDING_T2 epics without series data are flagged."""
        a, b, c = self._make_sections([], [])
        candidates = [_candidate("P.Q.R.S.IP", t2="PENDING_T2")]
        cand_json = _make_candidates_json(candidates)
        with patch("builtins.open", mock_open(read_data=cand_json)):
            d = scoper._compute_section_d(a, b, c)
        self.assertIn("P.Q.R.S.IP", d["pending_not_in_series"])

    def test_valid_not_in_series(self):
        """Fully-valid candidates with no series column are highest-priority gaps."""
        a, b, c = self._make_sections(["A.B.C.D.IP"], [])
        candidates = [_candidate("A.B.C.D.IP", t1="PASS", t2="YES", valid=True)]
        cand_json = _make_candidates_json(candidates)
        with patch("builtins.open", mock_open(read_data=cand_json)):
            d = scoper._compute_section_d(a, b, c)
        self.assertIn("A.B.C.D.IP", d["valid_not_in_series"])

    def test_same_base_variant_orphans_detected(self):
        """A series epic with matching 3-segment base but different exact epic is an orphan."""
        universe_epics = {"IX.D.FTSE.DAILY.IP"}
        series_epics   = {"IX.D.FTSE.CASH.IP"}  # same base IX.D.FTSE, different variant
        orphans = scoper._find_variant_orphans(series_epics, universe_epics)
        self.assertEqual(len(orphans), 1)
        self.assertEqual(orphans[0]["series_epic"],   "IX.D.FTSE.CASH.IP")
        self.assertEqual(orphans[0]["universe_epic"], "IX.D.FTSE.DAILY.IP")
        self.assertEqual(orphans[0]["base"],          "IX.D.FTSE")

    def test_exact_match_not_orphan(self):
        """An epic present in both series and universe is NOT reported as an orphan."""
        universe_epics = {"IX.D.FTSE.DAILY.IP"}
        series_epics   = {"IX.D.FTSE.DAILY.IP"}
        orphans = scoper._find_variant_orphans(series_epics, universe_epics)
        self.assertEqual(orphans, [])

    def test_no_base_match_not_orphan(self):
        """A series epic with no matching base in universe is not a variant orphan."""
        universe_epics = {"IX.D.FTSE.DAILY.IP"}
        series_epics   = {"CS.D.GBPUSD.MINI.IP"}
        orphans = scoper._find_variant_orphans(series_epics, universe_epics)
        self.assertEqual(orphans, [])


# ---------------------------------------------------------------------------
# Integration — build_report smoke test
# ---------------------------------------------------------------------------

class TestBuildReport(unittest.TestCase):
    """Smoke test for build_report with all files present."""

    def test_build_report_returns_four_sections(self):
        """build_report always returns a dict with sections a, b, c, d."""
        import pandas as pd
        df = _make_mock_dataframe(["A.B.C.D.IP"])

        universe_content    = _make_universe_json(["A.B.C.D.IP"])
        candidates_content  = _make_candidates_json([_candidate("A.B.C.D.IP")])

        def fake_open(path, *args, **kwargs):
            """Return appropriate content for each file path."""
            path_str = str(path)
            if "universe_candidates" in path_str:
                return mock_open(read_data=candidates_content)()
            return mock_open(read_data=universe_content)()

        with (
            patch("builtins.open", side_effect=fake_open),
            patch("os.path.exists", return_value=True),
            patch("pandas.read_excel", return_value=df),
            patch.object(scoper, "_list_historic_files", return_value=[]),
        ):
            report = scoper.build_report()

        self.assertIn("section_a", report)
        self.assertIn("section_b", report)
        self.assertIn("section_c", report)
        self.assertIn("section_d", report)

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
        self.assertIsNotNone(report["section_c"]["error"])


if __name__ == "__main__":
    unittest.main()
