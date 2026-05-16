"""
Unit tests for diagnostic_tools/refresh_universe.py — retry, Tier-1 classification,
and search-drilldown behaviour.

Tests use only stdlib mocks; no live IG API calls and no filesystem writes.
trading_ig is mocked at import time so these tests run in any environment.
"""

import importlib.util
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Stub out trading_ig before loading refresh_universe so the module-level
# ``from trading_ig import IGService`` does not require the broker library.
# ---------------------------------------------------------------------------
_trading_ig_stub = types.ModuleType("trading_ig")
_trading_ig_stub.IGService = MagicMock  # type: ignore[attr-defined]
sys.modules.setdefault("trading_ig", _trading_ig_stub)

# Ensure project root is on sys.path.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# Load refresh_universe via importlib (diagnostic_tools/ has no __init__.py).
_REFRESH_PATH = os.path.join(PROJECT_ROOT, "diagnostic_tools", "refresh_universe.py")
_spec = importlib.util.spec_from_file_location("refresh_universe", _REFRESH_PATH)
_mod  = importlib.util.module_from_spec(_spec)   # type: ignore[arg-type]
_spec.loader.exec_module(_mod)                    # type: ignore[union-attr]
ru = _mod   # shorthand used throughout the test classes


class TestApiCallWithRetry(unittest.TestCase):
    """Tests for _api_call_with_retry()."""

    @patch("time.sleep")
    def test_non_retryable_error_does_not_sleep(self, mock_sleep):
        """A deterministic epic.unavailable error must propagate immediately with zero sleeps."""
        def always_fails():
            raise RuntimeError("error.service.marketdata.instrument.epic.unavailable")

        with self.assertRaises(RuntimeError):
            ru._api_call_with_retry(always_fails)

        mock_sleep.assert_not_called()

    @patch("time.sleep")
    def test_transient_error_retries_with_sleep(self, mock_sleep):
        """A generic transient error must be retried with inter-attempt sleeps."""
        def always_fails():
            raise RuntimeError("network timeout")

        with self.assertRaises(RuntimeError):
            ru._api_call_with_retry(always_fails)

        # Retry uses a countdown timer (time.sleep(1) per second of wait), so
        # there must be at least one sleep call for the inter-attempt intervals.
        self.assertGreater(mock_sleep.call_count, 0)

    @patch("time.sleep")
    def test_success_on_first_attempt_no_sleep(self, mock_sleep):
        """A successful call must return the value with no retries."""
        result = ru._api_call_with_retry(lambda: {"market": "data"})
        self.assertEqual(result, {"market": "data"})
        mock_sleep.assert_not_called()


class TestValidateTier1(unittest.TestCase):
    """Tests for _validate_tier1()."""

    def setUp(self):
        self.mock_ig = MagicMock()

    @patch("time.sleep")
    def test_epic_unavailable_classified_as_not_recognized(self, _mock_sleep):
        """error.service.marketdata.instrument.epic.unavailable -> EPIC_NOT_RECOGNIZED."""
        self.mock_ig.fetch_market_by_epic.side_effect = RuntimeError(
            "error.service.marketdata.instrument.epic.unavailable"
        )

        t1_status, t1_reason = ru._validate_tier1(self.mock_ig, "IX.D.HSENG.DAILY.IP")

        self.assertEqual(t1_status, "EPIC_NOT_RECOGNIZED")
        self.assertIn("epic.unavailable", t1_reason.lower())

    @patch("time.sleep")
    def test_generic_exception_classified_as_api_error(self, _mock_sleep):
        """An unrecognised exception must fall through to API_ERROR, not EPIC_NOT_RECOGNIZED."""
        self.mock_ig.fetch_market_by_epic.side_effect = RuntimeError("connection reset by peer")

        t1_status, _reason = ru._validate_tier1(self.mock_ig, "SOME.EPIC")

        self.assertEqual(t1_status, "API_ERROR")

    def test_dealing_enabled_true_returns_pass(self):
        """A market with dealingEnabled=True must return PASS."""
        self.mock_ig.fetch_market_by_epic.return_value = {
            "snapshot": {"dealingEnabled": True},
            "instrument": {},
        }

        t1_status, _reason = ru._validate_tier1(self.mock_ig, "SOME.EPIC")

        self.assertEqual(t1_status, "PASS")

    def test_dealing_disabled_returns_dealing_disabled(self):
        """A market with dealingEnabled=False must return DEALING_DISABLED."""
        self.mock_ig.fetch_market_by_epic.return_value = {
            "snapshot": {"dealingEnabled": False},
            "instrument": {},
        }

        t1_status, _reason = ru._validate_tier1(self.mock_ig, "SOME.EPIC")

        self.assertEqual(t1_status, "DEALING_DISABLED")

    def test_dealing_enabled_absent_returns_pass(self):
        """A market where dealingEnabled is absent must be assumed tradeable (PASS)."""
        self.mock_ig.fetch_market_by_epic.return_value = {
            "snapshot": {},
            "instrument": {},
        }

        t1_status, t1_reason = ru._validate_tier1(self.mock_ig, "SOME.EPIC")

        self.assertEqual(t1_status, "PASS")
        self.assertIn("absent", t1_reason.lower())

    def test_none_result_returns_not_recognized(self):
        """A None response from the broker must return EPIC_NOT_RECOGNIZED."""
        self.mock_ig.fetch_market_by_epic.return_value = None

        t1_status, _reason = ru._validate_tier1(self.mock_ig, "SOME.EPIC")

        self.assertEqual(t1_status, "EPIC_NOT_RECOGNIZED")


class TestSearchWithDrilldown(unittest.TestCase):
    """Tests for _search_with_drilldown()."""

    def setUp(self):
        self.mock_ig = MagicMock()

    @patch("time.sleep")
    def test_drilldown_stops_at_call_cap(self, _mock_sleep):
        """When call_counter already equals MAX_SEARCH_CALLS, no API calls are made."""
        call_counter = [ru.MAX_SEARCH_CALLS]
        seen: set = set()

        result = ru._search_with_drilldown(self.mock_ig, "US 500", seen, call_counter)

        self.assertEqual(result, [])
        self.mock_ig.search_markets.assert_not_called()

    @patch("time.sleep")
    def test_new_epics_added_to_seen_set(self, _mock_sleep):
        """Markets returned by search_markets must be added to seen_epics."""
        self.mock_ig.search_markets.return_value = {
            "markets": [
                {"epic": "EPIC.A", "instrumentName": "Alpha", "instrumentType": "SHARES"},
                {"epic": "EPIC.B", "instrumentName": "Beta",  "instrumentType": "INDICES"},
            ]
        }
        call_counter = [0]
        seen: set = set()

        results = ru._search_with_drilldown(self.mock_ig, "US 500", seen, call_counter)

        self.assertEqual({r["epic"] for r in results}, {"EPIC.A", "EPIC.B"})
        self.assertIn("EPIC.A", seen)
        self.assertIn("EPIC.B", seen)

    @patch("time.sleep")
    def test_duplicate_epics_are_skipped(self, _mock_sleep):
        """Epics already in seen_epics must not appear in the results."""
        self.mock_ig.search_markets.return_value = {
            "markets": [
                {"epic": "EPIC.A", "instrumentName": "Alpha", "instrumentType": "SHARES"},
            ]
        }
        call_counter = [0]
        seen = {"EPIC.A"}   # already seen

        results = ru._search_with_drilldown(self.mock_ig, "US 500", seen, call_counter)

        self.assertEqual(results, [])

    @patch("time.sleep")
    def test_below_cap_does_not_trigger_drilldown(self, _mock_sleep):
        """When results are below _DRILL_CAP, no a–z suffix calls are made."""
        self.mock_ig.search_markets.return_value = {
            "markets": [
                {"epic": f"EP.{i}", "instrumentName": f"Name{i}", "instrumentType": "SHARES"}
                for i in range(5)   # well below the cap of 50
            ]
        }
        call_counter = [0]
        seen: set = set()

        ru._search_with_drilldown(self.mock_ig, "US 500", seen, call_counter)

        # Only one search call made (the root term); no drilldown iterations.
        self.assertEqual(self.mock_ig.search_markets.call_count, 1)


if __name__ == "__main__":
    unittest.main()
