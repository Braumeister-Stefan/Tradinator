"""
Unit tests for data/input/discover_universe.py -- retry and Tier-1 classification.

Tests use only stdlib mocks; no live IG API calls and no filesystem writes.
trading_ig is mocked at import time so these tests run in any environment.
"""

import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Stub out trading_ig before importing discover_universe so the module-level
# ``from trading_ig import IGService`` does not require the broker library.
# ---------------------------------------------------------------------------
_trading_ig_stub = types.ModuleType("trading_ig")
_trading_ig_stub.IGService = MagicMock  # type: ignore[attr-defined]
sys.modules.setdefault("trading_ig", _trading_ig_stub)

# Ensure project root is on sys.path.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import data.input.discover_universe as du  # noqa: E402


class TestApiCallWithRetry(unittest.TestCase):
    """Tests for _api_call_with_retry()."""

    @patch("time.sleep")
    def test_non_retryable_error_does_not_sleep(self, mock_sleep):
        """A deterministic epic.unavailable error must propagate immediately with zero sleeps."""
        def always_fails():
            raise RuntimeError("error.service.marketdata.instrument.epic.unavailable")

        with self.assertRaises(RuntimeError):
            du._api_call_with_retry(always_fails)

        mock_sleep.assert_not_called()

    @patch("time.sleep")
    def test_transient_error_retries_with_sleep(self, mock_sleep):
        """A generic transient error must be retried with inter-attempt sleeps."""
        def always_fails():
            raise RuntimeError("network timeout")

        with self.assertRaises(RuntimeError):
            du._api_call_with_retry(always_fails)

        # _API_MAX_RETRIES - 1 sleeps between attempts (last attempt prints, doesn't sleep).
        self.assertEqual(mock_sleep.call_count, du._API_MAX_RETRIES - 1)

    @patch("time.sleep")
    def test_success_on_first_attempt_no_sleep(self, mock_sleep):
        """A successful call must return the value with no retries."""
        result = du._api_call_with_retry(lambda: {"market": "data"})
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

        t1_status, t1_reason = du._validate_tier1(self.mock_ig, "IX.D.HSENG.DAILY.IP")

        self.assertEqual(t1_status, "EPIC_NOT_RECOGNIZED")
        self.assertIn("epic.unavailable", t1_reason.lower())

    @patch("time.sleep")
    def test_generic_exception_classified_as_api_error(self, _mock_sleep):
        """An unrecognised exception must fall through to API_ERROR, not EPIC_NOT_RECOGNIZED."""
        self.mock_ig.fetch_market_by_epic.side_effect = RuntimeError("connection reset by peer")

        t1_status, _reason = du._validate_tier1(self.mock_ig, "SOME.EPIC")

        self.assertEqual(t1_status, "API_ERROR")

    def test_dealing_enabled_true_returns_pass(self):
        """A market with dealingEnabled=True must return PASS."""
        self.mock_ig.fetch_market_by_epic.return_value = {
            "snapshot": {"dealingEnabled": True},
            "instrument": {},
        }

        t1_status, _reason = du._validate_tier1(self.mock_ig, "SOME.EPIC")

        self.assertEqual(t1_status, "PASS")

    def test_dealing_disabled_returns_dealing_disabled(self):
        """A market with dealingEnabled=False must return DEALING_DISABLED."""
        self.mock_ig.fetch_market_by_epic.return_value = {
            "snapshot": {"dealingEnabled": False},
            "instrument": {},
        }

        t1_status, _reason = du._validate_tier1(self.mock_ig, "SOME.EPIC")

        self.assertEqual(t1_status, "DEALING_DISABLED")


if __name__ == "__main__":
    unittest.main()
