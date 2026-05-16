"""Tests that RuntimeErrors from the pipeline are handled gracefully in main.py.

Reproduces the bug where _create_session sanitises IG error messages,
stripping strings that main.py relies on for pattern matching — causing
an unhandled traceback at line 152 (run_loop.start()).
"""

import os
import subprocess
import sys
import unittest

# Repository root: two levels up from this test file.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestMainErrorHandling(unittest.TestCase):
    """Verify main.py catches all RuntimeErrors without raw tracebacks."""

    # ------------------------------------------------------------------ #
    # Bug reproducer: sanitised connection error leaks as traceback
    # ------------------------------------------------------------------ #

    def test_sanitised_connection_error_does_not_produce_traceback(self):
        """A 'Failed to connect' RuntimeError must not produce a raw traceback.

        _create_session wraps the original IG exception in a sanitised
        RuntimeError whose message no longer contains
        'validation.pattern.invalid.authenticationRequest.identifier'.
        Before the fix, main.py re-raised this as an unhandled exception.
        """
        sanitised_msg = (
            "Failed to connect to IG after 3 attempts (HttpError). "
            "Check credentials and network connectivity."
        )
        self._assert_runtime_error_handled_cleanly(sanitised_msg)

    def test_account_not_found_error_does_not_produce_traceback(self):
        """RuntimeError from get_account_info should not leak."""
        self._assert_runtime_error_handled_cleanly(
            "Account MERTST123 not found in IG accounts"
        )

    def test_no_accounts_error_does_not_produce_traceback(self):
        """RuntimeError when IG returns no accounts should not leak."""
        self._assert_runtime_error_handled_cleanly(
            "IGBrokerAdapter: No accounts returned by IG API"
        )

    def test_demo_only_error_does_not_produce_traceback(self):
        """RuntimeError when ACC_TYPE is not DEMO should not leak."""
        self._assert_runtime_error_handled_cleanly(
            "Tradinator is restricted to paper trading (DEMO) only. "
            "IG_ACC_TYPE is set to 'LIVE'. Set it to 'DEMO' or remove it."
        )

    def test_missing_credentials_still_handled(self):
        """The existing 'Missing required IG credentials' path must still work."""
        self._assert_runtime_error_handled_cleanly(
            "Missing required IG credentials: IG_USERNAME, IG_PASSWORD. "
            "Set them as environment variables or in the .env file."
        )

    def test_validation_pattern_still_handled(self):
        """The existing 'validation.pattern.invalid' path must still work."""
        self._assert_runtime_error_handled_cleanly(
            "validation.pattern.invalid.authenticationRequest.identifier"
        )

    # ------------------------------------------------------------------ #
    # Helper
    # ------------------------------------------------------------------ #

    def _assert_runtime_error_handled_cleanly(self, error_msg: str):
        """Run main.py with a mocked Model that raises RuntimeError.

        Asserts:
        - Process exits with code 1 (not 0, not an unhandled-exception code).
        - stdout/stderr does NOT contain 'Traceback'.
        """
        # Inline script that patches Model before main.py's __main__ runs.
        script = (
            "import sys; sys.argv = ['main.py']; "
            "from unittest.mock import patch; "
            f"err = RuntimeError({error_msg!r}); "
            "p = patch('model.Model', side_effect=err); p.start(); "
            "exec(open('main.py').read())"
        )

        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=10,
            cwd=REPO_ROOT,
        )

        combined_output = result.stdout + result.stderr
        self.assertNotIn(
            "Traceback",
            combined_output,
            f"RuntimeError({error_msg!r}) produced a raw traceback:\n"
            f"{combined_output}",
        )
        self.assertEqual(
            result.returncode,
            1,
            f"Expected exit code 1 but got {result.returncode}. "
            f"Output:\n{combined_output}",
        )


class TestIBKRErrorHandling(unittest.TestCase):
    """Verify main.py handles IBKR connection errors gracefully."""

    def test_ibkr_connection_refused_does_not_produce_traceback(self):
        """A ConnectionRefusedError from IBKR must not produce a raw traceback."""
        self._assert_runtime_error_handled_cleanly(
            "IBKR: ConnectionRefusedError — TWS/IB Gateway refused the connection on port 4002."
        )

    def test_ibkr_paper_only_guard_raises_on_live_port(self):
        """IBKR_PAPER_ONLY=true with live port must produce a clear error, not a traceback."""
        self._assert_runtime_error_handled_cleanly(
            "IBKR_PAPER_ONLY is set to 'true' but IBKR_PORT is 4001 (live trading port). "
            "Set IBKR_PORT=4002 for paper trading or unset IBKR_PAPER_ONLY."
        )

    def _assert_runtime_error_handled_cleanly(self, error_msg: str):
        """Run main.py with a mocked Model that raises RuntimeError; assert no raw traceback.

        Disables the universe refresh (which requires __file__ undefined in -c context)
        via source substitution before exec, so only the error-handling path is exercised.
        """
        script = (
            "import sys; sys.argv = ['main.py']; "
            "from unittest.mock import patch; "
            f"err = RuntimeError({error_msg!r}); "
            "p = patch('model.Model', side_effect=err); p.start(); "
            # Disable universe refresh to avoid __file__ NameError in -c context.
            "src = open('main.py').read().replace('\"refresh_universe\": True', "
            "    '\"refresh_universe\": False'); "
            "exec(src)"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=REPO_ROOT,
        )
        combined_output = result.stdout + result.stderr
        self.assertNotIn(
            "Traceback",
            combined_output,
            f"RuntimeError({error_msg!r}) produced a raw traceback:\n{combined_output}",
        )


if __name__ == "__main__":
    unittest.main()
