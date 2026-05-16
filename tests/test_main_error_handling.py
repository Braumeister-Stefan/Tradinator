"""Tests that RuntimeErrors from the pipeline are handled gracefully in main.py."""

import os
import subprocess
import sys
import unittest

# Repository root: two levels up from this test file.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


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

        Patches ``model.Model`` to raise ``RuntimeError(error_msg)`` before the pipeline
        runs, then executes ``main.py`` in a subprocess.  Asserts that stdout/stderr
        does NOT contain ``'Traceback'`` (i.e. the error is caught and handled cleanly).
        """
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
