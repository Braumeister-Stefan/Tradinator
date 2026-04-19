"""
Tradinator — Entry point.

Defines major configuration parameters and launches the trading engine.
Run with: python main.py

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import argparse

from model import Model
from run_loop import RunLoop


def _print_credentials_setup_error(error: RuntimeError) -> None:
    """Print the required IG credential setup steps for first-time runs."""
    print("Tradinator could not start because IG credentials are not configured.")
    print(str(error))
    print("Next steps:")
    print("1. Copy secrets/.env.example to secrets/.env")
    print("2. Fill in IG_USERNAME, IG_PASSWORD, and IG_API_KEY")
    print("3. Run main.py again")


def _print_ig_authentication_error(error: RuntimeError) -> None:
    """Print actionable guidance for common IG authentication failures."""
    print("Tradinator could not authenticate with IG.")
    print(str(error))
    print("Next steps:")
    print("1. Verify IG_USERNAME is your IG login identifier, not your account number")
    print("2. Keep IG_ACC_NUMBER as the account id, for example MERTST...")
    print("3. Confirm the credentials belong to an IG DEMO account")
    print("4. Confirm the API key is enabled for the same IG account")


def _parse_args():
    """Parse command-line arguments for run mode and scheduling."""
    parser = argparse.ArgumentParser(description="Tradinator paper trading engine")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["run_once", "scheduled", "decoupled", "research_only"],
        default="run_once",
        help="Execution mode (default: run_once)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=3600,
        help="Seconds between runs for scheduled mode (default: 3600)",
    )
    parser.add_argument(
        "--research-interval",
        type=int,
        default=14400,
        help="Seconds between research cycles for decoupled mode (default: 14400)",
    )
    parser.add_argument(
        "--execution-interval",
        type=int,
        default=3600,
        help="Seconds between execution cycles for decoupled mode (default: 3600)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Major parameters — control *what* the engine does each run.
# ---------------------------------------------------------------------------
config = {
    # Credentials --------------------------------------------------------
    "env_path": "secrets/.env",        # path to .env file with IG creds

    # Universe -----------------------------------------------------------
    "universe": [                       # IG epics for the equity spot universe
        "IX.D.DAX.IFD.IP",             # Germany 40 (DAX) - Standard Demo Index
        "IX.D.FTSE.IFD.IP",            # FTSE 100 - Standard Demo Index
        "CS.D.AAPL.CFD.IP",            # Apple
        "CS.D.MSFT.CFD.IP",            # Microsoft
    ],

    # Market data --------------------------------------------------------
    "resolution": "DAY",                # price bar resolution
    "lookback": 50,                     # number of bars to fetch

    # Portfolio rules ----------------------------------------------------
    "max_position_pct": 0.25,           # max weight for a single position
    "cash_reserve_pct": 0.05,           # minimum cash to keep unallocated

    # Output -------------------------------------------------------------
    "output_dir": "data/output",        # base directory for all output files
}

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    args = _parse_args()
    try:
        model = Model(config)
        run_loop = RunLoop(
            model,
            args.mode,
            interval=args.interval,
            research_interval=args.research_interval,
            execution_interval=args.execution_interval,
        )
        run_loop.start()
    except RuntimeError as error:
        if "Missing required IG credentials" not in str(error):
            if "validation.pattern.invalid.authenticationRequest.identifier" not in str(error):
                raise
            _print_ig_authentication_error(error)
            raise SystemExit(1) from None
        _print_credentials_setup_error(error)
        raise SystemExit(1) from None
