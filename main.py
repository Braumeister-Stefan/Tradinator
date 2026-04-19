"""
Tradinator — Entry point.

Defines major configuration parameters and launches the trading engine.
Run with: python main.py

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import argparse
import json
import os

from model import Model
from run_loop import RunLoop

UNIVERSE_PATH = os.path.join("data", "input", "universe.json")


def _load_universe(path: str) -> list[str]:
    """Load the instrument universe from a JSON file.

    Returns a deduplicated list of IG epic strings.  When both
    ``.DAILY.IP`` and ``.IFD.IP`` (or ``.CASH.IP``) variants exist for
    the same underlying market, only the first encountered variant is
    kept so the pipeline does not fetch redundant price series.
    """
    try:
        with open(path) as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Universe file not found: {path}")
        print("Create data/input/universe.json or run data/input/discover_universe.py")
        raise SystemExit(1) from None
    except json.JSONDecodeError as exc:
        print(f"ERROR: Universe file contains invalid JSON: {path}")
        print(f"  {exc}")
        raise SystemExit(1) from None

    instruments = data.get("instruments", [])
    if not instruments:
        print(f"WARNING: No instruments found in {path} — pipeline will run with an empty universe.")

    seen_bases: set[str] = set()
    epics: list[str] = []
    for inst in instruments:
        epic = inst.get("epic", "")
        if not epic:
            continue
        # Base = first three dot-segments, e.g. "IX.D.FTSE" from
        # "IX.D.FTSE.DAILY.IP" — identical for DAILY / IFD / CASH variants.
        base = ".".join(epic.split(".")[:3])
        if base in seen_bases:
            continue
        seen_bases.add(base)
        epics.append(epic)
    return epics


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
    "universe_path": UNIVERSE_PATH,     # path to universe JSON file
    "universe": _load_universe(UNIVERSE_PATH),

    # Market data --------------------------------------------------------
    "resolution": "DAY",                # price bar resolution
    "lookback": 50,                     # number of bars to fetch

    # Portfolio rules ----------------------------------------------------
    "max_position_pct": 0.25,           # max weight for a single position
    "cash_reserve_pct": 0.05,           # minimum cash to keep unallocated

    # Output -------------------------------------------------------------
    "output_dir": "data/output",        # base directory for all output files
    "max_handoff_age_seconds": 7200,   # max age of handoff file before considered stale
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
