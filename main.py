"""
Tradinator — Entry point.

Defines major configuration parameters and launches the trading engine.
Run with: python main.py

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import json
import os

from model import Model

UNIVERSE_PATH = os.path.join("data", "input", "universe.json")


def _load_universe(path: str) -> list[str]:
    """Load the instrument universe from a JSON file.

    Returns a list of IG epic strings. Only instruments with status
    'verified' are included when status information is present.
    """
    with open(path) as f:
        data = json.load(f)

    instruments = data.get("instruments", [])
    epics = []
    for inst in instruments:
        epic = inst.get("epic", "")
        if not epic:
            continue
        # Include all instruments — the DataPipeline already skips those
        # that fail to return data, so unverified candidates are safe.
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
}

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        model = Model(config)
        model.run()
    except RuntimeError as error:
        if "Missing required IG credentials" not in str(error):
            if "validation.pattern.invalid.authenticationRequest.identifier" not in str(error):
                raise
            _print_ig_authentication_error(error)
            raise SystemExit(1) from None
        _print_credentials_setup_error(error)
        raise SystemExit(1) from None
