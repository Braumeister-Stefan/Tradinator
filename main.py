"""
Tradinator — Entry point.

Defines major configuration parameters and launches the trading engine.
Run with: python main.py

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

from model import Model


def _print_credentials_setup_error(error: RuntimeError) -> None:
    """Print the required credential setup steps for first-time runs."""
    print("Tradinator could not start because broker credentials are not configured.")
    print(str(error))
    print("Next steps:")
    print("1. Copy secrets/.env.example to secrets/.env")
    print("2. Fill in the required credentials for your broker (default: IG)")
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
    # Broker --------------------------------------------------------------
    "broker": "ig",                     # "ig" or "ibkr" (ibkr is placeholder)

    # Credentials --------------------------------------------------------
    "env_path": "secrets/.env",        # path to .env file with broker creds

    # Universe -----------------------------------------------------------
    "universe": [                       # instrument IDs (IG epics for IG broker)
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
    try:
        model = Model(config)
        model.run()
    except NotImplementedError as error:
        print(f"The selected broker adapter is not yet implemented: {error}")
        raise SystemExit(1) from None
    except RuntimeError as error:
        if "Missing required IG credentials" not in str(error):
            if "validation.pattern.invalid.authenticationRequest.identifier" not in str(error):
                raise
            _print_ig_authentication_error(error)
            raise SystemExit(1) from None
        _print_credentials_setup_error(error)
        raise SystemExit(1) from None
