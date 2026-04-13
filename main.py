"""
Tradinator — Entry point.

Defines major configuration parameters and launches the trading engine.
Run with: python main.py

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

from model import Model

# ---------------------------------------------------------------------------
# Major parameters — control *what* the engine does each run.
# ---------------------------------------------------------------------------
config = {
    # Credentials --------------------------------------------------------
    "env_path": "secrets/.env",        # path to .env file with IG creds

    # Universe -----------------------------------------------------------
    "universe": [                       # IG epics for the equity spot universe
        "CS.D.AAPL.CFD.IP",            # Apple
        "CS.D.MSFT.CFD.IP",            # Microsoft
        "CS.D.GOOGL.CFD.IP",           # Alphabet
        "CS.D.AMZN.CFD.IP",            # Amazon
        "CS.D.TSLA.CFD.IP",            # Tesla
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
    model = Model(config)
    model.run()
