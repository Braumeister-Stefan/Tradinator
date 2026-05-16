"""
Tradinator — Entry point.

Defines major configuration parameters and launches the trading engine.
Run with: python main.py

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import os

from model import Model, RunLoop, parse_run_args
from model.config_loader import load_env_config
from model.model_components.data_pipeline import load_universe

ENV_PATH = os.path.join("secrets", ".env")

# ---------------------------------------------------------------------------
# Major parameters — control *what* the engine does each run.
# ---------------------------------------------------------------------------
config = {
    # Broker --------------------------------------------------------------
    "broker": "ibkr",                   # "ibkr" or future supported broker

    # Credentials --------------------------------------------------------
    "env_path": ENV_PATH,               # path to .env file with broker creds

    # Universe -----------------------------------------------------------
    "universe_path": os.path.join("data", "input", "universe.json"),
    "universe_candidates_path": os.path.join("data", "input", "universe_candidates.json"),
    "refresh_universe": True,          # True = validate candidates → universe.json on startup
    "universe": [],                     # populated in __main__

    # Market data --------------------------------------------------------
    "resolution": "DAY",                # price bar resolution
    "lookback": 5,                      # number of bars to fetch

    # Portfolio rules ----------------------------------------------------
    "max_position_pct": 0.25,           # max weight for a single position
    "cash_reserve_pct": 0.05,           # minimum cash to keep unallocated

    # Output -------------------------------------------------------------
    "output_dir": "data/output",        # base directory for all output files
    "max_handoff_age_seconds": 7200,    # max age of handoff file before stale
}

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    args = parse_run_args()

    try:
        config.update(load_env_config(config["env_path"]))
        config["universe"] = load_universe(config["universe_path"])
        print(f"[main] Universe loaded: {len(config['universe'])} valid instrument(s).\n")

        model = Model(config)
        run_loop = RunLoop(
            model=model,
            mode=args.mode,
            interval=args.interval,
            research_interval=args.research_interval,
            execution_interval=args.execution_interval,
        )
        run_loop.start()
    except NotImplementedError as error:
        print(f"Selected broker adapter not implemented yet. {error}")
    except (RuntimeError, OSError) as error:
        print(f"ERROR: {error}")
        raise SystemExit(1) from None
