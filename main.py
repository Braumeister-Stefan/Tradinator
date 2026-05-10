"""
Tradinator — Entry point.

Defines major configuration parameters and launches the trading engine.
Run with: python main.py

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import argparse
import importlib.util
import json
import os

from dotenv import dotenv_values

from model import Model, RunLoop

ENV_PATH = os.path.join("secrets", ".env")
_env = dotenv_values(ENV_PATH) if os.path.exists(ENV_PATH) else {}

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
        print(
            f"WARNING: No instruments found in {path} — pipeline will run with an empty universe. "
            "Run discover_universe.py (or use --discover) to populate it."
        )

    seen_bases: set[str] = set()
    epics: list[str] = []
    for inst in instruments:
        epic = inst.get("epic", "")
        if not epic:
            continue
        if not inst.get("valid", True):
            print(
                f"WARNING: universe.json contains invalid instrument '{epic}' — skipping. "
                "Run discover_universe.py or edit universe_candidates.json."
            )
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
    parser.add_argument(
        "--discover",
        action="store_true",
        default=False,
        help=(
            "Run universe discovery and validation before the main pipeline. "
            "Validates all candidates in universe_candidates.json against the IG API "
            "(Tier 1: broker recognition + dealing enabled; Tier 2: price data available) "
            "and updates universe.json with only the valid instruments. "
            "Equivalent to setting run_discover=True in config."
        ),
    )
    return parser.parse_args()


def _run_discover(_config: dict) -> None:
    """Invoke discover_universe.main() to validate and refresh the universe.

    Loads ``data/input/discover_universe.py`` via importlib so it is not
    executed at import time and does not pollute the module namespace.
    The ``_config`` parameter is accepted for forward-compatibility (e.g.,
    to pass broker credentials in future) but is not used at present.
    """
    discover_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "data", "input", "discover_universe.py",
    )
    spec = importlib.util.spec_from_file_location("discover_universe", discover_path)
    if spec is None or spec.loader is None:
        print(f"ERROR: cannot load discover_universe from {discover_path}")
        raise SystemExit(1)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()


# ---------------------------------------------------------------------------
# Major parameters — control *what* the engine does each run.
# ---------------------------------------------------------------------------
config = {
    # Broker --------------------------------------------------------------
    "broker": "ig",                     # "ig" or "ibkr" (ibkr is placeholder)

    # Credentials --------------------------------------------------------
    "env_path": "secrets/.env",        # path to .env file with broker creds

    # Universe -----------------------------------------------------------
    "universe_path": UNIVERSE_PATH,     # path to universe JSON file
    "universe": _load_universe(UNIVERSE_PATH),
    "run_discover": False,              # set True or use --discover to re-validate universe

    # Market data --------------------------------------------------------
    "resolution": "DAY",                # price bar resolution
    "lookback": 5,                     # number of bars to fetch

    # Portfolio rules ----------------------------------------------------
    "max_position_pct": 0.25,           # max weight for a single position
    "cash_reserve_pct": 0.05,           # minimum cash to keep unallocated

    # Output -------------------------------------------------------------
    "output_dir": "data/output",        # base directory for all output files
    "max_handoff_age_seconds": 7200,   # max age of handoff file before considered stale

    # Dashboard delivery -------------------------------------------------
    # Set deliver_mode to "ftp" to publish the dashboard to a remote host via
    # FTPS, "netlify" to deploy via the Netlify Files API, "github_pages" to push
    # dashboard_data.json to a GitHub Pages repo via the Contents API, or
    # "file_only" for non-blocking local output without starting an HTTP server.
    # Default "localhost" preserves the current local HTTP server behaviour.
    "deliver_mode": _env.get("DELIVER_MODE", "localhost"),
    "dashboard_data_url": _env.get("DASHBOARD_DATA_URL", "dashboard_data.json"),
    "ftp_host":     _env.get("FTP_HOST", ""),
    "ftp_user":     _env.get("FTP_USER", ""),
    "ftp_password": _env.get("FTP_PASSWORD", ""),
    "ftp_remote_dir": _env.get("FTP_REMOTE_DIR", ""),
    "ftp_json_remote_dir": _env.get("FTP_JSON_REMOTE_DIR", ""),
    "netlify_token":   _env.get("NETLIFY_TOKEN", ""),
    "netlify_site_id": _env.get("NETLIFY_SITE_ID", ""),
    "github_pat":          _env.get("GITHUB_PAT", "").strip(),
    "github_repo":         _env.get("GITHUB_REPO", ""),
    "github_json_path":    _env.get("GITHUB_JSON_PATH", "dashboard_data.json"),
    "github_branch":       _env.get("GITHUB_BRANCH", "main"),
    "github_commit_message": _env.get("GITHUB_COMMIT_MESSAGE", "chore: update dashboard data"),
}

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    args = _parse_args()

    # Merge CLI --discover flag into config
    if args.discover:
        config["run_discover"] = True

    try:
        # --- Universe discovery (optional, gated by config key or --discover) ---
        if config.get("run_discover", False):
            print("\n[main] run_discover=True — running universe validation...")
            _run_discover(config)
            # Reload universe after discover updates universe.json
            config["universe"] = _load_universe(UNIVERSE_PATH)
            print(f"[main] Universe reloaded: {len(config['universe'])} valid instrument(s).\n")

        model = Model(config)
        run_loop = RunLoop(
            model,
            args.mode,
            interval=args.interval,
            research_interval=args.research_interval,
            execution_interval=args.execution_interval,
        )
        run_loop.start()
    except NotImplementedError as error:
        print(f"The selected broker adapter is not yet implemented: {error}")
        raise SystemExit(1) from None
    except RuntimeError as error:
        msg = str(error)
        if "Missing required IG credentials" in msg:
            _print_credentials_setup_error(error)
        elif "validation.pattern.invalid" in msg:
            _print_ig_authentication_error(error)
        else:
            print(f"ERROR: {error}")
        raise SystemExit(1) from None
