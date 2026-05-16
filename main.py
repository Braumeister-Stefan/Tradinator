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

    Returns a deduplicated list of broker-agnostic instrument_id strings.
    Skips entries where valid=False.
    """
    try:
        with open(path) as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Universe file not found: {path}")
        print(
            "Populate data/input/universe.json manually with IBKR canonical symbols "
            "(e.g. 'DAX', 'EURUSD') or IG epics."
        )
        raise SystemExit(1) from None
    except json.JSONDecodeError as exc:
        print(f"ERROR: Universe file contains invalid JSON: {path}")
        print(f"  {exc}")
        raise SystemExit(1) from None

    instruments = data.get("instruments", [])
    if not instruments:
        print(
            f"WARNING: No instruments found in {path} — pipeline will run with an empty universe. "
            "Populate data/input/universe.json manually with instrument_id strings."
        )

    seen: set[str] = set()
    symbols: list[str] = []
    for inst in instruments:
        iid = inst.get("instrument_id") or inst.get("epic", "")
        if not iid:
            continue
        if not inst.get("valid", True):
            print(f"WARNING: universe.json contains invalid=False instrument '{iid}' — skipping.")
            continue
        if iid in seen:
            continue
        seen.add(iid)
        symbols.append(iid)
    return symbols


def _print_credentials_setup_error(error: RuntimeError) -> None:
    """Print the required credential setup steps for first-time runs."""
    print("Tradinator could not start because broker credentials are not configured.")
    print(str(error))
    print("Next steps:")
    print("1. Copy secrets/.env.example to secrets/.env")
    print("2. Fill in the required credentials for your broker — see secrets/.env.example for both IG and IBKR credential keys")
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


def _print_ibkr_connection_error(error: RuntimeError) -> None:
    """Print IBKR-specific connection error guidance."""
    print("Tradinator could not connect to IBKR.")
    print(str(error))
    print("Next steps:")
    print("1. Start TWS or IB Gateway and confirm it listens on port 4002 (paper trading).")
    print("2. Verify IBKR_HOST=127.0.0.1, IBKR_PORT=4002, IBKR_CLIENT_ID=1 in secrets/.env.")
    print("3. Ensure no other session is using the same IBKR_CLIENT_ID.")


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
        "--refresh-universe",
        action="store_true",
        default=False,
        dest="refresh_universe",
        help=(
            "Run universe refresh before the main pipeline. "
            "Discovers all SP500 and FTSE100 instruments via IG search-drilldown, "
            "resolves Yahoo Finance tickers, performs Tier 1 broker validation, "
            "and rewrites universe_candidates.json and universe.json. "
            "Equivalent to setting refresh_universe=True in config. "
            "(IG only; silently skipped when broker=ibkr)"
        ),
    )
    return parser.parse_args()


def _run_refresh_universe(_config: dict) -> bool:
    """Invoke refresh_universe.run() to discover and validate the instrument universe.

    Loads ``diagnostic_tools/refresh_universe.py`` via importlib
    (diagnostic_tools/ has no __init__.py so a normal import is not possible).

    Returns True if the pipeline completed successfully, False if the broker API
    was unavailable (503/500 after retries) and the run was skipped.  In the
    False case the existing universe.json on disk is used unchanged.
    Always returns False immediately when broker=ibkr; this tool is IG-only.
    """
    if _config.get("broker", "ig") == "ibkr":
        print("[main] --refresh-universe is not supported for broker=ibkr — skipping.")
        return False

    refresh_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "diagnostic_tools", "refresh_universe.py",
    )
    spec = importlib.util.spec_from_file_location("refresh_universe", refresh_path)
    if spec is None or spec.loader is None:
        print(f"ERROR: cannot load refresh_universe from {refresh_path}")
        raise SystemExit(1)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.run(_config)


# ---------------------------------------------------------------------------
# Major parameters — control *what* the engine does each run.
# ---------------------------------------------------------------------------
config = {
    # Broker --------------------------------------------------------------
    "broker": "ig",                     # "ig" or "ibkr"

    # Credentials --------------------------------------------------------
    "env_path": "secrets/.env",        # path to .env file with broker creds

    # Universe -----------------------------------------------------------
    "universe_path": UNIVERSE_PATH,     # path to universe JSON file
    "universe": [],          # populated in __main__ after optional refresh
    "refresh_universe": True,  # set True or use --refresh-universe to refresh universe

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

    # Merge CLI --refresh-universe flag into config
    if args.refresh_universe:
        config["refresh_universe"] = True

    try:
        # --- Universe refresh (optional, gated by config key or --refresh-universe) ---
        if config.get("refresh_universe", False):
            print("\n[main] refresh_universe=True -- running universe refresh...")
            refreshed = _run_refresh_universe(config)
            if not refreshed:
                print("[main] Refresh skipped — broker unavailable. Using existing universe.json.\n")

        # Always load universe after optional refresh (errors here if file still missing).
        config["universe"] = _load_universe(UNIVERSE_PATH)
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

    except RuntimeError as error:
        msg = str(error)
        if "Missing required IG credentials" in msg:
            _print_credentials_setup_error(error)
        elif "validation.pattern.invalid" in msg:
            _print_ig_authentication_error(error)
        elif isinstance(error, (ConnectionRefusedError, TimeoutError)) or "IBKR" in msg or "ib_insync" in msg.lower():
            _print_ibkr_connection_error(error)
        else:
            print(f"ERROR: {error}")
        raise SystemExit(1) from None
