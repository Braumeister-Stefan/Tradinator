#!/usr/bin/env python3
"""
Tradinator — Universe Discovery & Validation.

Connects to the IG Demo API, tests each candidate epic in universe.json,
and updates the file with only the epics that return valid price data.
Also searches for additional working markets via the IG search endpoint.

Usage:
    1. Ensure IG credentials are set (secrets/.env or environment variables)
    2. python data/input/discover_universe.py

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import json
import os
import sys
import time

# Ensure project root is importable
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
from trading_ig import IGService

UNIVERSE_PATH = os.path.join(PROJECT_ROOT, "data", "input", "universe.json")
RATE_LIMIT_DELAY = 0.3  # seconds between API calls
LOOKBACK_BARS = 10       # minimal bars to test availability
RESOLUTION = "DAY"

# Search terms to discover additional markets via IG's search endpoint.
SEARCH_TERMS = [
    "Germany 40", "FTSE 100", "Wall Street", "US 500", "US Tech 100",
    "France 40", "Australia 200", "Japan 225", "Hang Seng", "Euro Stoxx",
    "Russell 2000", "Netherlands 25", "Spain 35", "Switzerland Blue Chip",
    "Italy 40", "FTSE 250",
    "Apple", "Microsoft", "Amazon", "Tesla", "Google", "NVIDIA",
    "Meta", "Netflix", "Vodafone", "Barclays", "AstraZeneca", "BP",
]


def _connect() -> "IGService":
    """Create and return an authenticated IG session."""
    env_path = os.path.join(PROJECT_ROOT, "secrets", ".env")
    if os.path.isfile(env_path):
        load_dotenv(env_path)

    username = os.environ.get("IG_USERNAME", "").strip()
    password = os.environ.get("IG_PASSWORD", "").strip()
    api_key = os.environ.get("IG_API_KEY", "").strip()
    acc_type = os.environ.get("IG_ACC_TYPE", "DEMO")
    acc_number = os.environ.get("IG_ACC_NUMBER")

    if acc_type.upper() != "DEMO":
        print(
            "ERROR: Tradinator is restricted to paper trading (DEMO) only. "
            f"IG_ACC_TYPE is set to '{acc_type}'. Set it to 'DEMO' or remove it."
        )
        raise SystemExit(1)

    if not all([username, password, api_key]):
        print("ERROR: Missing IG credentials. Set IG_USERNAME, IG_PASSWORD, IG_API_KEY.")
        raise SystemExit(1)

    ig = IGService(
        username, password, api_key,
        acc_type=acc_type,
        acc_number=acc_number,
        return_dataframe=False,
        return_munch=False,
    )
    ig.create_session(version="2")
    print(f"Connected to IG {acc_type.upper()}")
    return ig


def _test_epic(ig: "IGService", epic: str) -> tuple[bool, str]:
    """Return (works, detail) for a single epic."""
    try:
        raw = ig.fetch_historical_prices_by_epic_and_num_points(
            epic, RESOLUTION, LOOKBACK_BARS
        )
        bars = raw.get("prices", [])
        if not bars:
            return False, "no price bars"

        for bar in bars:
            cp = bar.get("closePrice")
            if cp and (cp.get("bid") is not None or cp.get("ask") is not None):
                return True, f"{len(bars)} bars OK"

        return False, f"{len(bars)} bars, all None"
    except Exception as exc:
        return False, str(exc)


def _load_universe() -> dict:
    """Load the current universe.json."""
    with open(UNIVERSE_PATH) as f:
        return json.load(f)


def _save_universe(data: dict) -> None:
    """Write the updated universe.json."""
    with open(UNIVERSE_PATH, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    print(f"\nSaved {len(data['instruments'])} instruments to {UNIVERSE_PATH}")


def _discover_via_search(ig: "IGService", known_epics: set) -> list[dict]:
    """Use IG search endpoint to find additional working markets."""
    discovered = []
    for term in SEARCH_TERMS:
        time.sleep(RATE_LIMIT_DELAY)
        try:
            results = ig.search_markets(term)
            markets = results.get("markets", [])
        except Exception as exc:
            print(f"  Search '{term}' failed: {exc}")
            continue

        for mkt in markets[:5]:
            epic = mkt.get("epic", "")
            if epic in known_epics:
                continue
            known_epics.add(epic)

            time.sleep(RATE_LIMIT_DELAY)
            ok, detail = _test_epic(ig, epic)
            name = mkt.get("instrumentName", epic)
            symbol = "✓" if ok else "✗"
            print(f"  {symbol} {epic} ({name}) — {detail}")
            if ok:
                discovered.append({"epic": epic, "name": name, "status": "verified"})

    return discovered


def main() -> None:
    ig = _connect()
    data = _load_universe()
    instruments = data.get("instruments", [])

    # --- Phase 1: validate existing candidates ---
    print("\n=== Phase 1: Validating existing candidates ===")
    verified = []
    for inst in instruments:
        epic = inst.get("epic", "")
        if not epic:
            print("  ✗ (skipped entry with missing epic)")
            continue
        time.sleep(RATE_LIMIT_DELAY)
        ok, detail = _test_epic(ig, epic)
        symbol = "✓" if ok else "✗"
        print(f"  {symbol} {epic} ({inst.get('name', '')}) — {detail}")
        if ok:
            inst["status"] = "verified"
            verified.append(inst)

    print(f"\n{len(verified)}/{len(instruments)} candidates verified.")

    # --- Phase 2: discover additional epics if < 20 verified ---
    known_epics = {inst.get("epic", "") for inst in verified}
    if len(verified) < 20:
        print(f"\n=== Phase 2: Discovering additional markets (need {20 - len(verified)} more) ===")
        discovered = _discover_via_search(ig, known_epics)
        verified.extend(discovered)
        print(f"\nTotal verified: {len(verified)}")

    # --- Save results ---
    data["instruments"] = verified
    data["description"] = (
        "Tradinator instrument universe — IG Demo epics validated to return data. "
        f"Last validated: {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}."
    )
    _save_universe(data)

    if len(verified) < 20:
        print(f"\nWARNING: Only {len(verified)} verified epics found (target: 20).")
        print("Consider running again or manually adding epics via IG's market search.")
    else:
        print(f"\nSUCCESS: {len(verified)} verified epics in universe.")


if __name__ == "__main__":
    main()
