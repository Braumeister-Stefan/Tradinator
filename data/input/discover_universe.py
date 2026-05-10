#!/usr/bin/env python3
"""
Tradinator — Universe Discovery & Validation.

Implements two-tier validation against the IG Demo API for all candidate
instruments registered in ``universe_candidates.json``:

  Tier 1 — Broker Recognition
    Calls ``fetch_market_by_epic`` to check whether the broker knows the epic
    and whether dealing is enabled.  Possible outcomes:
      PASS               — epic recognised and dealingEnabled=true
      EPIC_NOT_RECOGNIZED — call failed or returned no data
      DEALING_DISABLED   — epic found but dealingEnabled=false
      API_ERROR          — any other unexpected exception

  Tier 2 — Data Availability
    Only attempted when T1=PASS.  Calls the historical price endpoint and
    verifies that at least one bar with a valid bid or ask price is returned.
    Possible outcomes:
      YES — ≥1 bar with valid prices returned
      NO  — 0 bars, all prices None, or exception during fetch

An instrument is ``valid=true`` only when T1=PASS and T2=YES.

All candidates (pass and fail) are written to ``universe_candidates.json``
with their full validation metadata for human inspection.  Only ``valid=true``
instruments are written to ``universe.json`` for machine consumption by the
trading pipeline.

Usage:
    python data/input/discover_universe.py

    Or via the main pipeline gate:
    python main.py --discover

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
CANDIDATES_PATH = os.path.join(PROJECT_ROOT, "data", "input", "universe_candidates.json")
RATE_LIMIT_DELAY = 0.5   # seconds between API calls (two calls per epic: T1 + T2)
LOOKBACK_BARS = 10
RESOLUTION = "DAY"

# Search terms to discover additional markets via IG's search endpoint.
SEARCH_TERMS = [
    # Indices — Americas
    "US 500", "Wall Street", "US Tech 100", "Russell 2000",
    # Indices — UK / Europe
    "FTSE 100", "FTSE 250", "Germany 40", "France 40", "Euro Stoxx",
    "Netherlands 25", "Spain 35", "Switzerland Blue Chip", "Italy 40",
    # Indices — Asia / Pacific
    "Australia 200", "Japan 225", "Hang Seng",
    # Forex
    "EUR USD", "GBP USD", "USD JPY", "AUD USD", "USD CAD",
    "EUR GBP", "EUR JPY", "USD CHF", "NZD USD",
    # Commodities
    "US Crude", "Brent Crude", "Gold", "Silver", "Natural Gas", "Copper",
    # Individual equities (UK & US)
    "Apple", "Microsoft", "Amazon", "Tesla", "NVIDIA",
    "Meta", "Alphabet", "Netflix",
    "Vodafone", "Barclays", "AstraZeneca", "BP",
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


# ---------------------------------------------------------------------------
# Tier 1 — Broker Recognition
# ---------------------------------------------------------------------------

def _validate_tier1(ig: "IGService", epic: str) -> tuple[str, str]:
    """Check broker recognition and dealing eligibility.

    Returns ``(t1_status, t1_reason)`` where *t1_status* is one of:
      ``PASS``                — epic recognised and dealingEnabled=true
      ``EPIC_NOT_RECOGNIZED`` — call failed or returned no market data
      ``DEALING_DISABLED``    — epic found but dealingEnabled=false in snapshot
      ``API_ERROR``           — any other unexpected exception
    """
    try:
        market = ig.fetch_market_by_epic(epic)
    except Exception as exc:
        return "EPIC_NOT_RECOGNIZED", str(exc)

    if not market:
        return "EPIC_NOT_RECOGNIZED", "fetch_market_by_epic returned empty response"

    snapshot = market.get("snapshot", {})
    dealing_enabled = snapshot.get("dealingEnabled", None)

    if dealing_enabled is False:
        return "DEALING_DISABLED", "dealingEnabled=false in market snapshot"

    reason = (
        "dealingEnabled=true"
        if dealing_enabled
        else "dealingEnabled field absent — assumed tradeable"
    )
    return "PASS", reason


# ---------------------------------------------------------------------------
# Tier 2 — Data Availability
# ---------------------------------------------------------------------------

def _validate_tier2(ig: "IGService", epic: str) -> tuple[str, str]:
    """Check that price data is available for the epic.

    Returns ``(t2_status, t2_reason)`` where *t2_status* is one of:
      ``YES`` — ≥1 bar with a valid bid or ask price
      ``NO``  — 0 bars returned, all prices None, or exception during fetch
    """
    try:
        raw = ig.fetch_historical_prices_by_epic_and_num_points(
            epic, RESOLUTION, LOOKBACK_BARS
        )
        bars = raw.get("prices", [])
        if not bars:
            return "NO", "0 bars returned"

        valid_bars = 0
        for bar in bars:
            cp = bar.get("closePrice")
            if cp and (cp.get("bid") is not None or cp.get("ask") is not None):
                valid_bars += 1

        if valid_bars == 0:
            return "NO", f"{len(bars)} bars returned but all prices None"

        return "YES", f"{valid_bars}/{len(bars)} bars with valid prices"

    except Exception as exc:
        return "NO", f"exception: {exc}"


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

def _load_candidates() -> dict:
    """Load universe_candidates.json; return empty registry if absent."""
    if not os.path.isfile(CANDIDATES_PATH):
        return {
            "description": (
                "Tradinator universe candidate registry. "
                "Contains all candidate instruments with two-tier validation metadata. "
                "Edit this file to add new candidates; do not edit universe.json directly."
            ),
            "last_discover_run": None,
            "candidates": [],
        }
    with open(CANDIDATES_PATH) as f:
        return json.load(f)


def _save_candidates(data: dict) -> None:
    """Write updated universe_candidates.json."""
    with open(CANDIDATES_PATH, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    print(f"Saved {len(data['candidates'])} candidates to {CANDIDATES_PATH}")


def _load_universe() -> dict:
    """Load universe.json or return a default structure if absent."""
    if not os.path.isfile(UNIVERSE_PATH):
        return {"description": "", "instruments": []}
    with open(UNIVERSE_PATH) as f:
        return json.load(f)


def _save_universe(data: dict) -> None:
    """Write the updated universe.json (valid instruments only)."""
    with open(UNIVERSE_PATH, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    print(f"Saved {len(data['instruments'])} valid instruments to {UNIVERSE_PATH}")


# ---------------------------------------------------------------------------
# Phase 2 — Discover additional markets via IG search
# ---------------------------------------------------------------------------

def _discover_via_search(
    ig: "IGService",
    known_epics: set,
    validated: list,
    now_utc: str,
) -> list[dict]:
    """Use IG search endpoint to find additional working markets.

    Newly discovered epics are treated as implicit T1 PASS (their presence in
    the search results confirms broker recognition); T2 is still tested via a
    full price-bar fetch.  Newly added candidates are appended to *validated*
    and also returned as a list.
    """
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
            if not epic or epic in known_epics:
                continue
            known_epics.add(epic)

            time.sleep(RATE_LIMIT_DELAY)
            t2_status, t2_reason = _validate_tier2(ig, epic)
            is_valid = (t2_status == "YES")

            symbol = "✓" if is_valid else "✗"
            name = mkt.get("instrumentName", epic)
            print(f"  {symbol} {epic} ({name}) — T2={t2_status}: {t2_reason}")

            candidate = {
                "epic": epic,
                "name": name,
                "asset_class": "unknown",
                "region": "unknown",
                "t1_status": "PASS",
                "t1_reason": "discovered via IG search — broker recognition implicit",
                "t2_status": t2_status,
                "t2_reason": t2_reason,
                "valid": is_valid,
                "last_validated": now_utc,
            }
            validated.append(candidate)
            if is_valid:
                discovered.append(candidate)

    return discovered


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ig = _connect()
    candidates_data = _load_candidates()
    candidates = candidates_data.get("candidates", [])

    if not candidates:
        print("No candidates found in universe_candidates.json. Nothing to validate.")
        return

    print(f"\n=== Phase 1: Validating {len(candidates)} candidates (T1 + T2) ===")
    validated: list[dict] = []
    t1_pass_count = 0
    t2_pass_count = 0
    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    for inst in candidates:
        epic = inst.get("epic", "").strip()
        name = inst.get("name", epic)

        if not epic:
            print("  ✗ (skipped entry with missing epic)")
            inst.update({
                "t1_status": "UNTESTED",
                "t1_reason": "missing epic field",
                "t2_status": "NEVER_TRIED",
                "t2_reason": None,
                "valid": False,
                "last_validated": now_utc,
            })
            validated.append(inst)
            continue

        # --- Tier 1: broker recognition + dealing eligibility ---
        time.sleep(RATE_LIMIT_DELAY)
        t1_status, t1_reason = _validate_tier1(ig, epic)
        t1_symbol = "✓" if t1_status == "PASS" else "✗"
        print(f"  T1 {t1_symbol} {epic} ({name}) — {t1_status}: {t1_reason}")

        if t1_status != "PASS":
            inst.update({
                "t1_status": t1_status,
                "t1_reason": t1_reason,
                "t2_status": "NEVER_TRIED",
                "t2_reason": None,
                "valid": False,
                "last_validated": now_utc,
            })
            validated.append(inst)
            continue

        t1_pass_count += 1

        # --- Tier 2: price data availability ---
        time.sleep(RATE_LIMIT_DELAY)
        t2_status, t2_reason = _validate_tier2(ig, epic)
        t2_symbol = "✓" if t2_status == "YES" else "✗"
        print(f"  T2 {t2_symbol} {epic} ({name}) — {t2_status}: {t2_reason}")

        is_valid = (t2_status == "YES")
        if is_valid:
            t2_pass_count += 1

        inst.update({
            "t1_status": t1_status,
            "t1_reason": t1_reason,
            "t2_status": t2_status,
            "t2_reason": t2_reason,
            "valid": is_valid,
            "last_validated": now_utc,
        })
        validated.append(inst)

    print(f"\nPhase 1 complete: {t1_pass_count} T1-pass, {t2_pass_count} T2-pass (fully valid).")

    # --- Phase 2: discover additional epics via IG search if < 20 valid ---
    known_epics = {c.get("epic", "") for c in validated}
    valid_count = sum(1 for c in validated if c.get("valid"))

    if valid_count < 20:
        print(
            f"\n=== Phase 2: Discovering additional markets "
            f"(have {valid_count}/20 valid) ==="
        )
        discovered = _discover_via_search(ig, known_epics, validated, now_utc)
        print(f"Phase 2 added {len(discovered)} new valid instrument(s).")

    # --- Save candidates file (all candidates, pass and fail) ---
    candidates_data["candidates"] = validated
    candidates_data["last_discover_run"] = now_utc
    _save_candidates(candidates_data)

    # --- Build and save universe.json (valid instruments only) ---
    universe_data = _load_universe()
    valid_instruments = [
        {
            "epic": c["epic"],
            "name": c["name"],
            "asset_class": c["asset_class"],
            "region": c["region"],
            "valid": True,
        }
        for c in validated
        if c.get("valid")
    ]
    universe_data["instruments"] = valid_instruments
    universe_data["description"] = (
        "Tradinator instrument universe — IG Demo epics that have passed two-tier validation "
        "(T1: broker recognition + dealing enabled; T2: price data available). "
        f"Last validated: {now_utc}."
    )
    _save_universe(universe_data)

    valid_total = sum(1 for c in validated if c.get("valid"))
    print(f"\nValidation complete: {t1_pass_count} T1-pass, {valid_total} fully valid (T1+T2).")
    if valid_total < 20:
        print(
            f"WARNING: Only {valid_total} valid epics found (target: 20). "
            "Consider adding more candidates to universe_candidates.json."
        )
    else:
        print(f"SUCCESS: {valid_total} valid epics in universe.")


if __name__ == "__main__":
    main()
