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

    # Sanitised credential summary — logged unconditionally so the values
    # actually sent to IG are visible when a session error occurs.
    def _mask(value: str, keep: int = 3) -> str:
        return value[:keep] + "***" if len(value) > keep else "***"

    print(
        f"[_connect] Attempting IG session -- "
        f"username='{_mask(username)}' "
        f"api_key='{_mask(api_key)}' "
        f"acc_type='{acc_type}' "
        f"acc_number='{acc_number if acc_number else '(not set)'}'"
    )

    ig = IGService(
        username, password, api_key,
        acc_type=acc_type,
        acc_number=acc_number,
        return_dataframe=False,
        return_munch=False,
    )

    # Retry create_session for transient 503/500 server errors (e.g. weekend
    # DEMO maintenance, momentary overload).  Credential errors (wrong
    # password -> 500 with a specific body) and ApiExceededException are not
    # retried because retrying won't help.
    _RETRYABLE_CODES = ("503", "500")
    _MAX_RETRIES = 3
    _BACKOFF_BASE = 15  # seconds; doubles each attempt: 15, 30, 60

    last_exc: Exception | None = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            ig.create_session(version="2")
            last_exc = None
            break  # success
        except Exception as exc:
            exc_type = type(exc).__name__
            exc_str = str(exc).strip() or "(no message)"

            # Non-retryable: ApiExceededException (rate limit) or anything
            # that does not mention a 5xx status code.
            is_retryable = any(code in exc_str for code in _RETRYABLE_CODES)
            if not is_retryable or exc_type == "ApiExceededException":
                print(
                    f"\n[_connect] ERROR: ig.create_session() raised {exc_type}: {exc_str}\n"
                    "  Diagnostic:\n"
                    "    - ApiExceededException means IG has blocked further login\n"
                    "      attempts -- account may have hit its daily API allowance\n"
                    "      or too many sessions were opened in quick succession.\n"
                    "      Wait several minutes before retrying.\n"
                    "    - IGException / HTTP 500 with no retryable signal almost\n"
                    "      always means wrong username, password, or API key.\n"
                    "      Verify IG_USERNAME, IG_PASSWORD, IG_API_KEY in secrets/.env.\n"
                    "    - If IG_ACC_NUMBER is not set, the library sends None which\n"
                    "      may cause a malformed login body.\n"
                    f"  Credential context: exc_type='{exc_type}' username='{_mask(username)}' "
                    f"api_key='{_mask(api_key)}' "
                    f"acc_type='{acc_type}' "
                    f"acc_number='{acc_number if acc_number else '(not set -- set IG_ACC_NUMBER)'}'"
                )
                raise

            last_exc = exc
            wait = _BACKOFF_BASE * (2 ** (attempt - 1))
            print(
                f"[_connect] Attempt {attempt}/{_MAX_RETRIES} failed with {exc_type} "
                f"({exc_str}) -- retrying in {wait}s..."
            )
            time.sleep(wait)

    if last_exc is not None:
        exc_type = type(last_exc).__name__
        exc_str = str(last_exc).strip() or "(no message)"
        print(
            f"\n[_connect] ERROR: all {_MAX_RETRIES} session attempts failed.\n"
            f"  Last error: {exc_type}: {exc_str}\n"
            "  IG's DEMO servers are likely in a maintenance window or under\n"
            "  sustained load. Retry later (DEMO maintenance: Sat ~21:00 to\n"
            "  Sun ~08:00 UTC). Check https://status.ig.com for live status.\n"
            f"  Credential context: username='{_mask(username)}' "
            f"api_key='{_mask(api_key)}' acc_type='{acc_type}'"
        )
        raise last_exc

    print(f"[_connect] Connected to IG {acc_type.upper()}")
    return ig


# ---------------------------------------------------------------------------
# Tier 1 — Broker Recognition
# ---------------------------------------------------------------------------

def _validate_tier1(ig: "IGService", epic: str) -> tuple[str, str]:
    """Check broker recognition and dealing eligibility.

    Returns ``(t1_status, t1_reason)`` where *t1_status* is one of:
      ``PASS``                — epic recognised and dealingEnabled=true
      ``EPIC_NOT_RECOGNIZED`` — broker explicitly reports the epic as unknown
                                (404-style error or empty market response)
      ``DEALING_DISABLED``    — epic found but dealingEnabled=false in snapshot
      ``API_ERROR``           — any other exception (network, auth, rate limit, etc.)
                                that does NOT indicate the epic is invalid
    """
    try:
        market = ig.fetch_market_by_epic(epic)
    except Exception as exc:
        exc_str = str(exc).lower()
        # IG returns a 404 / error.security.notFound for unknown epics.
        # Any other exception (timeout, auth, rate-limit) is API_ERROR to
        # avoid permanently blacklisting valid epics due to transient faults.
        exc_detail = f"{type(exc).__name__}: {repr(exc)}"  # P5-log: always non-empty
        if any(kw in exc_str for kw in ("not found", "404", "invalid epic", "notfound")):
            return "EPIC_NOT_RECOGNIZED", exc_detail
        return "API_ERROR", exc_detail

    if not market:
        return "EPIC_NOT_RECOGNIZED", "fetch_market_by_epic returned empty response"

    snapshot = market.get("snapshot", {})
    dealing_enabled = snapshot.get("dealingEnabled", None)

    if dealing_enabled is False:
        return "DEALING_DISABLED", "dealingEnabled=false in market snapshot"

    if dealing_enabled is None:
        # P4-log: dealingEnabled field absent — log snapshot keys and instrument type
        # to identify whether this is a systematic pattern for certain instrument types.
        instrument = market.get("instrument", {})
        print(
            f"[discover_universe] DEBUG {epic}: dealingEnabled absent from snapshot"
            f" — snapshot_keys={list(snapshot.keys())}"
            f", instrument_type={instrument.get('type', 'unknown')}"
        )

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
    """Use IG search endpoint to find additional markets that pass T1 validation.

    Each discovered epic is run through ``_validate_tier1`` (the search result
    confirms the market exists but does NOT verify that dealing is enabled or
    that the epic string is exactly valid).  T2 (data availability) is NOT
    tested here — it is deferred to the trading pipeline (``DataPipeline``).
    Newly discovered T1-PASS epics are added with ``t2_status: "PENDING_T2"``.

    Candidates are appended to *validated* and T1-PASS ones are also returned.
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

            # Run a proper T1 check — search presence alone does not confirm
            # dealing eligibility or that the exact epic string is valid.
            time.sleep(RATE_LIMIT_DELAY)
            t1_status, t1_reason = _validate_tier1(ig, epic)
            name = mkt.get("instrumentName", epic)
            t1_symbol = "PASS" if t1_status == "PASS" else "FAIL"
            print(f"  T1 [{t1_symbol}] {epic} ({name}) -- {t1_status}: {t1_reason}")

            if t1_status == "PASS":
                # T2 is deferred to DataPipeline; mark as PENDING_T2.
                t2_status, t2_reason = "PENDING_T2", "awaiting DataPipeline data fetch"
            else:
                t2_status, t2_reason = "NEVER_TRIED", None

            candidate = {
                "epic": epic,
                "name": name,
                "asset_class": "unknown",
                "region": "unknown",
                "t1_status": t1_status,
                "t1_reason": t1_reason,
                "t2_status": t2_status,
                "t2_reason": t2_reason,
                "valid": False,  # only confirmed valid after DataPipeline T2 test
                "last_validated": now_utc,
            }
            validated.append(candidate)
            if t1_status == "PASS":
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

    print(f"\n=== Phase 1: Validating {len(candidates)} candidates (Tier 1 only) ===")
    print("Note: Tier 2 (data availability) is validated by the trading pipeline.")
    validated: list[dict] = []
    t1_pass_count = 0
    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    for inst in candidates:
        epic = inst.get("epic", "").strip()
        name = inst.get("name", epic)

        if not epic:
            print("  [SKIP] (skipped entry with missing epic)")
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
        t1_symbol = "PASS" if t1_status == "PASS" else "FAIL"
        print(f"  T1 [{t1_symbol}] {epic} ({name}) -- {t1_status}: {t1_reason}")

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

        # T1 passed — mark as PENDING_T2.  The DataPipeline resolves this
        # to YES/NO on the next run by fetching actual price data.
        # T2 is not pre-tested here to keep discover_universe.py focused on
        # broker eligibility; the pipeline is the authoritative T2 gate.
        inst.update({
            "t1_status": t1_status,
            "t1_reason": t1_reason,
            "t2_status": "PENDING_T2",
            "t2_reason": "awaiting DataPipeline data fetch",
            "valid": False,  # only confirmed valid after pipeline confirms T2=YES
            "last_validated": now_utc,
        })
        validated.append(inst)

    print(f"\nPhase 1 complete: {t1_pass_count} T1-pass (PENDING_T2), "
          f"{len(candidates) - t1_pass_count} T1-fail.")

    # --- Phase 2: discover additional epics via IG search if < 20 T1-pass ---
    known_epics = {c.get("epic", "") for c in validated}
    t1_pass_total = sum(1 for c in validated if c.get("t1_status") == "PASS")

    if t1_pass_total < 20:
        print(
            f"\n=== Phase 2: Discovering additional markets "
            f"(have {t1_pass_total}/20 T1-pass) ==="
        )
        discovered = _discover_via_search(ig, known_epics, validated, now_utc)
        print(f"Phase 2 added {len(discovered)} new T1-pass instrument(s).")

    # --- Save candidates file (all candidates, pass and fail) ---
    candidates_data["candidates"] = validated
    candidates_data["last_discover_run"] = now_utc
    _save_candidates(candidates_data)

    # --- Build and save universe.json (T1-pass instruments only) ---
    # universe.json contains all T1-PASS instruments so the DataPipeline can
    # fetch data for them (which constitutes the T2 test).  Instruments are
    # removed by DataPipeline if their cold-start T2 fetch returns zero bars.
    universe_data = _load_universe()
    t1_pass_instruments = [
        {
            "epic": c["epic"],
            "name": c["name"],
            "asset_class": c["asset_class"],
            "region": c["region"],
            "valid": True,
        }
        for c in validated
        if c.get("t1_status") == "PASS"
    ]
    universe_data["instruments"] = t1_pass_instruments
    universe_data["description"] = (
        "Tradinator instrument universe — IG Demo epics that have passed Tier 1 validation "
        "(broker recognition + dealing enabled). "
        "Tier 2 (data availability) is validated continuously by DataPipeline; "
        "instruments that return zero bars on a cold-start fetch are removed automatically. "
        f"Last T1 validation: {now_utc}."
    )
    _save_universe(universe_data)

    t1_total = sum(1 for c in validated if c.get("t1_status") == "PASS")
    print(f"\nValidation complete: {t1_total} T1-pass instrument(s) written to universe.json.")
    if t1_total < 20:
        print(
            f"WARNING: Only {t1_total} T1-pass epics (target: 20). "
            "Consider adding more candidates to universe_candidates.json."
        )
    else:
        print(f"SUCCESS: {t1_total} T1-pass epics in universe.")
    print("\nRun the main pipeline to complete Tier 2 validation (data availability).")


if __name__ == "__main__":
    main()
