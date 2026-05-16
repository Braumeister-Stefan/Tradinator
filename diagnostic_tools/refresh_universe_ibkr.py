#!/usr/bin/env python3
"""
Tradinator — refresh_universe_ibkr.py

Validates a manually-curated IBKR instrument universe by connecting to
IB Gateway (or TWS) and calling reqContractDetails on each candidate.

Pipeline phases
---------------
  Phase 1 — Load    : Read universe_candidates.json for the list of candidates.
                       If the file does not exist or has no candidates, fall back
                       to SEED_INSTRUMENTS (a hard-coded list of canonical IBKR
                       symbols for major equity indices).

  Phase 2 — Enrich  : Resolve Yahoo Finance ticker for each instrument.
                       Priority: INSTRUMENT_TO_YH_TICKER map → Yahoo Search API.

  Phase 3 — Validate: T1 check — call reqContractDetails and confirm the contract
                       resolves to exactly one result and tradingHours is non-empty.

  Phase 4 — Write   : universe_candidates.json (all candidates + metadata) and
                       universe.json (T1-PASS instruments only, valid=True).
                       Writes atomically after all phases complete.

Usage
-----
  python diagnostic_tools/refresh_universe_ibkr.py   # standalone

Or callable from main.py via importlib:
  success = mod.run(config)

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv  # noqa: E402

try:
    from ib_insync import IB, Contract, util as ib_util  # noqa: E402
    _IB_AVAILABLE = True
except ImportError:
    _IB_AVAILABLE = False

import yfinance as yf  # noqa: E402
from model.model_components.yh_finance_fetcher import INSTRUMENT_TO_YH_TICKER  # noqa: E402

# ---------------------------------------------------------------------------
# IBKR connection defaults
# ---------------------------------------------------------------------------
IBKR_PAPER_PORT = 4002  # IB Gateway / TWS paper-trading port

# ---------------------------------------------------------------------------
# Output paths (same as IG version)
# ---------------------------------------------------------------------------
CANDIDATES_PATH = os.path.join(PROJECT_ROOT, "data", "input", "universe_candidates.json")
UNIVERSE_PATH   = os.path.join(PROJECT_ROOT, "data", "input", "universe.json")

# ---------------------------------------------------------------------------
# Phase 1 — Seed instruments (fallback when no candidates file exists)
# These are canonical IBKR symbols for major global equity indices.
# ---------------------------------------------------------------------------
SEED_INSTRUMENTS = [
    # North America
    {"instrument_id": "SPX",   "name": "S&P 500",          "sec_type": "IND", "exchange": "CBOE",    "currency": "USD"},
    {"instrument_id": "INDU",  "name": "Dow Jones",         "sec_type": "IND", "exchange": "CME",     "currency": "USD"},
    {"instrument_id": "NDX",   "name": "Nasdaq 100",        "sec_type": "IND", "exchange": "NASDAQ",  "currency": "USD"},
    {"instrument_id": "RUT",   "name": "Russell 2000",      "sec_type": "IND", "exchange": "RUSSELL", "currency": "USD"},
    {"instrument_id": "TSX",   "name": "S&P/TSX Composite", "sec_type": "IND", "exchange": "TSX",     "currency": "CAD"},
    # Europe
    {"instrument_id": "DAX",   "name": "DAX 40",            "sec_type": "IND", "exchange": "EUREX",   "currency": "EUR"},
    {"instrument_id": "CAC40", "name": "CAC 40",            "sec_type": "IND", "exchange": "MONEP",   "currency": "EUR"},
    {"instrument_id": "FTSE",  "name": "FTSE 100",          "sec_type": "IND", "exchange": "LIFFE",   "currency": "GBP"},
    {"instrument_id": "IBEX35","name": "IBEX 35",           "sec_type": "IND", "exchange": "MEFFRV",  "currency": "EUR"},
    {"instrument_id": "SMI",   "name": "Swiss Market Index","sec_type": "IND", "exchange": "SOFFEX",  "currency": "CHF"},
    {"instrument_id": "AEX",   "name": "AEX",               "sec_type": "IND", "exchange": "FTA",     "currency": "EUR"},
    {"instrument_id": "SX5E",  "name": "Euro STOXX 50",     "sec_type": "IND", "exchange": "EUREX",   "currency": "EUR"},
    # Asia-Pacific
    {"instrument_id": "N225",  "name": "Nikkei 225",        "sec_type": "IND", "exchange": "OSE.JPN", "currency": "JPY"},
    {"instrument_id": "HSI",   "name": "Hang Seng",         "sec_type": "IND", "exchange": "HKFE",    "currency": "HKD"},
    {"instrument_id": "AS51",  "name": "ASX 200",           "sec_type": "IND", "exchange": "ASX",     "currency": "AUD"},
    # Forex majors (CASH)
    {"instrument_id": "EURUSD","name": "EUR/USD",            "sec_type": "CASH","exchange": "IDEALPRO","currency": "USD"},
    {"instrument_id": "GBPUSD","name": "GBP/USD",            "sec_type": "CASH","exchange": "IDEALPRO","currency": "USD"},
    {"instrument_id": "USDJPY","name": "USD/JPY",            "sec_type": "CASH","exchange": "IDEALPRO","currency": "JPY"},
]

# ---------------------------------------------------------------------------
# Phase 2 — Yahoo ticker resolution
# ---------------------------------------------------------------------------
_OVERLAP_THRESHOLD = 0.25
_SEARCH_DELAY_S    = 0.35
_STOPWORDS = frozenset({"the", "a", "of", "and", "for", "in", "on", "to", "by", "at"})
_TRAILING_NOISE = re.compile(r"[\s\-]+(?:[A-Z]{2,5}[a-z]?|\(\S+\))$")


def _clean_name(name: str) -> str:
    """Strip trailing exchange/region suffixes from an instrument name."""
    cleaned = _TRAILING_NOISE.sub("", name).strip()
    cleaned = re.sub(r"\s*-\s*$", "", cleaned).strip()
    return cleaned if cleaned else name


def _word_tokens(text: str) -> set[str]:
    """Return lowercase word tokens from text, excluding stopwords."""
    return set(re.findall(r"[a-z0-9]+", text.lower())) - _STOPWORDS


def _jaccard(a: str, b: str) -> float:
    """Compute Jaccard similarity between the word-token sets of two strings."""
    ta, tb = _word_tokens(a), _word_tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _search_yahoo(name: str) -> str | None:
    """Query Yahoo Finance Search API and return the best-matching ticker symbol."""
    try:
        results = yf.Search(name, max_results=6, enable_fuzzy_query=True).quotes
    except Exception as exc:
        print(f"    [Yahoo search error] {exc}")
        return None
    if not results:
        return None
    best_symbol, best_score = None, 0.0
    for r in results:
        candidate_name = r.get("longname") or r.get("shortname") or ""
        score = _jaccard(name, candidate_name)
        if score > best_score:
            best_score  = score
            best_symbol = r.get("symbol")
    if best_score < _OVERLAP_THRESHOLD:
        return None
    return best_symbol


def _find_ticker(instrument_id: str, name: str) -> tuple[str, bool]:
    """Return (ticker_or_NONE, did_search)."""
    if instrument_id in INSTRUMENT_TO_YH_TICKER:
        return INSTRUMENT_TO_YH_TICKER[instrument_id], False
    ticker = _search_yahoo(_clean_name(name))
    return (ticker if ticker else "NONE"), True


# ---------------------------------------------------------------------------
# Phase 3 — T1 Validation via IBKR reqContractDetails
# ---------------------------------------------------------------------------

def _build_contract(candidate: dict) -> "Contract":
    """Build an ib_insync Contract from a candidate dict."""
    sec_type = candidate.get("sec_type", "IND")
    symbol   = candidate["instrument_id"]
    exchange = candidate.get("exchange", "SMART")
    currency = candidate.get("currency", "USD")

    if sec_type == "CASH":
        # Forex: split "EURUSD" → symbol="EUR", currency="USD"
        if len(symbol) == 6:
            return Contract(
                symbol=symbol[:3],
                secType="CASH",
                exchange=exchange,
                currency=symbol[3:],
            )
    return Contract(
        symbol=symbol,
        secType=sec_type,
        exchange=exchange,
        currency=currency,
    )


def _validate_tier1(ib: "IB", candidate: dict) -> tuple[str, str]:
    """Call reqContractDetails; return (t1_status, t1_reason).

    Statuses:
      PASS              — contract resolves and tradingHours is non-empty
      CONTRACT_NOT_FOUND — no contract details returned
      DEALING_DISABLED  — contract found but tradingHours empty
      API_ERROR         — exception raised
    """
    try:
        contract = _build_contract(candidate)
        details  = ib.reqContractDetails(contract)
    except Exception as exc:
        return "API_ERROR", f"reqContractDetails raised: {exc}"

    if not details:
        return "CONTRACT_NOT_FOUND", "reqContractDetails returned empty list"

    cd = details[0]
    trading_hours = getattr(cd, "tradingHours", "") or ""
    if not trading_hours:
        return "DEALING_DISABLED", "tradingHours field is empty"

    return "PASS", f"contract resolved: conId={cd.contract.conId}"


# ---------------------------------------------------------------------------
# Phase 1 — Load candidates
# ---------------------------------------------------------------------------

def _load_candidates() -> list[dict]:
    """Load candidates from universe_candidates.json, or fall back to SEED_INSTRUMENTS."""
    if os.path.isfile(CANDIDATES_PATH):
        try:
            with open(CANDIDATES_PATH, encoding="utf-8") as f:
                data = json.load(f)
            cands = data.get("candidates", [])
            if cands:
                # Normalise: accept both "epic" (IG legacy) and "instrument_id" keys.
                normalised = []
                for c in cands:
                    iid = c.get("instrument_id") or c.get("epic", "")
                    if iid:
                        c = dict(c)
                        c["instrument_id"] = iid
                        normalised.append(c)
                if normalised:
                    print(f"[Phase 1] Loaded {len(normalised)} candidates from {CANDIDATES_PATH}")
                    return normalised
        except Exception as exc:
            print(f"[Phase 1] WARNING: could not read {CANDIDATES_PATH}: {exc}")

    print(f"[Phase 1] No existing candidates found — using {len(SEED_INSTRUMENTS)} seed instruments.")
    return [dict(c) for c in SEED_INSTRUMENTS]


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _connect() -> "IB":
    """Connect to IB Gateway or TWS and return an IB instance."""
    if not _IB_AVAILABLE:
        print("ERROR: ib_insync is not installed. Run: pip install ib_insync>=0.9.86")
        raise SystemExit(1)

    env_path = os.path.join(PROJECT_ROOT, "secrets", ".env")
    if os.path.isfile(env_path):
        load_dotenv(env_path, override=True)

    host      = os.environ.get("IBKR_HOST",      "127.0.0.1").strip()
    port      = int(os.environ.get("IBKR_PORT",  "4002").strip())
    client_id = int(os.environ.get("IBKR_CLIENT_ID", "1").strip())
    paper_only = os.environ.get("IBKR_PAPER_ONLY", "true").strip().lower() == "true"

    PAPER_PORT = IBKR_PAPER_PORT
    if paper_only and port != PAPER_PORT:
        raise RuntimeError(
            f"IBKR_PAPER_ONLY=true but IBKR_PORT={port}. "
            f"Set IBKR_PORT={PAPER_PORT} for paper trading or set IBKR_PAPER_ONLY=false."
        )

    print(f"[_connect] Connecting to IB Gateway at {host}:{port} (clientId={client_id}) ...")
    ib = IB()
    try:
        ib.connect(host, port, clientId=client_id, timeout=10)
    except Exception as exc:
        print(
            f"ERROR: Could not connect to IB Gateway ({exc}).\n"
            "Make sure IB Gateway or TWS is running with API connections enabled."
        )
        raise RuntimeError(str(exc)) from exc

    print("[_connect] Connected.\n")
    return ib


# ---------------------------------------------------------------------------
# Phase 4 — Write outputs
# ---------------------------------------------------------------------------

def _write_outputs(
    candidates: list[dict],
    ticker_map: dict[str, str | None],
    t1_results: dict[str, tuple[str, str]],
    now_utc: str,
) -> None:
    """Write universe_candidates.json and universe.json."""
    # Build enriched candidate list
    enriched = []
    for c in candidates:
        iid = c["instrument_id"]
        t1_status, t1_reason = t1_results.get(iid, ("UNVALIDATED", "not validated"))
        enriched.append({
            "instrument_id": iid,
            "name":          c.get("name", iid),
            "sec_type":      c.get("sec_type", ""),
            "exchange":      c.get("exchange", ""),
            "currency":      c.get("currency", ""),
            "yh_ticker":     ticker_map.get(iid),
            "asset_class":   "index" if c.get("sec_type") == "IND" else c.get("sec_type", "unknown").lower(),
            "region":        "unknown",
            "t1_status":     t1_status,
            "t1_reason":     t1_reason,
            "t2_status":     "PENDING_T2",
            "t2_reason":     "awaiting DataPipeline data fetch",
            "valid":         False,
            "last_validated": now_utc,
        })

    candidates_data = {
        "description": (
            "Tradinator universe candidate registry (IBKR). "
            "Contains all candidate instruments with T1 validation metadata. "
            "Generated by diagnostic_tools/refresh_universe_ibkr.py."
        ),
        "last_discover_run": now_utc,
        "candidates": enriched,
    }
    with open(CANDIDATES_PATH, "w", encoding="utf-8") as f:
        json.dump(candidates_data, f, indent=2)
        f.write("\n")
    print(f"  Saved {len(enriched)} candidates → {CANDIDATES_PATH}")

    # universe.json — T1-PASS only
    t1_pass = [
        {
            "instrument_id": c["instrument_id"],
            "name":          c["name"],
            "sec_type":      c["sec_type"],
            "exchange":      c["exchange"],
            "currency":      c["currency"],
            "yh_ticker":     c["yh_ticker"],
            "asset_class":   c["asset_class"],
            "region":        c["region"],
            "valid":         True,
        }
        for c in enriched
        if c["t1_status"] == "PASS"
    ]
    universe_data = {
        "description": (
            "Tradinator instrument universe — IBKR contracts that have passed "
            "Tier 1 validation (reqContractDetails resolves + tradingHours non-empty). "
            f"Last T1 validation: {now_utc}."
        ),
        "instruments": t1_pass,
    }
    with open(UNIVERSE_PATH, "w", encoding="utf-8") as f:
        json.dump(universe_data, f, indent=2)
        f.write("\n")
    print(f"  Saved {len(t1_pass)} T1-pass instruments → {UNIVERSE_PATH}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(config: dict | None = None) -> bool:
    """Run the IBKR universe refresh pipeline.

    Returns True on success, False on transient connection failure.
    Raises on credential/config errors.
    """
    # --- Phase 1: Load candidates ---
    print("=== Phase 1: Load candidates ===")
    candidates = _load_candidates()

    # --- Connect ---
    try:
        ib = _connect()
    except (RuntimeError, SystemExit):
        raise
    except Exception as exc:
        print(
            f"[refresh_universe_ibkr] WARNING: connection failed ({exc}) — "
            "using existing universe.json unchanged."
        )
        return False

    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    try:
        # --- Phase 2: Enrich ---
        print("\n=== Phase 2: Enrich (Yahoo Finance tickers) ===")
        ticker_map: dict[str, str | None] = {}
        for c in candidates:
            iid = c["instrument_id"]
            ticker, did_search = _find_ticker(iid, c.get("name", iid))
            ticker_map[iid] = ticker if ticker != "NONE" else None
            if did_search and ticker != "NONE":
                print(f"  {iid:<20} → {ticker}")
            if did_search:
                time.sleep(_SEARCH_DELAY_S)

        matched = sum(1 for v in ticker_map.values() if v)
        print(f"\nPhase 2 complete: {matched}/{len(candidates)} tickers resolved.")

        # --- Phase 3: T1 validate ---
        print("\n=== Phase 3: T1 Validation (IBKR reqContractDetails) ===")
        t1_results: dict[str, tuple[str, str]] = {}
        t1_pass_count = 0
        total = len(candidates)
        for i, c in enumerate(candidates, 1):
            iid = c["instrument_id"]
            t1_status, t1_reason = _validate_tier1(ib, c)
            symbol = "PASS" if t1_status == "PASS" else "FAIL"
            print(f"  [{i:4d}/{total}] T1 [{symbol}] {iid:<20} — {t1_reason}")
            t1_results[iid] = (t1_status, t1_reason)
            if t1_status == "PASS":
                t1_pass_count += 1
            time.sleep(0.1)  # brief pause between IBKR requests

        print(f"\nPhase 3 complete: {t1_pass_count}/{total} T1-pass.")

        # --- Phase 4: Write outputs ---
        print("\n=== Phase 4: Writing outputs ===")
        _write_outputs(candidates, ticker_map, t1_results, now_utc)

    finally:
        ib.disconnect()

    print(f"\nrefresh_universe_ibkr complete — {t1_pass_count} instruments in universe.json.")
    return True


if __name__ == "__main__":
    run()
