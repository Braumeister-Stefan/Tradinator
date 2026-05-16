#!/usr/bin/env python3
"""
Tradinator — data/input/stock_scoper.py

Phase 1+2+3 universe discovery for IBKR:
  Phase 1 — Load    : Read ``universe_candidates.json`` (or seed list).
  Phase 2 — Enrich  : Resolve Yahoo Finance ticker for each instrument.
  Phase 3 — T1      : Call reqContractDetails per candidate, capture conId.
  Phase 4 — Persist : When ``push_candidates`` is True, merge the enriched
                       candidates into ``universe_candidates.json`` by conId
                       (append new, update existing — never remove entries).
                       When False, no file write happens.

This module no longer writes ``universe.json``; that is the sole
responsibility of ``model.model_components.universe_refresher``.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

from __future__ import annotations

import json
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if __name__ == "__main__" and PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv  # noqa: E402

try:
    from ib_insync import IB, Contract  # noqa: E402
    _IB_AVAILABLE = True
except ImportError:
    _IB_AVAILABLE = False

from model.model_components.yh_finance_fetcher import (  # noqa: E402
    _LEGACY_SYMBOL_TO_YH_TICKER,
    get_yh_ticker,
    resolve_ticker_by_name,
)
from model.model_components.universe_refresher import (  # noqa: E402
    T1_API_ERROR, T1_FAIL, T1_PASS, T2_PENDING,
)


# ---------------------------------------------------------------------------
# Default behaviour: persist enriched candidates into universe_candidates.json.
# Overridable via ``config["push_candidates"]``.
# ---------------------------------------------------------------------------
PUSH_CANDIDATES = True

# ---------------------------------------------------------------------------
# IBKR connection defaults
# ---------------------------------------------------------------------------
IBKR_PAPER_PORT = 4002  # IB Gateway / TWS paper-trading port

# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------
CANDIDATES_PATH = os.path.join(PROJECT_ROOT, "data", "input", "universe_candidates.json")

# ---------------------------------------------------------------------------
# Phase 1 — Seed instruments (used when universe_candidates.json is empty).
# Seed entries carry an IBKR ``symbol`` (NOT a conId); conId is filled in
# during T1 from reqContractDetails.
# ---------------------------------------------------------------------------
SEED_INSTRUMENTS = [
    # North America
    {"symbol": "SPX",   "name": "S&P 500",            "sec_type": "IND",  "exchange": "CBOE",     "currency": "USD"},
    {"symbol": "INDU",  "name": "Dow Jones",          "sec_type": "IND",  "exchange": "CME",      "currency": "USD"},
    {"symbol": "NDX",   "name": "Nasdaq 100",         "sec_type": "IND",  "exchange": "NASDAQ",   "currency": "USD"},
    {"symbol": "RUT",   "name": "Russell 2000",       "sec_type": "IND",  "exchange": "RUSSELL",  "currency": "USD"},
    {"symbol": "TSX",   "name": "S&P/TSX Composite",  "sec_type": "IND",  "exchange": "TSX",      "currency": "CAD"},
    # Europe
    {"symbol": "DAX",   "name": "DAX 40",             "sec_type": "IND",  "exchange": "EUREX",    "currency": "EUR"},
    {"symbol": "CAC40", "name": "CAC 40",             "sec_type": "IND",  "exchange": "MONEP",    "currency": "EUR"},
    {"symbol": "FTSE",  "name": "FTSE 100",           "sec_type": "IND",  "exchange": "LIFFE",    "currency": "GBP"},
    {"symbol": "IBEX35","name": "IBEX 35",            "sec_type": "IND",  "exchange": "MEFFRV",   "currency": "EUR"},
    {"symbol": "SMI",   "name": "Swiss Market Index", "sec_type": "IND",  "exchange": "SOFFEX",   "currency": "CHF"},
    {"symbol": "AEX",   "name": "AEX",                "sec_type": "IND",  "exchange": "FTA",      "currency": "EUR"},
    {"symbol": "SX5E",  "name": "Euro STOXX 50",      "sec_type": "IND",  "exchange": "EUREX",    "currency": "EUR"},
    # Asia-Pacific
    {"symbol": "N225",  "name": "Nikkei 225",         "sec_type": "IND",  "exchange": "OSE.JPN",  "currency": "JPY"},
    {"symbol": "HSI",   "name": "Hang Seng",          "sec_type": "IND",  "exchange": "HKFE",     "currency": "HKD"},
    {"symbol": "AS51",  "name": "ASX 200",            "sec_type": "IND",  "exchange": "ASX",      "currency": "AUD"},
    # Forex majors (CASH)
    {"symbol": "EURUSD","name": "EUR/USD",            "sec_type": "CASH", "exchange": "IDEALPRO", "currency": "USD"},
    {"symbol": "GBPUSD","name": "GBP/USD",            "sec_type": "CASH", "exchange": "IDEALPRO", "currency": "USD"},
    {"symbol": "USDJPY","name": "USD/JPY",            "sec_type": "CASH", "exchange": "IDEALPRO", "currency": "JPY"},
]

_SEARCH_DELAY_S = 0.35


# ---------------------------------------------------------------------------
# Enrichment helpers
# ---------------------------------------------------------------------------

def _find_ticker(symbol: str, name: str) -> tuple[str | None, bool]:
    """Return (ticker_or_None, did_search) for an IBKR symbol + name.

    Priority: legacy symbol → YH map → Yahoo search by name.
    """
    mapped = _LEGACY_SYMBOL_TO_YH_TICKER.get(symbol)
    if mapped is not None:
        return mapped, False
    return resolve_ticker_by_name(name), True


# ---------------------------------------------------------------------------
# T1 validation via IBKR reqContractDetails
# ---------------------------------------------------------------------------

def _build_contract(candidate: dict) -> "Contract":
    """Build an ib_insync Contract from a candidate dict (keyed on 'symbol')."""
    sec_type = candidate.get("sec_type", "IND")
    symbol   = candidate.get("symbol") or candidate.get("conId", "")
    exchange = candidate.get("exchange", "SMART")
    currency = candidate.get("currency", "USD")

    if sec_type == "CASH" and len(symbol) == 6:
        # Forex: "EURUSD" → symbol="EUR", currency="USD".
        return Contract(symbol=symbol[:3], secType="CASH", exchange=exchange, currency=symbol[3:])
    return Contract(symbol=symbol, secType=sec_type, exchange=exchange, currency=currency)


def _validate_tier1_ib(ib: "IB", candidate: dict) -> tuple[str, str, str]:
    """Call reqContractDetails directly via ib_insync; return (status, reason, conId)."""
    try:
        contract = _build_contract(candidate)
        details  = ib.reqContractDetails(contract)
    except Exception as exc:
        return T1_API_ERROR, f"reqContractDetails raised: {exc}", ""

    if not details:
        return T1_FAIL, "reqContractDetails returned empty list", ""

    cd = details[0]
    trading_hours = getattr(cd, "tradingHours", "") or ""
    if not trading_hours:
        return T1_FAIL, "tradingHours field is empty", str(cd.contract.conId or "")

    return T1_PASS, f"contract resolved: conId={cd.contract.conId}", str(cd.contract.conId)


def _validate_tier1_adapter(adapter, candidate: dict) -> tuple[str, str, str]:
    """T1 via a pre-connected BrokerAdapter; conId may stay empty when unknown."""
    sym = candidate.get("symbol") or candidate.get("conId", "")
    if not sym:
        return T1_FAIL, "candidate has no symbol or conId", ""
    try:
        info = adapter.fetch_instrument_info(sym)
    except Exception as exc:
        return T1_API_ERROR, f"adapter raised: {exc}", ""
    if not info:
        return T1_FAIL, "adapter returned empty info", ""
    cid = str(info.get("conId", "") or "")
    return T1_PASS, "contract resolved via adapter", cid


# ---------------------------------------------------------------------------
# Phase 1 — Load candidates from universe_candidates.json
# ---------------------------------------------------------------------------

def _load_candidates() -> tuple[list[dict], dict]:
    """Return (working_candidates, existing_doc).

    ``working_candidates`` is the list to enrich+validate.  ``existing_doc`` is
    the on-disk JSON so we can merge into it later without losing fields.
    """
    if os.path.isfile(CANDIDATES_PATH):
        try:
            with open(CANDIDATES_PATH, encoding="utf-8") as f:
                doc = json.load(f)
            cands = doc.get("candidates", [])
            if cands:
                print(f"[Phase 1] Loaded {len(cands)} candidates from {CANDIDATES_PATH}")
                return [dict(c) for c in cands], doc
            return [dict(c) for c in SEED_INSTRUMENTS], doc
        except Exception as exc:
            print(f"[Phase 1] WARNING: could not read {CANDIDATES_PATH}: {exc}")

    print(f"[Phase 1] No existing candidates found — using {len(SEED_INSTRUMENTS)} seed instruments.")
    return [dict(c) for c in SEED_INSTRUMENTS], {"candidates": []}


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

    if paper_only and port != IBKR_PAPER_PORT:
        raise RuntimeError(
            f"IBKR_PAPER_ONLY=true but IBKR_PORT={port}. "
            f"Set IBKR_PORT={IBKR_PAPER_PORT} for paper trading or IBKR_PAPER_ONLY=false."
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
# Phase 4 — Merge enriched candidates into universe_candidates.json
# ---------------------------------------------------------------------------

# Allowed enrichment fields on every candidate entry (8 enrichment + 4 status).
_ENRICHMENT_FIELDS = ("conId", "name", "sec_type", "exchange", "currency",
                      "yh_ticker", "asset_class", "region")
_STATUS_FIELDS     = ("t1_status", "t2_status", "valid", "last_validated")
_CANDIDATE_FIELDS  = _ENRICHMENT_FIELDS + _STATUS_FIELDS


def _build_candidate_entry(
    src: dict,
    conId: str,
    yh_ticker: str | None,
    t1_status: str,
    t2_status: str,
    now_utc: str,
) -> dict:
    """Project a working candidate dict to the canonical 12-field entry."""
    sec_type = src.get("sec_type", "")
    asset_class = "index" if sec_type == "IND" else (sec_type.lower() if sec_type else "unknown")
    return {
        "conId":          str(conId or ""),
        "name":           src.get("name", ""),
        "sec_type":       sec_type,
        "exchange":       src.get("exchange", ""),
        "currency":       src.get("currency", ""),
        "yh_ticker":      yh_ticker,
        "asset_class":    src.get("asset_class", asset_class),
        "region":         src.get("region", "unknown"),
        "t1_status":      t1_status,
        "t2_status":      t2_status,
        "valid":          False,
        "last_validated": now_utc,
    }


def _merge_into_doc(
    existing_doc: dict, new_entries: list[dict], now_utc: str,
) -> dict:
    """Merge *new_entries* into *existing_doc* by conId (append-or-update).

    Existing conIds are never removed; their fields are updated from the
    incoming entry.  ``last_discover_run`` is bumped to *now_utc*.
    """
    existing = existing_doc.get("candidates", []) or []
    by_id: dict[str, dict] = {}
    order: list[str] = []
    for c in existing:
        cid = str(c.get("conId", "") or "")
        if not cid:
            # Preserve entries that pre-date the conId rename so we never shrink.
            cid = f"__legacy_{len(order)}__"
        by_id[cid] = {k: c.get(k) for k in _CANDIDATE_FIELDS if k in c}
        order.append(cid)

    for entry in new_entries:
        cid = str(entry.get("conId", "") or "")
        if not cid:
            continue
        if cid in by_id:
            by_id[cid].update({k: entry[k] for k in _CANDIDATE_FIELDS if k in entry})
        else:
            by_id[cid] = {k: entry[k] for k in _CANDIDATE_FIELDS if k in entry}
            order.append(cid)

    merged = [by_id[cid] for cid in order]
    if len(merged) < len(existing):
        raise RuntimeError(
            f"_merge_into_doc shrank candidates list: "
            f"existing={len(existing)} merged={len(merged)}"
        )
    out = dict(existing_doc)
    out["description"] = (
        "Tradinator universe candidate registry (IBKR). "
        "Contains all candidate instruments with T1 validation metadata. "
        "Generated by data/input/stock_scoper.py."
    )
    out["last_discover_run"] = now_utc
    out["candidates"]        = merged
    return out


def _write_candidates(doc: dict) -> None:
    """Atomically write the merged candidates document."""
    os.makedirs(os.path.dirname(CANDIDATES_PATH), exist_ok=True)
    with open(CANDIDATES_PATH, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")
    print(f"  Saved {len(doc.get('candidates', []))} candidates → {CANDIDATES_PATH}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(config: dict | None = None, adapter=None) -> bool:
    """Run the IBKR universe discovery pipeline.

    Parameters
    ----------
    config:
        Optional config dict; honoured key is ``push_candidates`` (bool).
        Defaults to module-level :data:`PUSH_CANDIDATES`.
    adapter:
        Optional pre-connected ``BrokerAdapter``.  When provided, T1 uses
        ``adapter.fetch_instrument_info`` instead of opening a direct
        ib_insync connection.
    """
    cfg = config or {}
    push_candidates = bool(cfg.get("push_candidates", PUSH_CANDIDATES))

    # --- Phase 1 ---
    print("=== Phase 1: Load candidates ===")
    working, existing_doc = _load_candidates()

    # --- Connect (only if no adapter was supplied) ---
    ib = None
    use_adapter = adapter is not None
    if not use_adapter:
        try:
            ib = _connect()
        except (RuntimeError, SystemExit):
            raise
        except Exception as exc:
            print(f"[stock_scoper] WARNING: connection failed ({exc}).")
            return False

    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    try:
        # --- Phase 2: Enrich Yahoo tickers ---
        print("\n=== Phase 2: Enrich (Yahoo Finance tickers) ===")
        ticker_map: dict[int, str | None] = {}
        for idx, c in enumerate(working):
            sym = c.get("symbol") or c.get("conId", "")
            name = c.get("name", sym)
            pre_mapped = c.get("yh_ticker")
            if pre_mapped:
                ticker_map[idx] = pre_mapped
                continue
            ticker, did_search = _find_ticker(sym, name)
            ticker_map[idx] = ticker
            if did_search:
                if ticker:
                    print(f"  {sym:<20} → {ticker}")
                time.sleep(_SEARCH_DELAY_S)
        matched = sum(1 for v in ticker_map.values() if v)
        print(f"Phase 2 complete: {matched}/{len(working)} tickers resolved.")

        # --- Phase 3: T1 validation ---
        print("\n=== Phase 3: T1 Validation (reqContractDetails) ===")
        new_entries: list[dict] = []
        pass_count = 0
        total = len(working)
        for i, c in enumerate(working, 1):
            if use_adapter:
                t1_status, t1_reason, conId = _validate_tier1_adapter(adapter, c)
            else:
                t1_status, t1_reason, conId = _validate_tier1_ib(ib, c)
            if not conId:
                conId = str(c.get("conId", "") or "")
            sym = c.get("symbol") or conId
            tag = "PASS" if t1_status == T1_PASS else "FAIL"
            print(f"  [{i:4d}/{total}] T1 [{tag}] {sym:<20} — {t1_reason}")
            if t1_status == T1_PASS:
                pass_count += 1
            entry = _build_candidate_entry(
                c,
                conId=conId,
                yh_ticker=ticker_map.get(i - 1),
                t1_status=t1_status,
                t2_status=T2_PENDING,
                now_utc=now_utc,
            )
            new_entries.append(entry)
            if not use_adapter:
                time.sleep(0.1)
        print(f"Phase 3 complete: {pass_count}/{total} T1-pass.")

        # --- Phase 4: Merge + write (only when push_candidates=True) ---
        if push_candidates:
            print("\n=== Phase 4: Merge candidates ===")
            merged_doc = _merge_into_doc(existing_doc, new_entries, now_utc)
            _write_candidates(merged_doc)
        else:
            print("\n=== Phase 4: SKIPPED (push_candidates=False) — no files written ===")
    finally:
        if ib is not None:
            try:
                ib.disconnect()
            except Exception:
                pass

    print(f"\nstock_scoper complete — {pass_count}/{total} T1-pass.")
    return True


if __name__ == "__main__":
    run()
