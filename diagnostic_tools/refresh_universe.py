#!/usr/bin/env python3
"""
Tradinator — refresh_universe.py

Unified pipeline that discovers, enriches, validates, and writes the
instrument universe entirely through live IG API queries.  No intermediate
files are produced; all data flows through memory.

Pipeline phases
---------------
  Phase 1 — Discover  : IG search_markets with alphabet-drilldown.
                         Seeds: broad equity-index names covering all major
                         world regions (US 500, Wall Street, Germany 40,
                         Japan 225, Hong Kong, …).
                         When search_markets returns the IG result cap (50),
                         sub-queries "<term> a" … "<term> z a" … are issued
                         recursively until results drop below the cap or the
                         safety limit MAX_SEARCH_CALLS is reached.
                         Only instruments with instrumentType == INDICES are
                         retained; all other asset classes (forex, commodities,
                         bonds, options, binary, individual shares) are excluded.
                         No company names or epic codes are hardcoded — the IG
                         API itself determines what instruments are discoverable.

  Phase 2 — Enrich    : Resolve a Yahoo Finance ticker for each discovered
                         epic.  Priority: curated INSTRUMENT_TO_YH_TICKER map (no
                         API call), then Yahoo Search API with Jaccard
                         name-matching as a fallback.

  Phase 3 — Validate  : Tier 1 check — call fetch_market_by_epic and confirm
                         the broker recognises the epic and dealing is enabled.

  Phase 4 — Write     : universe_candidates.json (all epics + metadata) and
                         universe.json (T1-PASS epics only, valid=True).
                         Writes happen atomically after all phases complete;
                         existing files are not modified if the run fails.

Usage
-----
  python diagnostic_tools/refresh_universe.py   # standalone

Or callable from main.py via importlib (diagnostic_tools/ has no __init__.py):
  spec = importlib.util.spec_from_file_location(
      "refresh_universe",
      os.path.join(os.path.dirname(__file__), "diagnostic_tools", "refresh_universe.py"),
  )
  mod = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(mod)
  success = mod.run()

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind.  Use at your own risk.
"""

from __future__ import annotations

import json
import os
import re
import string
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv          # noqa: E402
from trading_ig import IGService        # noqa: E402
import yfinance as yf                   # noqa: E402

from model.model_components.yh_finance_fetcher import INSTRUMENT_TO_YH_TICKER  # noqa: E402

# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------
CANDIDATES_PATH = os.path.join(PROJECT_ROOT, "data", "input", "universe_candidates.json")
UNIVERSE_PATH   = os.path.join(PROJECT_ROOT, "data", "input", "universe.json")
EXCLUDED_PATH   = os.path.join(PROJECT_ROOT, "data", "input", "ig_assets_excluded.json")

# ---------------------------------------------------------------------------
# Phase 1 — Discovery constants
# ---------------------------------------------------------------------------
# Seed terms for global equity index discovery.  Each term is passed to
# ig.search_markets(); the IG API itself determines which epics are returned.
# Only INDICES-type instruments are kept (see _INCLUDED_TYPES below).
# No company names, epic codes, or ticker symbols are hardcoded here.
SEED_TERMS = [
    # ── Tier A — cap-trigger seeds ────────────────────────────────────────
    # Broad enough to hit the 50-result cap and activate a–z alphabet
    # drilldown, which is the primary discovery multiplier.
    "STOXX",            # STOXX family: Euro STOXX 50, STOXX Europe 600, sectors…
    "FTSE",             # FTSE family: 100, 250, All-Share, MIB, China A50, Techmark…
    "S&P",              # S&P family: 500, MidCap 400, SmallCap 600, TSX Composite…
    "Russell",          # Russell family: 2000, 3000…
    "MSCI",             # MSCI family: World, EM, regional…
    "Nikkei",           # Nikkei family: 225, 400…
    "Hang Seng",        # Hang Seng family: HSI, HSCEI, Tech…
    "DAX",              # DAX family: 40, MDAX, TecDAX, SDAX…
    "CAC",              # CAC family: 40, Mid 60…
    "Nifty",            # Nifty family: 50, 100, Bank Nifty…
    "ASX",              # ASX family: 200, 50…
    "KOSPI",            # KOSPI family: 200…
    "Index",            # catch-all for any IG product whose name contains "Index"
    # ── North America ────────────────────────────────────────────────────
    "US 500",           # S&P 500
    "Wall Street",      # Dow Jones Industrial Average
    "US Tech 100",      # Nasdaq 100
    "US 2000",          # Russell 2000 small-cap
    "US 400",           # S&P MidCap 400
    "Canada 60",        # S&P/TSX 60
    "Brazil 60",        # Bovespa / Ibovespa
    "Mexico",           # IPC (Mexbol)
    # ── UK ───────────────────────────────────────────────────────────────
    "FTSE 100",         # FTSE 100
    "FTSE 250",         # FTSE 250 Mid-Cap
    "FTSE All",         # FTSE All-Share
    # ── Eurozone & European Union ────────────────────────────────────────
    "Germany 40",       # DAX
    "France 40",        # CAC 40
    "Europe 50",        # Euro Stoxx 50
    "EURO STOXX",       # broader STOXX family (sector + regional variants)
    "Spain 35",         # IBEX 35
    "Netherlands 25",   # AEX
    "Italy 40",         # FTSE MIB
    "Switzerland 20",   # SMI
    # ── Rest of Europe ───────────────────────────────────────────────────
    "Sweden 30",        # OMX Stockholm 30
    "Norway 25",        # OBX
    "Denmark 25",       # OMX Copenhagen 25
    "Finland 25",       # OMX Helsinki 25
    "Austria 20",       # ATX
    "Belgium 20",       # BEL 20
    "Poland 20",        # WIG 20
    "Portugal 20",      # PSI 20
    "Czech",            # Prague SE (PX)
    "Hungary",          # BUX
    "Greece",           # Athens General Composite (Athex)
    "Turkey",           # BIST National 100
    "Russia",           # MOEX / RTS
    # ── Asia-Pacific ─────────────────────────────────────────────────────
    "Japan 225",        # Nikkei 225
    "Hong Kong",        # Hang Seng
    "China A50",        # FTSE China A50
    "China H",          # Hang Seng China H-Shares / HSCEI
    "Australia 200",    # ASX 200
    "South Korea",      # KOSPI 200
    "India",            # Nifty 50 / Sensex
    "Singapore",        # Straits Times Index (STI)
    "Taiwan",           # TAIEX
    "Indonesia",        # IDX Composite
    "New Zealand",      # NZX 50
    "Malaysia",         # FTSE Bursa Malaysia KLCI
    "Thailand",         # SET 50
    # ── Africa & Middle East ─────────────────────────────────────────────
    "South Africa 40",  # JSE Top 40
    # ── Multi-region / MSCI ──────────────────────────────────────────────
    "MSCI World",       # MSCI World (developed markets)
    "MSCI EM",          # MSCI Emerging Markets
    # ── Tier B — IG-confirmed precision seeds ────────────────────────────
    # Net-new vs Tier A drilldown: either the IG product name doesn't share
    # a prefix with any Tier A term, or the Tier A drilldown won't reach it
    # via alpha suffixes (e.g. numeric tokens, brand names, weekend products).
    "US Fang",              # US FANG+ / Big Tech basket
    "Cannabis Index",       # Cannabis sector index
    "AI Index",             # Artificial Intelligence sector index
    "Germany Mid-Cap 50",   # MDAX via IG name (not prefixed with "DAX")
    "Germany Tech 30",      # TecDAX via IG name (not prefixed with "DAX")
    "GR25",                 # IG Greece 25
    "Tokyo First Section",  # TSE First Section (not Nikkei-prefixed in IG)
    "Hong Kong Tech",       # HK Tech Index (not Hang-Seng-prefixed in IG)
    "IND50",                # IG India 50 shortform
    "Hong Kong HS50",       # IG Hang Seng 50 variant (not Hang-Seng-prefixed)
    "Weekend UK 100",       # IG weekend product: FTSE 100
    "Weekend Germany 40",   # IG weekend product: DAX
    "Weekend Wall Street",  # IG weekend product: DJIA
    "Weekend US Tech 100",  # IG weekend product: Nasdaq 100
    "Weekend Hong Kong",    # IG weekend product: Hang Seng
    "Dow Jones",            # Dow Jones-branded products (complement to Wall Street)
    "Nasdaq",               # Nasdaq family: Composite, 100… (cap-trigger for Nasdaq)
    "MDAX",                 # German mid-cap (IG may use this name directly)
    "TecDAX",               # German tech (IG may use this name directly)
    "IBEX",                 # IBEX 35 variants
    "AEX",                  # Amsterdam Exchange Index
    "SMI",                  # Swiss Market Index
    "OMX",                  # OMX family cap-trigger (Stockholm, Copenhagen, Helsinki)
    "WIG",                  # Warsaw Stock Exchange WIG variants
    "BUX",                  # Budapest Stock Exchange BUX
    "ATX",                  # Vienna Stock Exchange ATX
    "RTS",                  # Russian Trading System
    "Moex",                 # Moscow Exchange
    "Tel Aviv",             # Tel Aviv Stock Exchange
    "TA-35",                # TA-35 index
    "All Ordinaries",       # Australian All Ordinaries (not ASX-prefixed)
    "NZX",                  # New Zealand Exchange indices
    "Sensex",               # BSE Sensex (complement to Nifty)
    "CSI 300",              # CSI 300 (China A-shares)
    "SET",                  # Stock Exchange of Thailand
    "TSX",                  # Toronto Stock Exchange (broader than Canada 60)
    # ── Tier C — US and European sector indices ──────────────────────────
    "US Banks",             # US banking sector
    "US Financials",        # US financial sector
    "US Tech",              # US technology sector
    "US Healthcare",        # US healthcare sector
    "US Energy",            # US energy sector
    "US Biotech",           # US biotechnology sector
    "US Semiconductor",     # US semiconductors
    "SOX",                  # Philadelphia Semiconductor Index
    "US REIT",              # US real estate investment trusts
    "US Consumer",          # US consumer sector
    "US Industrials",       # US industrials sector
    "US Materials",         # US materials sector
    "US Utilities",         # US utilities sector
    "US Defense",           # US defense sector
    "US Internet",          # US internet sector
    "Europe Banks",         # European banking sector
    "Europe Technology",    # European technology sector
    "Europe Healthcare",    # European healthcare sector
    "Europe Energy",        # European energy sector
    "STOXX 600",            # STOXX Europe 600 (numeric suffix; not reached by alpha drilldown)
    "Euro STOXX Banks",     # Euro STOXX Banks sector
    # ── Tier D — Frontier / EM and niche ────────────────────────────────
    "Vietnam",              # Ho Chi Minh Stock Exchange (VN-Index)
    "Philippines",          # Philippine Stock Exchange Index (PSEi)
    "Egypt",                # Egyptian Exchange
    "EGX 30",               # EGX 30 (Cairo)
    "Nigeria",              # Nigerian Stock Exchange
    "Kenya",                # Nairobi Securities Exchange
    "Morocco",              # Casablanca Stock Exchange
    "MASI",                 # Moroccan All Shares Index
    "Colombia",             # Colombian Stock Exchange
    "COLCAP",               # COLCAP index
    "Chile",                # Santiago Stock Exchange
    "IPSA",                 # IPSA (Chile)
    "Peru",                 # Lima Stock Exchange
    "Argentina",            # Buenos Aires Stock Exchange
    "MERVAL",               # MERVAL (Argentina)
    "Qatar",                # Qatar Stock Exchange
    "UAE",                  # UAE markets (ADX, DFM)
    "Dubai",                # Dubai Financial Market
    "Abu Dhabi",            # Abu Dhabi Securities Exchange
    "Bahrain",              # Bahrain Bourse
    "Kuwait",               # Kuwait Stock Exchange
    "Oman",                 # Muscat Securities Market
    "Saudi",                # Saudi Exchange (Tadawul)
    "Tadawul",              # Tadawul All Share Index (TASI)
    "Israel",               # Tel Aviv Stock Exchange (complement to "Tel Aviv" seed)
    "Romania",              # Bucharest Stock Exchange
    "Bulgaria",             # Bulgarian Stock Exchange
    "Jordan",               # Amman Stock Exchange
    "Pakistan",             # Pakistan Stock Exchange (KSE-100)
    "Kazakhstan",           # Kazakhstan Stock Exchange (KASE)
    "Sri Lanka",            # Colombo Stock Exchange
    "Bangladesh",           # Dhaka Stock Exchange
    # ── Tier E — IG-confirmed names (numeric suffixes), sector/factor, GCC ─
    # IG-confirmed names that are not reachable via Tier A a–z drilldown
    # because their suffixes are numeric, or the IG name differs from the
    # common name (e.g. "Singapore Blue Chip" vs "Straits Times Index").
    "Emerging Markets Index",   # IG-confirmed EM broad index
    "US Inflation Index",       # IG-confirmed TIPS-linked US index
    "FTSE Techmark",            # IG-confirmed UK tech sector
    "FTSE Mid 250",             # IG-confirmed FTSE 250 (numeric suffix)
    "Switzerland Blue Chip",    # IG-confirmed SMI
    "Singapore Blue Chip",      # IG-confirmed Straits Times Index
    "Weekend US 500",           # IG weekend product: S&P 500
    "Nasdaq 100",               # Numeric — not reached by 'Nasdaq' a–z drill
    "Nasdaq Composite",
    "S&P 500",                  # Numeric — not reached by 'S&P' a–z drill
    "S&P 100",
    "S&P 400",
    "S&P 600",
    "Russell 1000",             # Numeric — not reached by 'Russell' a–z drill
    "Russell 3000",
    # European sector supplements (not already in Tier C)
    "UK Banks",
    "UK Technology",
    "UK Healthcare",
    "UK Energy",
    "Europe Utilities",
    "Europe Financials",
    "Europe Consumer",
    "Europe Industrials",
    "Europe Real Estate",
    "Europe Telecom",
    "Europe Materials",
    "Europe Oil",
    "STOXX Europe",
    # Factor / style / volatility indices
    "Dividend",
    "ESG",
    "Value",
    "Growth",
    "VSTOXX",                   # Euro Stoxx 50 Volatility Index
    "VIX",                      # CBOE Volatility Index
    # Additional EM/frontier
    "Ibovespa",                 # Brazil (alternative to Brazil 60)
    "Bovespa",
    "TAIEX",                    # Taiwan (alternative to Taiwan seed)
    "Straits Times",            # Singapore STI (alternative)
    "KLCI",                     # Malaysia KLCI (alternative)
    "MICEX",                    # Russia MICEX (alternative)
    "SDAX",                     # Germany small-cap
    "SBF 120",                  # France broader index
    "FTSE SmallCap",            # UK small-cap
    "FTSE AIM",                 # UK AIM market
    "Wilshire",                 # US Wilshire 5000
    "NYSE Composite",           # US broad market
    "GCC",                      # Gulf Co-operation Council composite
    "ASEAN",                    # ASEAN regional index
    "BRICS",                    # BRICS market composite
    "Nordic",                   # Nordic regional index
    "Frontier",                 # Frontier markets
]
RATE_LIMIT_DELAY = 2.0   # seconds between every IG search call (IG enforces ≥2 s)
_EXCEEDED_WAIT   = 65    # seconds to wait when ApiExceededException is raised
_DRILL_CAP       = 50    # result count that signals the IG result cap was hit
_MAX_DRILL_DEPTH = 2     # maximum alphabet-suffix recursion depth
MAX_SEARCH_CALLS = 2000  # safety cap on total search_markets calls per run

# ---------------------------------------------------------------------------
# Phase 2 — Ticker-resolution constants
# ---------------------------------------------------------------------------
_OVERLAP_THRESHOLD = 0.25    # minimum Jaccard score to accept a Yahoo match
_SEARCH_DELAY_S    = 0.35    # seconds between Yahoo Search API calls
_STOPWORDS = frozenset({"the", "a", "of", "and", "for", "in", "on", "to", "by", "at"})
_TRAILING_NOISE = re.compile(
    r"[\s\-]+"
    r"(?:[A-Z]{2,5}[a-z]?|\(\S+\))"   # e.g. USD, USDA, GBp, (£1)
    r"$"
)

# ---------------------------------------------------------------------------
# Phase 3 — T1-validation constants
# ---------------------------------------------------------------------------
_API_MAX_RETRIES      = 3
_API_EXCEEDED_WAITS   = [60, 120, 240]   # back-off per attempt on ApiExceededException
_API_TRANSIENT_WAITS  = [30,  60, 240]   # back-off per attempt on other transient errors
_NON_RETRYABLE_PATTERNS: tuple[str, ...] = (
    "instrument.epic.unavailable",   # epic does not exist on this broker
)

# ---------------------------------------------------------------------------
# Instrument-type whitelist — only equity index CFDs are retained.
# Every instrument whose instrumentType is NOT in this set is moved to the
# excluded list (forex, commodities, bonds, options, binary, share CFDs).
# ---------------------------------------------------------------------------
_INCLUDED_TYPES: frozenset[str] = frozenset({"INDICES"})


# ===========================================================================
# Connection
# ===========================================================================

def _connect() -> IGService:
    """Create and return an authenticated IG DEMO session."""
    env_path = os.path.join(PROJECT_ROOT, "secrets", ".env")
    if os.path.isfile(env_path):
        load_dotenv(env_path, override=True)  # override=True: .env wins over system env vars

    username   = os.environ.get("IG_USERNAME", "").strip()
    password   = os.environ.get("IG_PASSWORD", "").strip()
    api_key    = os.environ.get("IG_API_KEY",  "").strip()
    acc_type   = os.environ.get("IG_ACC_TYPE",  "DEMO").strip()
    acc_number = os.environ.get("IG_ACC_NUMBER")

    if acc_type.upper() != "DEMO":
        print(
            f"ERROR: IG_ACC_TYPE must be 'DEMO', got '{acc_type}'. "
            "Tradinator only supports paper trading on DEMO accounts."
        )
        raise SystemExit(1)

    missing = [
        n for n, v in [
            ("IG_USERNAME", username),
            ("IG_PASSWORD", password),
            ("IG_API_KEY",  api_key),
        ]
        if not v
    ]
    if missing:
        print(f"ERROR: Missing IG credentials: {', '.join(missing)}. Set them in secrets/.env.")
        raise SystemExit(1)

    def _mask(v: str, keep: int = 3) -> str:
        return v[:keep] + "***" if len(v) > keep else "***"

    print(
        f"[_connect] username='{_mask(username)}' api_key='{_mask(api_key)}' "
        f"acc_type='{acc_type}' acc_number='{acc_number or '(not set)'}'"
    )

    ig = IGService(
        username, password, api_key,
        acc_type=acc_type,
        acc_number=acc_number,
        return_dataframe=False,
        return_munch=False,
    )

    _RETRYABLE_CODES     = ("503", "500")
    _MAX_SESSION_RETRIES = 3
    _BACKOFF_BASE        = 15   # doubles each attempt: 15 s, 30 s, 60 s

    last_exc: Exception | None = None
    for attempt in range(1, _MAX_SESSION_RETRIES + 1):
        try:
            ig.create_session(version="2")
            last_exc = None
            break
        except Exception as exc:
            exc_name = type(exc).__name__
            exc_msg  = str(exc)
            is_retryable = (
                exc_name == "ApiExceededException"
                or any(code in exc_msg for code in _RETRYABLE_CODES)
            )
            if not is_retryable:
                raise
            wait     = _BACKOFF_BASE * (2 ** (attempt - 1))
            last_exc = exc
            if attempt < _MAX_SESSION_RETRIES:
                print(f"[_connect] {exc_name} on attempt {attempt} — retrying in {wait}s ...")
                time.sleep(wait)
            else:
                print(f"[_connect] {exc_name} — all retries exhausted.")

    if last_exc is not None:
        raise last_exc

    print("[_connect] Connected to IG DEMO.\n")
    return ig


# ===========================================================================
# Phase 1 — Discover
# ===========================================================================

def _do_search(ig: IGService, term: str, call_counter: list[int]) -> list[dict]:
    """Call ig.search_markets with rate limiting and ApiExceededException back-off.

    *call_counter* is a single-element list used as a mutable integer; it is
    incremented here so callers can enforce a safety cap on total API calls.
    """
    time.sleep(RATE_LIMIT_DELAY)
    call_counter[0] += 1
    try:
        results = ig.search_markets(term)
        return results.get("markets", []) or []
    except Exception as exc:
        exc_name  = type(exc).__name__
        is_exceeded = exc_name == "ApiExceededException" or "Exceeded" in exc_name
        if is_exceeded:
            print(
                f"  [ApiExceededException] — waiting {_EXCEEDED_WAIT}s for rate-limit reset ...",
                flush=True,
            )
            time.sleep(_EXCEEDED_WAIT)
            try:
                results = ig.search_markets(term)
                return results.get("markets", []) or []
            except Exception as retry_exc:
                print(f"  Retry failed: {type(retry_exc).__name__}: {retry_exc}")
                return []
        print(f"  WARNING ({exc_name}): search '{term}' failed: {exc}")
        return []


def _search_with_drilldown(
    ig: IGService,
    term: str,
    seen_epics: set[str],
    call_counter: list[int],
    depth: int = 0,
) -> list[dict]:
    """Search *term* and recurse with letter suffixes when the IG result cap is hit.

    When search_markets returns exactly *_DRILL_CAP* results the IG API is
    truncating; appending each letter a–z as a suffix narrows the query and
    surfaces additional instruments.  Recursion stops at *_MAX_DRILL_DEPTH*
    or when *call_counter* reaches *MAX_SEARCH_CALLS*.

    Returns new market dicts ``{epic, name, ig_type}``; *seen_epics* is updated
    in-place so duplicates across seeds and drill iterations are de-duplicated.
    """
    if call_counter[0] >= MAX_SEARCH_CALLS:
        print(
            f"  [cap] MAX_SEARCH_CALLS={MAX_SEARCH_CALLS} reached — stopping drilldown.",
            flush=True,
        )
        return []

    hits = _do_search(ig, term, call_counter)

    if call_counter[0] % 50 == 0 and call_counter[0] > 0:
        print(f"  ... {call_counter[0]} search calls made so far", flush=True)

    new_markets: list[dict] = []
    for mkt in hits:
        epic = mkt.get("epic", "")
        if not epic or epic in seen_epics:
            continue
        seen_epics.add(epic)
        ig_type = (mkt.get("instrumentType") or "").strip()
        if not ig_type:
            print(f"  [WARN] {epic}: missing instrumentType — included by default")
        new_markets.append({
            "epic":    epic,
            "name":    mkt.get("instrumentName", epic),
            "ig_type": ig_type,
        })

    if len(hits) >= _DRILL_CAP and depth < _MAX_DRILL_DEPTH:
        print(
            f"  [drill] '{term}' hit cap ({len(hits)}) — "
            f"expanding a–z (depth {depth + 1}) ...",
            flush=True,
        )
        for letter in string.ascii_lowercase:
            if call_counter[0] >= MAX_SEARCH_CALLS:
                break
            sub = _search_with_drilldown(
                ig, f"{term} {letter}", seen_epics, call_counter, depth + 1
            )
            new_markets.extend(sub)

    return new_markets


# ===========================================================================
# Phase 2 — Enrich (Yahoo Finance ticker resolution)
# ===========================================================================

def _clean_name(name: str) -> str:
    """Strip IG-specific trailing currency/noise tokens before a Yahoo name search."""
    cleaned = _TRAILING_NOISE.sub("", name).strip()
    cleaned = re.sub(r"\s*-\s*$", "", cleaned).strip()
    return cleaned if cleaned else name


def _word_tokens(text: str) -> set[str]:
    """Lowercase alphanumeric tokens with stopwords removed."""
    return set(re.findall(r"[a-z0-9]+", text.lower())) - _STOPWORDS


def _jaccard(a: str, b: str) -> float:
    """Jaccard similarity of word-token sets between two name strings."""
    ta, tb = _word_tokens(a), _word_tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _search_yahoo(name: str, prefer_lse: bool) -> str | None:
    """Query Yahoo Finance Search for *name*; return the best-matching ticker or None."""
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
        if prefer_lse and r.get("exchange") == "LSE":
            score += 0.10
        if score > best_score:
            best_score  = score
            best_symbol = r.get("symbol")

    if best_score < _OVERLAP_THRESHOLD:
        return None
    return best_symbol


def _find_ticker(epic: str, ig_type: str, name: str) -> tuple[str, bool]:
    """Return *(ticker_or_NONE, did_search)* for one instrument.

    Priority: curated INSTRUMENT_TO_YH_TICKER map (no API call) → Yahoo Search API.
    """
    if epic in INSTRUMENT_TO_YH_TICKER:
        return INSTRUMENT_TO_YH_TICKER[epic], False
    if ig_type not in _INCLUDED_TYPES:
        return "NONE", False
    prefer_lse = "LN." in epic   # London-listed share epic
    ticker = _search_yahoo(_clean_name(name), prefer_lse=prefer_lse)
    return (ticker if ticker else "NONE"), True


# ===========================================================================
# Phase 3 — T1 Validation
# ===========================================================================

def _api_call_with_retry(fn, *args, **kwargs):
    """Call an IGService method with automatic retry on transient failures.

    ``ApiExceededException`` uses a long back-off (60 s, 120 s, 240 s).
    Other transient exceptions use a shorter back-off (30 s, 60 s, 240 s).
    Deterministic non-retryable errors (epic.unavailable) are re-raised
    immediately to avoid burning API quota.
    """
    last_exc: Exception | None = None
    for attempt in range(_API_MAX_RETRIES):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            exc_name = type(exc).__name__
            exc_msg  = str(exc)

            if any(p in exc_msg.lower() for p in _NON_RETRYABLE_PATTERNS):
                raise   # fast-fail; retrying won't help

            if "exceeded-account-historical-data-allowance" in exc_msg:
                print(
                    "[refresh_universe] FATAL: IG weekly historical-data allowance exhausted.\n"
                    "  This resets on a weekly basis (typically Monday UTC).\n"
                    "  No point retrying — aborting."
                )
                raise

            is_exceeded = exc_name == "ApiExceededException" or "Exceeded" in exc_name
            wait_list   = _API_EXCEEDED_WAITS if is_exceeded else _API_TRANSIENT_WAITS
            wait        = wait_list[min(attempt, len(wait_list) - 1)]
            last_exc    = exc

            if attempt < _API_MAX_RETRIES - 1:
                print(
                    f"[refresh_universe] {exc_name}('{exc_msg}') on attempt {attempt + 1}/"
                    f"{_API_MAX_RETRIES} — retrying in {wait}s",
                    flush=True,
                )
                for remaining in range(wait, 0, -1):
                    print(f"\r  waiting {remaining:3d}s ...  ", end="", flush=True)
                    time.sleep(1)
                print("\r" + " " * 30 + "\r", end="", flush=True)
            else:
                print(
                    f"[refresh_universe] {exc_name}('{exc_msg}') on attempt {attempt + 1}/"
                    f"{_API_MAX_RETRIES} — all retries exhausted"
                )
    raise last_exc  # type: ignore[misc]


def _validate_tier1(ig: IGService, epic: str) -> tuple[str, str]:
    """Check broker recognition and dealing eligibility for *epic*.

    Returns ``(t1_status, t1_reason)`` where *t1_status* is one of:
      ``PASS``               — epic recognised and dealing enabled
      ``EPIC_NOT_RECOGNIZED`` — call failed or returned no data
      ``DEALING_DISABLED``   — epic found but dealingEnabled=false
      ``API_ERROR``          — other unexpected exception after retries
    """
    try:
        result = _api_call_with_retry(ig.fetch_market_by_epic, epic)
    except Exception as exc:
        exc_msg = str(exc)
        if any(p in exc_msg.lower() for p in _NON_RETRYABLE_PATTERNS):
            return "EPIC_NOT_RECOGNIZED", f"epic unavailable: {exc_msg}"
        return "API_ERROR", f"exception after retries: {exc_msg}"

    if result is None:
        return "EPIC_NOT_RECOGNIZED", "no data returned"

    snapshot        = (result.get("snapshot") or {}) if isinstance(result, dict) else {}
    dealing_enabled = snapshot.get("dealingEnabled")

    if dealing_enabled is False:
        return "DEALING_DISABLED", "dealingEnabled=false"

    reason = (
        "dealingEnabled=true"
        if dealing_enabled
        else "dealingEnabled field absent — assumed tradeable"
    )
    return "PASS", reason


# ===========================================================================
# Phase 4 — Write outputs
# ===========================================================================

def _write_outputs(
    included: dict[str, dict],
    excluded: list[dict],
    ticker_map: dict[str, str | None],
    t1_results: dict[str, tuple[str, str]],
    now_utc: str,
) -> None:
    """Write universe_candidates.json, universe.json, and ig_assets_excluded.json.

    All three files are written only after all phases have completed successfully,
    so a mid-run failure leaves existing files untouched.
    """
    # --- universe_candidates.json ---
    candidates = []
    for epic, info in included.items():
        t1_status, t1_reason = t1_results.get(epic, ("UNVALIDATED", "not validated"))
        candidates.append({
            "epic":           epic,
            "name":           info["name"],
            "ig_type":        info["ig_type"] or None,
            "yh_ticker":      ticker_map.get(epic),
            "asset_class":    "unknown",
            "region":         "unknown",
            "t1_status":      t1_status,
            "t1_reason":      t1_reason,
            "t2_status":      "PENDING_T2",
            "t2_reason":      "awaiting DataPipeline data fetch",
            "valid":          False,
            "last_validated": now_utc,
        })

    candidates_data = {
        "description": (
            "Tradinator universe candidate registry. "
            "Contains all candidate instruments with two-tier validation metadata. "
            "Generated by diagnostic_tools/refresh_universe.py."
        ),
        "last_discover_run": now_utc,
        "candidates": candidates,
    }
    with open(CANDIDATES_PATH, "w", encoding="utf-8") as f:
        json.dump(candidates_data, f, indent=2)
        f.write("\n")
    print(f"  Saved {len(candidates)} candidates → {CANDIDATES_PATH}")

    # --- universe.json (T1-PASS only) ---
    t1_pass_instruments = [
        {
            "epic":        c["epic"],
            "name":        c["name"],
            "ig_type":     c["ig_type"],
            "yh_ticker":   c["yh_ticker"],
            "asset_class": c["asset_class"],
            "region":      c["region"],
            "valid":       True,
        }
        for c in candidates
        if c["t1_status"] == "PASS"
    ]
    universe_data = {
        "description": (
            "Tradinator instrument universe — IG Demo epics that have passed Tier 1 validation "
            "(broker recognition + dealing enabled). "
            "Tier 2 (data availability) is validated continuously by DataPipeline. "
            f"Last T1 validation: {now_utc}."
        ),
        "instruments": t1_pass_instruments,
    }
    with open(UNIVERSE_PATH, "w", encoding="utf-8") as f:
        json.dump(universe_data, f, indent=2)
        f.write("\n")
    print(f"  Saved {len(t1_pass_instruments)} T1-pass instruments → {UNIVERSE_PATH}")

    # --- ig_assets_excluded.json ---
    excluded_data = {
        "description": (
            "IG DEMO assets excluded from the Tradinator candidate list. "
            "Excluded because instrumentType is not in the equity-index whitelist (INDICES). "
            "Includes forex, commodities, bonds, options, binary products, and individual share CFDs."
        ),
        "count": len(excluded),
        "assets": excluded,
    }
    with open(EXCLUDED_PATH, "w", encoding="utf-8") as f:
        json.dump(excluded_data, f, indent=2)
        f.write("\n")
    print(f"  Saved {len(excluded)} excluded assets → {EXCLUDED_PATH}")


# ===========================================================================
# Entry point
# ===========================================================================

def run(config: dict | None = None) -> bool:
    """Run the full refresh pipeline.

    Parameters
    ----------
    config:
        Optional dict forwarded from main.py.  Currently unused; accepted
        for API compatibility so main.py's caller contract is stable.

    Returns
    -------
    True  — pipeline ran to completion and outputs were written.
    False — transient IG failure (503 / 500 / ApiExceededException); existing
            files on disk are left unchanged.

    Raises for all other errors (credentials missing, logic fault, etc.).
    """
    # --- Connect ---
    try:
        ig = _connect()
    except SystemExit:
        raise
    except Exception as exc:
        exc_name = type(exc).__name__
        exc_msg  = str(exc)
        is_transient = (
            any(code in exc_msg for code in ("503", "500"))
            or exc_name == "ApiExceededException"
        )
        if is_transient:
            print(
                f"[refresh_universe] WARNING: connection failed ({exc_name}: {exc_msg}) — "
                "skipping refresh, using existing universe.json."
            )
            return False
        raise

    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # --- Phase 1: Discover ---
    print("=== Phase 1: Discover (IG search-drilldown) ===")
    print(
        f"Seeds: {SEED_TERMS}  |  max_depth={_MAX_DRILL_DEPTH}  "
        f"|  call_cap={MAX_SEARCH_CALLS}\n"
    )
    seen_epics:   set[str]   = set()
    call_counter: list[int]  = [0]
    raw_markets:  list[dict] = []

    for seed in SEED_TERMS:
        print(f"  Seed: '{seed}'", flush=True)
        found = _search_with_drilldown(ig, seed, seen_epics, call_counter)
        raw_markets.extend(found)
        print(f"  → {len(found)} new  (running total: {len(raw_markets)})")

    print(
        f"\nPhase 1 complete: {len(raw_markets)} unique instruments "
        f"in {call_counter[0]} search call(s)."
    )

    included: dict[str, dict] = {}
    excluded: list[dict]      = []
    for mkt in raw_markets:
        ig_type = mkt.get("ig_type", "") or ""
        if ig_type not in _INCLUDED_TYPES:
            excluded.append({"epic": mkt["epic"], "ig_type": ig_type, "name": mkt["name"]})
        else:
            included[mkt["epic"]] = {"name": mkt["name"], "ig_type": ig_type}

    print(f"  Equity indices (INDICES type): {len(included)}  |  Excluded (other types): {len(excluded)}")

    # --- Phase 2: Enrich ---
    print("\n=== Phase 2: Enrich (Yahoo Finance tickers) ===")
    ticker_map: dict[str, str | None] = {}
    for epic, info in included.items():
        ticker, did_search = _find_ticker(epic, info["ig_type"], info["name"])
        ticker_map[epic] = ticker if ticker != "NONE" else None
        if did_search and ticker != "NONE":
            print(f"  {epic:<40} → {ticker}")
        if did_search:
            time.sleep(_SEARCH_DELAY_S)

    matched = sum(1 for v in ticker_map.values() if v)
    print(f"\nPhase 2 complete: {matched}/{len(included)} tickers resolved.")

    # --- Phase 3: T1 validate ---
    print("\n=== Phase 3: T1 Validation (IG API) ===")
    t1_results: dict[str, tuple[str, str]] = {}
    t1_pass_count = 0
    total = len(included)
    for i, epic in enumerate(included, 1):
        time.sleep(RATE_LIMIT_DELAY)
        t1_status, t1_reason = _validate_tier1(ig, epic)
        symbol = "PASS" if t1_status == "PASS" else "FAIL"
        print(f"  [{i:4d}/{total}] T1 [{symbol}] {epic}  — {t1_reason}")
        t1_results[epic] = (t1_status, t1_reason)
        if t1_status == "PASS":
            t1_pass_count += 1

    print(f"\nPhase 3 complete: {t1_pass_count}/{total} T1-pass.")

    # --- Phase 4: Write outputs ---
    print("\n=== Phase 4: Writing outputs ===")
    _write_outputs(included, excluded, ticker_map, t1_results, now_utc)

    print(f"\nrefresh_universe complete — {t1_pass_count} instruments in universe.json.")
    if t1_pass_count == 0:
        print(
            "WARNING: Zero T1-pass instruments. "
            "Check IG credentials and account status."
        )

    return True


if __name__ == "__main__":
    run()
