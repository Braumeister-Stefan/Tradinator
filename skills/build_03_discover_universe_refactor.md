# BUILD PROMPT 03 — Refactor discover_universe.py (Two-Tier Validation)

**Prepared by:** FUNCTIONALIST  
**Scope:** Refactor `data/input/discover_universe.py` to implement two-tier validation, write all candidates (pass and fail) to `universe_candidates.json`, and write only `valid: true` instruments to `universe.json`.

**Must run AFTER:** Build 01 (universe_candidates.json exists), Build 02 (universe.json uses valid flag)

---

## Context

The repository is at `/home/runner/work/Tradinator/Tradinator`.

**Current behaviour:**
- Single-tier test: calls `fetch_historical_prices_by_epic_and_num_points` and returns pass/fail.
- Failed epics are permanently deleted from `universe.json`.
- Reason for failure is printed to stdout only, never persisted.
- No distinction between "broker doesn't recognise epic" and "dealing disabled" and "no data".

**New behaviour:**
- Two-tier validation for each candidate in `universe_candidates.json`.
- Tier 1 (T1): Call `ig.fetch_market_by_epic(epic)`. Check:
  - If the call throws or returns no market → `EPIC_NOT_RECOGNIZED`
  - If the call succeeds but `dealingEnabled=false` in snapshot → `DEALING_DISABLED`
  - Otherwise → `PASS`
- Tier 2 (T2): Only run if T1 = PASS. Call `fetch_historical_prices_by_epic_and_num_points(epic, "DAY", 10)`.
  - If ≥1 bar returned with valid bid or ask price → `YES`
  - If 0 bars or all None → `NO`
  - If exception → record as `NO` with `t2_reason = str(exc)`
- After validation: `valid = (t1_status == "PASS") and (t2_status == "YES")`
- Candidates that pass T1 but haven't been run through T2 (i.e., T2=NO because the test just ran and failed) get `t2_status: "NO"`. Candidates that pass T1 and also pass T2 here get `t2_status: "YES"`.
- Note: "PENDING_T2" is set by discover_universe.py ONLY for candidates that are NOT directly tested for T2 (e.g. newly discovered epics that passed T1 search but weren't validated with a full price fetch). In Phase 1 (validate existing candidates), all T1-pass epics ARE tested for T2 immediately, so they get YES or NO. The PENDING_T2 state arises only if discover_universe.py is extended to do T1-only pre-screening without T2.
- All 30 candidates are retained in `universe_candidates.json` with their validation results.
- `universe.json` is overwritten with ONLY `valid: true` instruments.

---

## Detailed Implementation

### Constants to add/change

```python
CANDIDATES_PATH = os.path.join(PROJECT_ROOT, "data", "input", "universe_candidates.json")
RATE_LIMIT_DELAY = 0.5   # increase from 0.3 — two API calls per epic
LOOKBACK_BARS = 10
RESOLUTION = "DAY"
```

### New function: `_validate_tier1(ig, epic) -> tuple[str, str]`

Returns `(t1_status, t1_reason)`.

```python
def _validate_tier1(ig, epic: str) -> tuple[str, str]:
    """Tier 1: check broker recognition and dealing eligibility."""
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

    # dealingEnabled=True, or field absent (treat as pass with note)
    reason = "dealingEnabled=true" if dealing_enabled else "dealingEnabled field absent, assumed tradeable"
    return "PASS", reason
```

### New function: `_validate_tier2(ig, epic) -> tuple[str, str]`

Returns `(t2_status, t2_reason)`.

```python
def _validate_tier2(ig, epic: str) -> tuple[str, str]:
    """Tier 2: check that price data is available."""
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
```

### Remove old `_test_epic()` function entirely.

### New functions: `_load_candidates()` and `_save_candidates()`

```python
def _load_candidates() -> dict:
    """Load universe_candidates.json. Create default structure if absent."""
    if not os.path.isfile(CANDIDATES_PATH):
        return {"description": "Tradinator universe candidate registry.", "last_discover_run": None, "candidates": []}
    with open(CANDIDATES_PATH) as f:
        return json.load(f)

def _save_candidates(data: dict) -> None:
    """Write updated universe_candidates.json."""
    with open(CANDIDATES_PATH, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    print(f"Saved {len(data['candidates'])} candidates to {CANDIDATES_PATH}")
```

### Updated `_save_universe(data)` — keep but update docstring and logic

The `_save_universe` function saves only `valid: true` instruments. Add a log line:
```python
print(f"Saved {len(data['instruments'])} valid instruments to {UNIVERSE_PATH}")
```

### New `main()` function

Replace the existing `main()` with:

```python
def main() -> None:
    ig = _connect()
    candidates_data = _load_candidates()
    candidates = candidates_data.get("candidates", [])

    if not candidates:
        print("No candidates found in universe_candidates.json. Nothing to validate.")
        return

    print(f"\n=== Validating {len(candidates)} candidates ===")
    validated = []
    t1_pass_count = 0
    t2_pass_count = 0
    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    for inst in candidates:
        epic = inst.get("epic", "").strip()
        name = inst.get("name", epic)

        if not epic:
            print("  ✗ (skipped entry with missing epic)")
            inst.update({
                "t1_status": "UNTESTED", "t1_reason": "missing epic field",
                "t2_status": "NEVER_TRIED", "t2_reason": None,
                "valid": False, "last_validated": now_utc,
            })
            validated.append(inst)
            continue

        # --- Tier 1 ---
        time.sleep(RATE_LIMIT_DELAY)
        t1_status, t1_reason = _validate_tier1(ig, epic)
        t1_symbol = "✓" if t1_status == "PASS" else "✗"
        print(f"  T1 {t1_symbol} {epic} ({name}) — {t1_status}: {t1_reason}")

        if t1_status != "PASS":
            inst.update({
                "t1_status": t1_status, "t1_reason": t1_reason,
                "t2_status": "NEVER_TRIED", "t2_reason": None,
                "valid": False, "last_validated": now_utc,
            })
            validated.append(inst)
            continue

        t1_pass_count += 1

        # --- Tier 2 ---
        time.sleep(RATE_LIMIT_DELAY)
        t2_status, t2_reason = _validate_tier2(ig, epic)
        t2_symbol = "✓" if t2_status == "YES" else "✗"
        print(f"  T2 {t2_symbol} {epic} ({name}) — {t2_status}: {t2_reason}")

        is_valid = (t2_status == "YES")
        if is_valid:
            t2_pass_count += 1

        inst.update({
            "t1_status": t1_status, "t1_reason": t1_reason,
            "t2_status": t2_status, "t2_reason": t2_reason,
            "valid": is_valid, "last_validated": now_utc,
        })
        validated.append(inst)

    # --- Phase 2: discover additional epics via search if < 20 valid ---
    known_epics = {c.get("epic", "") for c in validated}
    valid_count = sum(1 for c in validated if c.get("valid"))

    if valid_count < 20:
        print(f"\n=== Phase 2: Discovering additional markets (have {valid_count}/20 valid) ===")
        discovered = _discover_via_search(ig, known_epics, validated, now_utc)
        print(f"Phase 2 added {len(discovered)} new valid instruments.")

    # --- Save candidates file (all 30+) ---
    candidates_data["candidates"] = validated
    candidates_data["last_discover_run"] = now_utc
    _save_candidates(candidates_data)

    # --- Build and save universe.json (valid only) ---
    universe_data = _load_universe()
    valid_instruments = [
        {k: v for k, v in c.items()
         if k in ("epic", "name", "asset_class", "region", "valid")}
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
        print(f"WARNING: Only {valid_total} valid epics (target: 20). Consider adding more candidates.")
```

### Update `_discover_via_search()`

Signature changes to `_discover_via_search(ig, known_epics: set, validated: list, now_utc: str) -> list[dict]`:

- For each discovered market that passes `_validate_tier2()`, build a full candidate dict using the new schema and append to `validated`.
- Set `t1_status: "PASS"`, `t1_reason: "discovered via IG search"`, `t2_status` and `t2_reason` from the T2 check, `valid: True/False`, `last_validated: now_utc`.
- Return the list of newly appended (valid) candidate dicts.
- Remove the old `{"epic": epic, "name": name, "status": "verified"}` dict construction.

**Note:** Discovered epics are NOT run through `_validate_tier1()` with a full `fetch_market_by_epic` call to save API calls — the search result's presence is treated as implicit T1 PASS. Document this assumption in a comment.

---

## Rate Limit Assumptions

- `RATE_LIMIT_DELAY = 0.5` seconds between each API call.
- Two calls per candidate (T1 + T2), so 1 call/second effective rate.
- 30 candidates = ~60 seconds for Phase 1.
- IG Demo limit: assumed ~60 historical price requests/minute. The 0.5s sleep keeps us safely below this.

---

## Acceptance Criteria

1. `_test_epic()` is removed. `_validate_tier1()` and `_validate_tier2()` exist with correct signatures.
2. After running, `universe_candidates.json` retains ALL candidates (pass and fail) with structured `t1_status`, `t1_reason`, `t2_status`, `t2_reason`, `valid`, `last_validated` fields.
3. `universe.json` contains ONLY `valid: true` instruments (no `status` field).
4. The distinction between `EPIC_NOT_RECOGNIZED` and `DEALING_DISABLED` is captured in `t1_status`.
5. Script runs standalone: `python data/input/discover_universe.py` — syntax and import checks pass.
6. No `status` or `note` fields written to either file.

---

## Files to Touch

| File | Action |
|---|---|
| `data/input/discover_universe.py` | MODIFY — full refactor |
