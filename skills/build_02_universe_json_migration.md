# BUILD PROMPT 02 — Migrate universe.json Schema (valid flag replaces status)

**Prepared by:** FUNCTIONALIST  
**Scope:** Migrate `data/input/universe.json` and `main.py`'s `_load_universe()` to replace the `status` field with a `valid: bool` field. Also update the DataSource_Scoper spec (`skills/DataSource_Scoper.md`) to reflect the new schema.

**Must run AFTER:** Build 01 (universe_candidates.json created)

---

## Context

The repository is at `/home/runner/work/Tradinator/Tradinator`.

`data/input/universe.json` currently holds 30 instruments (5 `status: "verified"`, 25 `status: "candidate"`). The new design:
- `universe.json` → contains ONLY instruments that are `valid: true` (passed T1 + T2 validation).
- All 30 candidates live in `data/input/universe_candidates.json` (created by Build 01).
- `status` field is removed everywhere; `valid: bool` replaces it.

**Important:** The user requested re-validation from scratch. However, to prevent the pipeline from starting with an empty universe before `discover_universe.py` is re-run, the 5 previously `"verified"` epics are retained in `universe.json` with `valid: true` as a migration convenience. They will be re-validated (and possibly removed) the next time `discover_universe.py` runs.

---

## Task 1 — Migrate `data/input/universe.json`

**Transform each entry:**
- Remove the `status` field.
- Remove the `note` field (superseded by inspection file `t1_reason`/`t2_reason`).
- Add `"valid": true` to the 5 entries that were `status: "verified"`.
- Remove all 25 entries that were `status: "candidate"` (they are not yet validated; they live in `universe_candidates.json` only).

**New schema for universe.json entries:**
```json
{
  "epic":        "IX.D.FTSE.DAILY.IP",
  "name":        "FTSE 100",
  "asset_class": "index",
  "region":      "UK",
  "valid":       true
}
```

**Updated `description` field:**
```
"Tradinator instrument universe — IG Demo epics that have passed two-tier validation (T1: broker recognition + dealing enabled; T2: price data available). Managed automatically by discover_universe.py and DataPipeline. Do not edit manually; edit universe_candidates.json to add new candidates."
```

**The resulting `instruments` array should contain exactly 5 entries:**
1. `IX.D.FTSE.DAILY.IP` / FTSE 100 / index / UK / valid: true
2. `IX.D.SPTRD.DAILY.IP` / US 500 (S&P 500) / index / US / valid: true
3. `CS.D.EURUSD.MINI.IP` / EUR/USD Mini / forex / global / valid: true
4. `CS.D.GBPUSD.MINI.IP` / GBP/USD Mini / forex / global / valid: true
5. `CC.D.CL.UMP.IP` / Oil - US Crude / commodity / global / valid: true

---

## Task 2 — Update `main.py` `_load_universe()`

**File:** `main.py`

The current `_load_universe()` function loads ALL entries from `universe.json` without any filtering. Since `universe.json` now only contains `valid: true` instruments, the function does not need to filter. However, add a safety guard that warns if any entry has `valid: false` (which should not happen but guards against manual edits).

**Specific change:** In the `for inst in instruments:` loop, add a guard after reading `epic`:

```python
if not inst.get("valid", True):
    print(f"WARNING: universe.json contains invalid instrument '{epic}' — skipping. "
          "Run discover_universe.py or edit universe_candidates.json.")
    continue
```

Place this guard immediately after `if not epic: continue`.

Do NOT change any other logic in `_load_universe()`.

Also update the `WARNING` message for an empty universe (after `if not instruments:`) to:
```python
print(f"WARNING: No instruments found in {path} — pipeline will run with an empty universe. "
      "Run discover_universe.py (or set run_discover=True in config) to populate it.")
```

---

## Task 3 — Update `skills/DataSource_Scoper.md`

The DataSource_Scoper spec references the old `status: "verified" | "candidate"` schema in several places. Update all references:

1. **Section 2.1 Permitted reads** — Add `data/input/universe_candidates.json` to the allowlist table.

2. **Section 3.1 Inputs / universe.json schema block** — Replace:
   ```
   "status":      "verified" | "candidate",
   "note":        str (optional)
   ```
   With:
   ```
   "valid":       true           // always true in universe.json (invalid instruments are excluded)
   ```

3. **Section 3.1 paragraph** after the schema block — Update:
   - `"Current state: 30 instruments — 5 verified, 25 candidate."` → `"Current state: 5 valid instruments (previously verified; pending re-validation)."`
   - Remove the paragraph about `unknown_status` for values other than "verified"/"candidate" — replace with: `"Instruments with valid: false are reported under invalid_instruments (should not normally appear in universe.json)."`

4. **Section 3.2 Section A — Universe Scope table** — Replace:
   - `verified_count` → `valid_count` (int — count with `valid == true`)
   - `candidate_count` → remove (candidates live in `universe_candidates.json`)
   - `verified_epics` → `valid_epics` (list[str])
   - `candidate_epics` → remove
   - `unknown_status` → `invalid_instruments` (list[str] — epics where `valid` is false; normally empty)

5. **Section 4 Scope 1** — Update wording: replace references to "verified" and "candidate" with "valid" and "invalid". Remove reference to discover_universe.py setting status; replace with discover_universe.py sets the `valid` flag via two-tier validation.

6. **Section 7.1 Assets that must not be touched** — Add:
   ```
   | `data/input/universe_candidates.json` | Data file | Candidate registry; written by discover_universe.py and DataPipeline |
   ```

---

## Acceptance Criteria

1. `universe.json` is valid JSON with exactly 5 entries, all `valid: true`, no `status` or `note` fields.
2. `main.py` compiles without errors (`python -c "import ast; ast.parse(open('main.py').read())"`).
3. The safety guard is present and correctly placed.
4. `DataSource_Scoper.md` no longer references `"status"`, `"verified"`, or `"candidate"` in the context of universe.json schema.

---

## Files to Touch

| File | Action |
|---|---|
| `data/input/universe.json` | MODIFY — migrate schema |
| `main.py` | MODIFY — add safety guard, update warning message |
| `skills/DataSource_Scoper.md` | MODIFY — update schema references |
