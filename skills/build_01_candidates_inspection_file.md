# BUILD PROMPT 01 — Create universe_candidates.json (Inspection File)

**Prepared by:** FUNCTIONALIST  
**Scope:** Create a new file `data/input/universe_candidates.json` that holds ALL 30 universe candidates with their full validation metadata. This is the human-readable inspection file. The existing `universe.json` is NOT touched by this task.

---

## Context

The repository is at `/home/runner/work/Tradinator/Tradinator`.

Currently `data/input/universe.json` holds 30 instruments with a simple `status: "verified" | "candidate"` field. Validation failure reasons are never persisted — they are printed to stdout and lost.

The new design separates concerns:
- `universe.json` → machine-read by the pipeline; contains ONLY `valid: true` instruments.
- `universe_candidates.json` (this task) → human-readable; contains ALL 30 candidates with full validation metadata.

---

## Task

Create `data/input/universe_candidates.json` as a new file. Do NOT modify any existing file.

### Schema

```json
{
  "description": "Tradinator universe candidate registry. Contains all candidate instruments with two-tier validation metadata. Edit this file to add new candidates; do not edit universe.json directly.",
  "last_discover_run": null,
  "candidates": [
    {
      "epic":        "<IG epic string>",
      "name":        "<human-readable name>",
      "asset_class": "<index | forex | commodity>",
      "region":      "<UK | US | EU | APAC | global>",
      "t1_status":   "<UNTESTED | PASS | EPIC_NOT_RECOGNIZED | DEALING_DISABLED | API_ERROR>",
      "t1_reason":   "<freeform string or null>",
      "t2_status":   "<NEVER_TRIED | PENDING_T2 | YES | NO>",
      "t2_reason":   "<freeform string or null>",
      "valid":       false,
      "last_validated": null
    }
  ]
}
```

### Field Definitions

| Field | Type | Description |
|---|---|---|
| `epic` | str | IG epic identifier, e.g. `IX.D.FTSE.DAILY.IP` |
| `name` | str | Human-readable instrument name |
| `asset_class` | str | `"index"` \| `"forex"` \| `"commodity"` |
| `region` | str | `"UK"` \| `"US"` \| `"EU"` \| `"APAC"` \| `"global"` |
| `t1_status` | str | Tier 1 validation outcome (see below) |
| `t1_reason` | str\|null | Freeform explanation for the T1 outcome |
| `t2_status` | str | Tier 2 validation outcome (see below) |
| `t2_reason` | str\|null | Freeform explanation for the T2 outcome |
| `valid` | bool | `true` only when T1=PASS AND T2=YES |
| `last_validated` | str\|null | ISO-8601 UTC timestamp of most recent validation attempt, or `null` |

**T1 status vocabulary:**

| Value | Meaning |
|---|---|
| `UNTESTED` | Tier 1 has never been attempted |
| `PASS` | Broker recognises the epic AND `dealingEnabled=true` |
| `EPIC_NOT_RECOGNIZED` | `fetch_market_by_epic` raised an exception or returned no market data |
| `DEALING_DISABLED` | Epic found but `dealingEnabled=false` in market metadata |
| `API_ERROR` | Any other exception during the T1 API call |

**T2 status vocabulary:**

| Value | Meaning |
|---|---|
| `NEVER_TRIED` | T1 has not been run yet (also covers T1 failed — no point fetching) |
| `PENDING_T2` | T1 passed but the main pipeline has not yet run a data fetch for this epic |
| `YES` | Most recent data fetch returned ≥1 bar |
| `NO` | Most recent data fetch returned zero bars |

### Initial Data Population

Populate `candidates` with all 30 instruments from the current `data/input/universe.json`.  
The source instruments are listed below; copy the `epic`, `name`, `asset_class`, and `region` from each.  
Set ALL fields to their initial/untested values:

```
t1_status: "UNTESTED"
t1_reason: null
t2_status: "NEVER_TRIED"
t2_reason: null
valid: false
last_validated: null
```

The 5 instruments that currently have `status: "verified"` in `universe.json` should receive no special treatment here — they are also set to UNTESTED, because the user has requested re-validation from scratch.

Drop the `note` field (it is superseded by `t1_reason` / `t2_reason`). Do NOT carry `status` into the new file.

**Source instruments (copy epic/name/asset_class/region from universe.json exactly):**

1. `IX.D.FTSE.DAILY.IP` / FTSE 100 / index / UK
2. `IX.D.SPTRD.DAILY.IP` / US 500 (S&P 500) / index / US
3. `CS.D.EURUSD.MINI.IP` / EUR/USD Mini / forex / global
4. `CS.D.GBPUSD.MINI.IP` / GBP/USD Mini / forex / global
5. `CC.D.CL.UMP.IP` / Oil - US Crude / commodity / global
6. `IX.D.DAX.DAILY.IP` / Germany 40 (DAX) / index / EU
7. `IX.D.DOW.DAILY.IP` / Wall Street (DJIA) / index / US
8. `CS.D.USDJPY.MINI.IP` / USD/JPY Mini / forex / global
9. `CC.D.GC.UMP.IP` / Gold / commodity / global
10. `CC.D.LCO.UMP.IP` / Oil - Brent Crude / commodity / global
11. `IX.D.NASDAQ.DAILY.IP` / US Tech 100 (Nasdaq) / index / US
12. `IX.D.CAC.DAILY.IP` / France 40 (CAC 40) / index / EU
13. `IX.D.NIKKEI.DAILY.IP` / Japan 225 (Nikkei) / index / APAC
14. `CC.D.SILVER.UMP.IP` / Silver / commodity / global
15. `CS.D.AUDUSD.MINI.IP` / AUD/USD Mini / forex / global
16. `IX.D.ASX.DAILY.IP` / Australia 200 / index / APAC
17. `IX.D.HSENG.DAILY.IP` / Hang Seng / index / APAC
18. `IX.D.STXE.DAILY.IP` / Euro Stoxx 50 / index / EU
19. `CS.D.EURGBP.MINI.IP` / EUR/GBP Mini / forex / global
20. `CC.D.NGAS.UMP.IP` / Natural Gas / commodity / global
21. `IX.D.RUSSELL.DAILY.IP` / Russell 2000 / index / US
22. `CS.D.USDCAD.MINI.IP` / USD/CAD Mini / forex / global
23. `CC.D.COPPER.UMP.IP` / Copper / commodity / global
24. `CS.D.EURJPY.MINI.IP` / EUR/JPY Mini / forex / global
25. `IX.D.AEX.DAILY.IP` / Netherlands 25 (AEX) / index / EU
26. `IX.D.IBEX.DAILY.IP` / Spain 35 (IBEX 35) / index / EU
27. `IX.D.SMI.DAILY.IP` / Switzerland Blue Chip (SMI) / index / EU
28. `IX.D.ITLY.DAILY.IP` / Italy 40 (FTSE MIB) / index / EU
29. `CS.D.USDCHF.MINI.IP` / USD/CHF Mini / forex / global
30. `CS.D.NZDUSD.MINI.IP` / NZD/USD Mini / forex / global

---

## Acceptance Criteria

1. File `data/input/universe_candidates.json` is created.
2. It contains exactly 30 entries in the `candidates` array.
3. Every entry has all 8 candidate fields (`epic`, `name`, `asset_class`, `region`, `t1_status`, `t1_reason`, `t2_status`, `t2_reason`, `valid`, `last_validated`).
4. All entries have `t1_status: "UNTESTED"`, `t2_status: "NEVER_TRIED"`, `valid: false`, `last_validated: null`.
5. The file is valid JSON (run `python -c "import json; json.load(open('data/input/universe_candidates.json'))"` to verify).
6. No existing file is modified.

---

## Files to Touch

| File | Action |
|---|---|
| `data/input/universe_candidates.json` | CREATE (new file) |
