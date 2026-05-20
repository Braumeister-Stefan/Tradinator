"""
Tradinator — Universe Refresher.

Re-runs the Tier-1 contract-resolution check against the broker adapter for
every candidate in ``universe_candidates.csv`` and rewrites
``universe.csv`` with only the candidates that currently pass T1.

T2 (data availability) is *not* re-validated here — that is owned by
``DataPipeline``.

Invariants
----------
* ``universe_candidates.csv`` is *never* shrunk: only the per-candidate
  ``t1_status`` / ``last_validated`` fields are mutated.
* ``universe.csv`` is fully replaced and may shrink.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

from __future__ import annotations

import os
import time

from data.input import registry_io


# ---------------------------------------------------------------------------
# Canonical T1 status string constants (re-used by stock_scoper / DataPipeline).
# ---------------------------------------------------------------------------
T1_PASS       = "PASS"
T1_FAIL       = "FAIL"
T1_API_ERROR  = "API_ERROR"
T2_PENDING    = "PENDING_T2"


class UniverseRefresher:
    """Re-validate every candidate against the broker and rewrite universe.csv."""

    def __init__(self, config: dict):
        """Store config; paths are taken from it with safe defaults."""
        self.config = config
        self.candidates_path = config.get(
            "universe_candidates_path",
            os.path.join("data", "input", "universe_candidates.csv"),
        )
        self.universe_path = config.get(
            "universe_path",
            os.path.join("data", "input", "universe.csv"),
        )

    def run(self, adapter) -> None:
        """Re-run T1 for every candidate and rewrite universe.csv.

        ``adapter`` must be a connected ``BrokerAdapter``.  Only the
        ``t1_status`` / ``last_validated`` fields on each
        candidate are updated; the candidate list itself is preserved
        (never shrunk).
        """
        candidates = registry_io.load_candidate_rows(self.candidates_path)
        original_count = len(candidates)

        # Sync user-managed overwrite_exclusion flags from the existing
        # universe.csv onto the candidate rows. Candidates are the durable
        # store for the flag, so it survives T1 failures (when a conId is
        # temporarily dropped from universe.csv).
        existing_universe = registry_io.load_universe_rows(self.universe_path)
        universe_exclusion: dict[str, bool] = {
            str(r.get("conId", "") or ""): bool(r.get("overwrite_exclusion", False))
            for r in existing_universe
        }
        for cand in candidates:
            cid = str(cand.get("conId", "") or "")
            if cid in universe_exclusion:
                cand["overwrite_exclusion"] = universe_exclusion[cid]

        now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        passing: list[dict] = []
        for cand in candidates:
            cid = str(cand.get("conId", "") or "")
            t1_status, t1_reason = self._validate_tier1(adapter, cand)
            cand["t1_status"]      = t1_status
            cand["last_validated"] = now_utc
            print(f"[UniverseRefresher] T1 {t1_status} {cid or cand.get('name','')} — {t1_reason}")
            if t1_status == T1_PASS:
                passing.append({
                    "conId":               cid,
                    "name":                cand.get("name", ""),
                    "sec_type":            cand.get("sec_type", ""),
                    "exchange":            cand.get("exchange", ""),
                    "currency":            cand.get("currency", ""),
                    "asset_class":         cand.get("asset_class", ""),
                    "region":              cand.get("region", ""),
                    "valid":               True,
                    "overwrite_exclusion": bool(cand.get("overwrite_exclusion", False)),
                })
            elif bool(cand.get("overwrite_exclusion", False)):
                print(
                    f"[UniverseRefresher] NOTE: user-excluded conId '{cid}' "
                    f"failed T1 ({t1_status}); exclusion flag retained on "
                    f"candidate and will be reapplied if T1 passes again."
                )

        # Guard: never shrink the candidates list.
        if len(candidates) != original_count:
            raise RuntimeError(
                "UniverseRefresher must not shrink universe_candidates.csv"
            )

        registry_io.save_candidate_rows(candidates, self.candidates_path)
        registry_io.update_candidate_meta({"last_t1_run": now_utc})

        registry_io.save_universe_rows(passing, self.universe_path)
        registry_io.write_universe_meta({
            "description": (
                "Tradinator instrument universe — IBKR contracts that have "
                "passed Tier 1 validation (reqContractDetails resolves). "
                f"Last T1 run: {now_utc}."
            )
        })
        print(
            f"[UniverseRefresher] T1: {len(passing)}/{original_count} pass — "
            f"wrote {self.universe_path}"
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_tier1(adapter, candidate: dict) -> tuple[str, str]:
        """Resolve the contract via the adapter; return (t1_status, reason)."""
        cid = str(candidate.get("conId", "") or "")
        if not cid:
            return T1_FAIL, "candidate has no conId"
        try:
            # Adapters expose ``fetch_instrument_info`` which internally calls
            # reqContractDetails — a successful call confirms T1.
            info = adapter.fetch_instrument_info(cid)
        except Exception as exc:
            return T1_API_ERROR, f"adapter raised: {exc}"
        if not info:
            return T1_FAIL, "adapter returned empty instrument info"
        return T1_PASS, "contract resolved via adapter"
