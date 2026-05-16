"""
Tradinator — Universe Refresher.

Re-runs the Tier-1 contract-resolution check against the broker adapter for
every candidate in ``universe_candidates.json`` and rewrites
``universe.json`` with only the candidates that currently pass T1.

T2 (data availability) is *not* re-validated here — that is owned by
``DataPipeline``.

Invariants
----------
* ``universe_candidates.json`` is *never* shrunk: only the per-candidate
  ``t1_status`` / ``last_validated`` fields are mutated.
* ``universe.json`` is fully replaced and may shrink.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

from __future__ import annotations

import json
import os
import time


# ---------------------------------------------------------------------------
# Canonical T1 status string constants (re-used by stock_scoper / DataPipeline).
# ---------------------------------------------------------------------------
T1_PASS       = "PASS"
T1_FAIL       = "FAIL"
T1_API_ERROR  = "API_ERROR"
T2_PENDING    = "PENDING_T2"


class UniverseRefresher:
    """Re-validate every candidate against the broker and rewrite universe.json."""

    def __init__(self, config: dict):
        """Store config; paths are taken from it with safe defaults."""
        self.config = config
        self.candidates_path = config.get(
            "universe_candidates_path",
            os.path.join("data", "input", "universe_candidates.json"),
        )
        self.universe_path = config.get(
            "universe_path",
            os.path.join("data", "input", "universe.json"),
        )

    def run(self, adapter) -> None:
        """Re-run T1 for every candidate and rewrite universe.json.

        ``adapter`` must be a connected ``BrokerAdapter``.  Only the
        ``t1_status`` / ``last_validated`` fields on each
        candidate are updated; the candidate list itself is preserved
        (never shrunk).
        """
        candidates_doc = self._load_candidates_doc()
        candidates = candidates_doc.get("candidates", [])
        original_count = len(candidates)

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
                    "conId":       cid,
                    "name":        cand.get("name", ""),
                    "sec_type":    cand.get("sec_type", ""),
                    "exchange":    cand.get("exchange", ""),
                    "currency":    cand.get("currency", ""),
                    "yh_ticker":   cand.get("yh_ticker"),
                    "asset_class": cand.get("asset_class", ""),
                    "region":      cand.get("region", ""),
                    "valid":       True,
                })

        # Guard: never shrink the candidates list.
        if len(candidates) != original_count:
            raise RuntimeError(
                "UniverseRefresher must not shrink universe_candidates.json"
            )

        candidates_doc["candidates"]     = candidates
        candidates_doc["last_t1_run"]    = now_utc
        self._write_json(self.candidates_path, candidates_doc)

        universe_doc = {
            "description": (
                "Tradinator instrument universe — IBKR contracts that have "
                "passed Tier 1 validation (reqContractDetails resolves). "
                f"Last T1 run: {now_utc}."
            ),
            "instruments": passing,
        }
        self._write_json(self.universe_path, universe_doc)
        print(
            f"[UniverseRefresher] T1: {len(passing)}/{original_count} pass — "
            f"wrote {self.universe_path}"
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_candidates_doc(self) -> dict:
        """Read universe_candidates.json; return a doc with at least 'candidates'."""
        if not os.path.isfile(self.candidates_path):
            return {"candidates": []}
        try:
            with open(self.candidates_path, encoding="utf-8") as f:
                doc = json.load(f)
        except Exception as exc:
            print(
                f"[UniverseRefresher] WARNING: could not read "
                f"{self.candidates_path} — {exc}"
            )
            return {"candidates": []}
        if "candidates" not in doc:
            doc["candidates"] = []
        return doc

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

    @staticmethod
    def _write_json(path: str, doc: dict) -> None:
        """Write *doc* to *path* as indented JSON with a trailing newline."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2)
            f.write("\n")
