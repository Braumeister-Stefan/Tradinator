"""
Tradinator — Handoff.

Persistent bridge between the Research and Execution phases in Decoupled
mode.  Research writes a target portfolio to a JSON file; Execution reads
it.  The file acts as the sole coupling point between the two halves of
the pipeline when they run on independent schedules.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import copy
import datetime
import json
import os


class Handoff:
    """Atomic read/write of research output for decoupled execution."""

    FILENAME = "handoff.json"
    TMP_FILENAME = "handoff.tmp"

    @staticmethod
    def write(research_output, output_dir):
        """Atomically persist research_output to handoff.json.

        Excludes the non-serializable broker session object from
        broker_state before writing.
        """
        data = copy.deepcopy(research_output)

        # Remove non-serializable IGService session object
        if "broker_state" in data and "session" in data["broker_state"]:
            del data["broker_state"]["session"]

        data["written_at"] = datetime.datetime.utcnow().isoformat()

        os.makedirs(output_dir, exist_ok=True)
        tmp_path = os.path.join(output_dir, Handoff.TMP_FILENAME)
        final_path = os.path.join(output_dir, Handoff.FILENAME)

        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)

        os.replace(tmp_path, final_path)
        print(f"[Handoff] Written to {final_path}")

    @staticmethod
    def read(output_dir, max_age_seconds=7200):
        """Load handoff.json and return its contents if fresh, else None.

        Returns None if the file is missing, corrupt, or stale (older
        than max_age_seconds).
        """
        path = os.path.join(output_dir, Handoff.FILENAME)

        if not os.path.isfile(path):
            print("[Handoff] No handoff file found.")
            return None

        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (json.JSONDecodeError, OSError):
            print("[Handoff] ⚠ Corrupt handoff file.")
            return None

        written_at_str = data.get("written_at")
        if not written_at_str:
            print("[Handoff] ⚠ Handoff file missing timestamp.")
            return None

        try:
            written_at = datetime.datetime.fromisoformat(written_at_str)
        except ValueError:
            print("[Handoff] ⚠ Handoff file has invalid timestamp.")
            return None

        age = (datetime.datetime.utcnow() - written_at).total_seconds()
        if age > max_age_seconds:
            print(
                f"[Handoff] ⚠ Handoff stale ({age:.0f}s > {max_age_seconds}s), skipping."
            )
            return None

        print(f"[Handoff] Loaded handoff ({age:.0f}s old).")
        return data
