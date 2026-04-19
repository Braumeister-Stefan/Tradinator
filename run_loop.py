"""
Tradinator — Run Loop.

Controls *when* the pipeline runs. Model retains *what* runs.
Supports four modes: run_once, research_only, scheduled, decoupled.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import threading
import time


class RunLoop:
    """Schedule and dispatch pipeline execution based on the chosen mode."""

    def __init__(self, model, mode, interval=3600, research_interval=14400,
                 execution_interval=3600):
        """Store model and scheduling parameters."""
        self.model = model
        self.mode = mode
        self.interval = interval
        self.research_interval = research_interval
        self.execution_interval = execution_interval
        self._latest_research = None

    def start(self):
        """Dispatch execution based on the configured mode."""
        try:
            if self.mode == "run_once":
                self._run_once()
            elif self.mode == "research_only":
                self._research_only()
            elif self.mode == "scheduled":
                self._scheduled()
            elif self.mode == "decoupled":
                self._decoupled()
            else:
                raise ValueError(f"Unknown mode: {self.mode}")
        except KeyboardInterrupt:
            print("[RunLoop] Shutting down.")

    def _run_once(self):
        """Execute the full pipeline once and return."""
        self.model.run()

    def _research_only(self):
        """Execute only the research phase and return."""
        self.model.run_research()
        print("[RunLoop] Research complete.")

    def _scheduled(self):
        """Run the full pipeline on a fixed interval."""
        while True:
            try:
                self.model.run()
            except Exception as exc:
                print(f"[RunLoop] ⚠ Error: {exc}")
            time.sleep(self.interval)

    def _decoupled(self):
        """Run research and execution on independent schedules."""
        research_thread = threading.Thread(
            target=self._research_loop, daemon=True
        )
        execution_thread = threading.Thread(
            target=self._execution_loop, daemon=True
        )
        research_thread.start()
        execution_thread.start()
        threading.Event().wait()

    def _research_loop(self):
        """Loop: run research, store result, sleep."""
        while True:
            try:
                result = self.model.run_research()
                self._latest_research = result
            except Exception as exc:
                print(f"[RunLoop] ⚠ Error in research: {exc}")
            time.sleep(self.research_interval)

    def _execution_loop(self):
        """Loop: read latest research, run execution, sleep."""
        while True:
            try:
                research = self._latest_research
                if research is None:
                    print("[RunLoop] No research available yet, skipping execution.")
                else:
                    self.model.run_execution(research)
            except Exception as exc:
                print(f"[RunLoop] ⚠ Error in execution: {exc}")
            time.sleep(self.execution_interval)
