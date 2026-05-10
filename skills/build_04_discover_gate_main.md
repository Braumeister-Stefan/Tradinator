# BUILD PROMPT 04 — Discover Gate in main.py (Config Key + CLI Arg)

**Prepared by:** FUNCTIONALIST  
**Scope:** Add a `run_discover` config key (default `False`) and a `--discover` CLI argument to `main.py`. When either is `True`, invoke the universe discovery and validation logic from `discover_universe.py` before the main pipeline runs.

**Must run AFTER:** Build 03 (discover_universe.py refactored with importable entry point)

---

## Context

The repository is at `/home/runner/work/Tradinator/Tradinator`.

Currently `discover_universe.py` is a standalone script with no connection to `main.py`. The user wants discovery to be gated: it should only run when explicitly requested, and the default should be `False`.

**Decision (Q3-c):** Both a config key AND a CLI argument control the gate:
- `config["run_discover"]` (default `False`) — persists across runs; set it to `True` in `main.py` to make discovery the default.
- `--discover` CLI flag — per-run opt-in; overrides the config key to `True` for that run.

---

## Task 1 — Add `run_discover` to config dict in `main.py`

In the `config` dict (around line 121), add one new entry in the `# Universe` section:

```python
# Universe -----------------------------------------------------------
"universe_path": UNIVERSE_PATH,     # path to universe JSON file
"universe": _load_universe(UNIVERSE_PATH),
"run_discover": False,              # set True or use --discover to re-validate universe
```

---

## Task 2 — Add `--discover` CLI argument to `_parse_args()`

In the `_parse_args()` function, add a new argument after the existing `--execution-interval` argument:

```python
parser.add_argument(
    "--discover",
    action="store_true",
    default=False,
    help=(
        "Run discover_universe.py validation before the main pipeline. "
        "Validates all candidates in universe_candidates.json against the IG API "
        "(Tier 1: broker recognition + dealing enabled; Tier 2: price data available) "
        "and updates universe.json with only the valid instruments. "
        "Equivalent to setting run_discover=True in config. Default: False."
    ),
)
```

---

## Task 3 — Import and invoke discover_universe in `__main__` block

The `if __name__ == "__main__":` block currently calls `Model(config)` immediately. Add discovery invocation before `Model(config)`.

**Step 3a — Add import at the top of main.py** (after the existing imports):

```python
import importlib.util as _importlib_util
import os as _os
```

These are likely already imported. The discover invocation should use a function-based import to avoid executing discover_universe.py at import time. Add this helper near the top of the file (before `config`):

```python
def _run_discover(config: dict) -> None:
    """Invoke discover_universe.main() to validate/update the universe."""
    discover_path = _os.path.join(
        _os.path.dirname(_os.path.abspath(__file__)),
        "data", "input", "discover_universe.py",
    )
    spec = _importlib_util.spec_from_file_location("discover_universe", discover_path)
    module = _importlib_util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()
```

**Step 3b — Invoke `_run_discover()` in `__main__` block:**

Update the `if __name__ == "__main__":` block to:

```python
if __name__ == "__main__":
    args = _parse_args()

    # Merge CLI --discover flag into config
    if args.discover:
        config["run_discover"] = True

    try:
        # --- Universe discovery (optional, gated by config) ---
        if config.get("run_discover", False):
            print("\n[main] run_discover=True — running universe validation...")
            _run_discover(config)
            # Reload universe after discovery updates universe.json
            config["universe"] = _load_universe(UNIVERSE_PATH)
            print(f"[main] Universe reloaded: {len(config['universe'])} valid instrument(s).\n")

        model = Model(config)
        run_loop = RunLoop(
            model,
            args.mode,
            interval=args.interval,
            research_interval=args.research_interval,
            execution_interval=args.execution_interval,
        )
        run_loop.start()
    except NotImplementedError as error:
        ...  # keep existing error handlers unchanged
```

Keep ALL existing `except` handlers exactly as they are. Only add the new `if config.get("run_discover")` block before `model = Model(config)`.

---

## Task 4 — Update README.md command-line arguments table

In the README `### Command-line arguments` table, add one row:

```
| `--discover` | (not set) | Run universe validation before the pipeline (see `data/input/discover_universe.py`) |
```

And in the `## Configuration` section, add `"run_discover"` to the config table:

```python
"run_discover": False,             # True or --discover flag to re-validate universe
```

---

## Acceptance Criteria

1. `python main.py --help` shows `--discover` argument without errors.
2. `python -c "import ast; ast.parse(open('main.py').read())"` passes.
3. `config["run_discover"]` defaults to `False`.
4. When `args.discover` is `True`, `config["run_discover"]` is set to `True` before the pipeline starts.
5. Discovery only runs when `config["run_discover"]` is `True`.
6. After discovery, `config["universe"]` is refreshed from the updated `universe.json`.
7. All existing error handlers (NotImplementedError, RuntimeError) are unchanged.

---

## Files to Touch

| File | Action |
|---|---|
| `main.py` | MODIFY — add config key, CLI arg, discovery invocation |
| `README.md` | MODIFY — add `--discover` to CLI args table and config block |
