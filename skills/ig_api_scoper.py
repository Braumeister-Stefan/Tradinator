"""IG API Call Scoper — static analysis tool.

Walks all .py files in the repository (excluding skills/, .git/, cache dirs,
and virtual environments), uses Python's ``ast`` module to detect every call
to a trading_ig ``IGService`` method, and writes the inventory to
``skills/ig_api_calls.csv``.

Run from the repository root:
    python skills/ig_api_scoper.py

Output: skills/ig_api_calls.csv

Guardrails enforced:
    - Read-only: no writes to data/, secrets/, or production source files.
    - No broker calls, no pipeline imports.
    - Errors in individual files are logged and skipped; the tool never aborts.
"""

import ast
import csv
import os
import sys

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_CSV = os.path.join(REPO_ROOT, "skills", "ig_api_calls.csv")

# Directories to skip entirely during the walk.
EXCLUDED_DIRS = {
    "skills",
    ".git",
    "__pycache__",
    "venv",
    ".venv",
    "env",
    ".env",
    "node_modules",
}

# Known IGService method names and their logical category.
IG_METHOD_CATEGORIES = {
    "create_session": "session",
    "fetch_accounts": "account",
    "fetch_open_positions": "account",
    "fetch_market_by_epic": "market_data",
    "fetch_historical_prices_by_epic_and_num_points": "market_data",
    "fetch_historical_prices_by_epic_and_date_range": "market_data",
    "search_markets": "market_data",
    "create_open_position": "order",
    "close_open_position": "order",
    "fetch_deal_by_deal_reference": "order",
}

# Heuristic keywords that, when present in the surrounding context, indicate
# retry logic is already in place at that call site.
RETRY_KEYWORDS = {
    "attempt",
    "retry",
    "backoff",
    "wait",
    "ApiExceededException",
    "RateLimit",
    "Exceeded",
    "_call_ig_api",
    "_api_call_with_retry",
}


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------

class _FunctionFinder(ast.NodeVisitor):
    """Maps every line number to the name of the innermost enclosing function."""

    def __init__(self):
        self._stack: list[str] = []
        self.line_to_func: dict[int, str] = {}

    def visit_FunctionDef(self, node: ast.FunctionDef):
        self._stack.append(node.name)
        for lineno in range(node.lineno, (node.end_lineno or node.lineno) + 1):
            self.line_to_func[lineno] = node.name
        self.generic_visit(node)
        self._stack.pop()

    visit_AsyncFunctionDef = visit_FunctionDef  # handle async defs too


def _build_line_to_func(tree: ast.AST) -> dict[int, str]:
    """Return a mapping of line → enclosing function name for the parsed AST."""
    finder = _FunctionFinder()
    finder.visit(tree)
    return finder.line_to_func


class _IGCallFinder(ast.NodeVisitor):
    """Collects every attribute call that matches a known IG API method name.

    Handles two patterns:
    1. Direct call: ``ig.method_name(...)``
    2. Wrapper call: ``_call_ig_api(ig.method_name, ...)`` or
       ``self._call_ig_api(ig.method_name, ...)`` where the IG method name
       is passed as a callable argument (not invoked directly).
    """

    def __init__(self, line_to_func: dict[int, str], source_lines: list[str]):
        self.line_to_func = line_to_func
        self.source_lines = source_lines
        self.hits: list[dict] = []
        self._seen: set[tuple] = set()  # deduplicate (line, method) pairs

    def _add_hit(self, line: int, method: str) -> None:
        """Record one IG API call site (deduplicated by line + method)."""
        key = (line, method)
        if key in self._seen:
            return
        self._seen.add(key)
        func = self.line_to_func.get(line, "<module>")
        category = IG_METHOD_CATEGORIES[method]
        has_retry = self._detect_retry(func, line)
        self.hits.append(
            {
                "line": line,
                "function": func,
                "ig_method": method,
                "category": category,
                "has_retry": has_retry,
                "notes": "",
            }
        )

    def visit_Call(self, node: ast.Call):
        # Pattern 1 — direct call: ig.method_name(...)
        if isinstance(node.func, ast.Attribute):
            method = node.func.attr
            if method in IG_METHOD_CATEGORIES:
                self._add_hit(node.lineno, method)

        # Pattern 2 — wrapper call: fn(ig.method_name, ...) where the IG
        # method is passed as a callable reference (ast.Attribute, not ast.Call).
        for arg in node.args:
            if isinstance(arg, ast.Attribute) and arg.attr in IG_METHOD_CATEGORIES:
                self._add_hit(arg.lineno, arg.attr)

        self.generic_visit(node)

    def _detect_retry(self, func_name: str, call_line: int) -> bool:
        """Return True if any retry-indicator keyword appears within ±30 lines."""
        start = max(0, call_line - 30 - 1)
        end = min(len(self.source_lines), call_line + 30)
        window = " ".join(self.source_lines[start:end])
        return any(kw in window for kw in RETRY_KEYWORDS)


# ---------------------------------------------------------------------------
# File walking
# ---------------------------------------------------------------------------

def _collect_py_files(root: str) -> list[str]:
    """Yield relative paths of all .py files under root, honouring exclusions."""
    result = []
    for dirpath, dirnames, filenames in os.walk(root):
        # Prune excluded directories in-place so os.walk does not descend.
        dirnames[:] = [
            d for d in dirnames if d not in EXCLUDED_DIRS
        ]
        for filename in filenames:
            if filename.endswith(".py"):
                full_path = os.path.join(dirpath, filename)
                rel_path = os.path.relpath(full_path, root)
                result.append(rel_path)
    return sorted(result)


# ---------------------------------------------------------------------------
# Per-file analysis
# ---------------------------------------------------------------------------

def _analyse_file(rel_path: str, repo_root: str) -> list[dict]:
    """Parse one file and return a list of IG API call records."""
    full_path = os.path.join(repo_root, rel_path)
    try:
        with open(full_path, encoding="utf-8") as fh:
            source = fh.read()
    except OSError as exc:
        print(f"[scoper] WARNING: cannot read {rel_path} — {exc}", file=sys.stderr)
        return []

    try:
        tree = ast.parse(source, filename=rel_path)
    except SyntaxError as exc:
        print(
            f"[scoper] WARNING: syntax error in {rel_path} — {exc}",
            file=sys.stderr,
        )
        return []

    source_lines = source.splitlines()
    line_to_func = _build_line_to_func(tree)
    finder = _IGCallFinder(line_to_func, source_lines)
    finder.visit(tree)

    records = []
    for hit in finder.hits:
        records.append(
            {
                "file": rel_path,
                "function": hit["function"],
                "line": hit["line"],
                "ig_method": hit["ig_method"],
                "category": hit["category"],
                "has_retry": str(hit["has_retry"]),
                "notes": hit["notes"],
            }
        )
    return records


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

CSV_FIELDS = ["file", "function", "line", "ig_method", "category", "has_retry", "notes"]


def _write_csv(records: list[dict], output_path: str) -> None:
    """Write records to a CSV file, creating parent directories if needed."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(records)
    print(f"[scoper] Wrote {len(records)} record(s) to {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(repo_root: str = REPO_ROOT, output_csv: str = OUTPUT_CSV) -> list[dict]:
    """Scan the repository and write ig_api_calls.csv.  Returns the record list."""
    py_files = _collect_py_files(repo_root)
    print(f"[scoper] Scanning {len(py_files)} Python file(s) under {repo_root}")

    all_records: list[dict] = []
    for rel_path in py_files:
        records = _analyse_file(rel_path, repo_root)
        if records:
            print(
                f"[scoper]   {rel_path}: {len(records)} IG API call(s) found"
            )
        all_records.extend(records)

    # Sort by file then line for deterministic output.
    all_records.sort(key=lambda r: (r["file"], int(r["line"])))

    _write_csv(all_records, output_csv)
    print(
        f"[scoper] Complete — {len(all_records)} total IG API call(s) "
        f"across {len({r['file'] for r in all_records})} file(s)."
    )
    return all_records


if __name__ == "__main__":
    run()
