"""Minimal .env-style config loader (stdlib only)."""

from __future__ import annotations

import os


def load_env_config(env_path: str) -> dict:
    """Parse a KEY=VALUE .env file and return a dict.

    Blank lines and lines beginning with ``#`` are ignored. Keys are kept
    verbatim (case preserved). Surrounding whitespace and matching single or
    double quotes around values are stripped. Returns an empty dict when the
    file does not exist (with a printed warning, matching project style).
    """
    if not os.path.isfile(env_path):
        print(f"WARNING: env file not found at {env_path} — continuing with empty config.")
        return {}

    out: dict = {}
    with open(env_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            out[key] = value
    return out
