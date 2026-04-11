#!/usr/bin/env python3
"""Run repository tests via pytest (fixture-compatible)."""

from __future__ import annotations

import pathlib
import sys


def main(argv: list[str] | None = None) -> int:
    root = pathlib.Path(__file__).resolve().parents[1]
    src_dir = root / "src"
    if src_dir.exists():
        sys.path.insert(0, str(src_dir))

    tests_dir = root / "tests"
    if tests_dir.exists():
        sys.path.insert(0, str(tests_dir))

    try:
        import pytest
    except ModuleNotFoundError:
        print("pytest is required to run tests. Install test dependencies and retry.")
        return 2

    args = list(argv if argv is not None else sys.argv[1:])
    if not args:
        args = ["-q", "tests"]
    return int(pytest.main(args))


if __name__ == "__main__":
    raise SystemExit(main())
