#!/usr/bin/env python3
"""Lightweight code-shape guard for Python modules.

Checks:
- file length (line count)
- function/method length (AST span, inclusive lines)

Usage:
  python scripts/check_code_shape.py
  python scripts/check_code_shape.py --root src/device_hub
"""

from __future__ import annotations

import argparse
import ast
import fnmatch
import json
import pathlib
import sys
from dataclasses import dataclass


DEFAULT_EXCLUDES = (
    "*/tests/*",
    "*/test_*.py",
    "*/migrations/*",
    "*/__pycache__/*",
    "*/.venv/*",
    "*/.pytest_cache/*",
)
DEFAULT_FAIL_BASELINE_PATH = "governance/baselines/code-shape-fail-baseline.json"


@dataclass(frozen=True)
class Finding:
    level: str
    kind: str
    path: pathlib.Path
    lineno: int
    metric: int
    threshold: int
    name: str = ""

    def format(self) -> str:
        suffix = f" ({self.name})" if self.name else ""
        return (
            f"{self.path}:{self.lineno}: {self.level} {self.kind}{suffix}: "
            f"{self.metric} > {self.threshold}"
        )


def _iter_python_files(
    root: pathlib.Path,
    *,
    include_glob: str,
    exclude_globs: tuple[str, ...],
) -> list[pathlib.Path]:
    files: list[pathlib.Path] = []
    for path in root.rglob(include_glob):
        path_str = str(path)
        if any(fnmatch.fnmatch(path_str, pat) for pat in exclude_globs):
            continue
        files.append(path)
    return sorted(files)


def _file_line_count(path: pathlib.Path) -> int:
    text = path.read_text(encoding="utf-8")
    if not text:
        return 0
    return len(text.splitlines())


def _function_spans(path: pathlib.Path) -> list[tuple[str, int, int]]:
    text = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(text, filename=str(path))
    except SyntaxError:
        return []

    spans: list[tuple[str, int, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        start = getattr(node, "lineno", 1) or 1
        end = getattr(node, "end_lineno", start) or start
        spans.append((node.name, start, max(start, end)))
    return spans


def _load_fail_baseline(path: pathlib.Path) -> tuple[set[pathlib.Path], set[tuple[pathlib.Path, str]]]:
    if not path.exists():
        return set(), set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return set(), set()
    ignored = payload.get("ignored_failures") if isinstance(payload, dict) else None
    if not isinstance(ignored, dict):
        return set(), set()

    cwd = pathlib.Path.cwd().resolve()
    ignored_files: set[pathlib.Path] = set()
    for item in ignored.get("file_lines", []):
        if not isinstance(item, str) or not item.strip():
            continue
        candidate = pathlib.Path(item.strip())
        if not candidate.is_absolute():
            candidate = cwd / candidate
        ignored_files.add(candidate.resolve())

    ignored_functions: set[tuple[pathlib.Path, str]] = set()
    for item in ignored.get("function_lines", []):
        if not isinstance(item, dict):
            continue
        raw_path = item.get("path")
        raw_name = item.get("name")
        if not isinstance(raw_path, str) or not raw_path.strip():
            continue
        if not isinstance(raw_name, str) or not raw_name.strip():
            continue
        candidate = pathlib.Path(raw_path.strip())
        if not candidate.is_absolute():
            candidate = cwd / candidate
        ignored_functions.add((candidate.resolve(), raw_name.strip()))

    return ignored_files, ignored_functions


def main() -> int:
    parser = argparse.ArgumentParser(description="Check Python code shape thresholds")
    parser.add_argument("--root", default="src/device_hub", help="Directory to scan")
    parser.add_argument("--include-glob", default="*.py")
    parser.add_argument(
        "--exclude-glob",
        action="append",
        default=list(DEFAULT_EXCLUDES),
        help="Glob pattern to exclude (can repeat)",
    )
    parser.add_argument("--warn-file-lines", type=int, default=300)
    parser.add_argument("--fail-file-lines", type=int, default=500)
    parser.add_argument("--warn-function-lines", type=int, default=40)
    parser.add_argument("--fail-function-lines", type=int, default=80)
    parser.add_argument(
        "--fail-baseline",
        default=DEFAULT_FAIL_BASELINE_PATH,
        help="Optional JSON baseline listing known fail findings to ignore",
    )
    args = parser.parse_args()

    root = pathlib.Path(args.root).resolve()
    if not root.exists():
        print(f"path not found: {root}", file=sys.stderr)
        return 2

    findings: list[Finding] = []
    ignored_file_failures, ignored_function_failures = _load_fail_baseline(
        pathlib.Path(args.fail_baseline).resolve()
    )
    files = _iter_python_files(
        root,
        include_glob=args.include_glob,
        exclude_globs=tuple(args.exclude_glob),
    )

    for path in files:
        line_count = _file_line_count(path)
        if line_count > args.fail_file_lines:
            if path.resolve() in ignored_file_failures:
                continue
            findings.append(
                Finding(
                    level="FAIL",
                    kind="file_lines",
                    path=path,
                    lineno=1,
                    metric=line_count,
                    threshold=args.fail_file_lines,
                )
            )
        elif line_count > args.warn_file_lines:
            findings.append(
                Finding(
                    level="WARN",
                    kind="file_lines",
                    path=path,
                    lineno=1,
                    metric=line_count,
                    threshold=args.warn_file_lines,
                )
            )

        for fn_name, start, end in _function_spans(path):
            fn_lines = end - start + 1
            if fn_lines > args.fail_function_lines:
                if (path.resolve(), fn_name) in ignored_function_failures:
                    continue
                findings.append(
                    Finding(
                        level="FAIL",
                        kind="function_lines",
                        path=path,
                        lineno=start,
                        metric=fn_lines,
                        threshold=args.fail_function_lines,
                        name=fn_name,
                    )
                )
            elif fn_lines > args.warn_function_lines:
                findings.append(
                    Finding(
                        level="WARN",
                        kind="function_lines",
                        path=path,
                        lineno=start,
                        metric=fn_lines,
                        threshold=args.warn_function_lines,
                        name=fn_name,
                    )
                )

    fail_count = 0
    warn_count = 0
    for item in findings:
        if item.level == "FAIL":
            fail_count += 1
        else:
            warn_count += 1
        print(item.format())

    print(
        f"code-shape summary: files={len(files)} warnings={warn_count} failures={fail_count}"
    )
    return 1 if fail_count > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
