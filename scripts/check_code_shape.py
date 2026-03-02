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
    args = parser.parse_args()

    root = pathlib.Path(args.root).resolve()
    if not root.exists():
        print(f"path not found: {root}", file=sys.stderr)
        return 2

    findings: list[Finding] = []
    files = _iter_python_files(
        root,
        include_glob=args.include_glob,
        exclude_globs=tuple(args.exclude_glob),
    )

    for path in files:
        line_count = _file_line_count(path)
        if line_count > args.fail_file_lines:
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

