#!/usr/bin/env python3
"""Boundary import guard.

Disallow direct imports of sibling platform service internals from `src/`.

Examples blocked:
- `from runtime_execution.service import RuntimeExecutionService`
- `import device_hub.service_api.app`

Examples allowed:
- importing own package internals
- importing sibling package root only when explicitly allow-listed
"""

from __future__ import annotations

import argparse
import ast
import pathlib
import sys
from dataclasses import dataclass

SERVICE_PACKAGES = {
    "runtime_gateway",
    "runtime_execution",
    "device_hub",
    "langgraph_core",
    "ai_gateway",
}


@dataclass(frozen=True)
class Violation:
    path: pathlib.Path
    lineno: int
    message: str


def _iter_python_files(src_root: pathlib.Path) -> list[pathlib.Path]:
    return sorted(
        p for p in src_root.rglob("*.py") if "__pycache__" not in p.parts and ".venv" not in p.parts
    )


def _check_file(
    path: pathlib.Path,
    *,
    own_package: str,
    allow_public: set[str],
) -> list[Violation]:
    text = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(text, filename=str(path))
    except SyntaxError as exc:
        return [Violation(path=path, lineno=exc.lineno or 1, message=f"syntax error: {exc.msg}")]

    violations: list[Violation] = []

    def record(lineno: int, module: str) -> None:
        violations.append(
            Violation(
                path=path,
                lineno=lineno,
                message=f"disallowed cross-service internal import: {module}",
            )
        )

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                module = alias.name
                root = module.split(".", 1)[0]
                if root not in SERVICE_PACKAGES or root == own_package:
                    continue

                # Root-only import may be an explicit public dependency.
                if "." not in module:
                    if root in allow_public:
                        continue
                    record(node.lineno, module)
                    continue

                # Nested module import of sibling service is always blocked.
                record(node.lineno, module)

        elif isinstance(node, ast.ImportFrom):
            if node.level and node.level > 0:
                continue
            if not node.module:
                continue
            module = node.module
            root = module.split(".", 1)[0]
            if root not in SERVICE_PACKAGES or root == own_package:
                continue

            # from sibling_root import X -> public import, allow if in allow_public
            if "." not in module:
                if root in allow_public:
                    continue
                record(node.lineno, module)
                continue

            # from sibling_root.something import X -> private/internal coupling
            record(node.lineno, module)

    return violations


def main() -> int:
    parser = argparse.ArgumentParser(description="Check boundary import violations")
    parser.add_argument("--own-package", required=True, help="Top-level package name of current repo")
    parser.add_argument(
        "--allow-public",
        action="append",
        default=[],
        help="Allow root-level import from sibling package (can be repeated)",
    )
    parser.add_argument("--src", default="src", help="Source root path")
    args = parser.parse_args()

    src_root = pathlib.Path(args.src).resolve()
    if not src_root.exists():
        print(f"src path not found: {src_root}", file=sys.stderr)
        return 2

    allow_public = set(args.allow_public)
    violations: list[Violation] = []
    for py_file in _iter_python_files(src_root):
        violations.extend(
            _check_file(
                py_file,
                own_package=args.own_package,
                allow_public=allow_public,
            )
        )

    if not violations:
        print("boundary import check: OK")
        return 0

    print("boundary import check: FAILED")
    for item in violations:
        print(f"{item.path}:{item.lineno}: {item.message}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
