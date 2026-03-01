#!/usr/bin/env python3
"""Run repository test functions without external test framework dependency."""

from __future__ import annotations

import importlib.util
import inspect
import pathlib
import sys
import traceback


def main() -> int:
    root = pathlib.Path(__file__).resolve().parents[1]
    src_dir = root / "src"
    if src_dir.exists():
        sys.path.insert(0, str(src_dir))

    tests_dir = root / "tests"
    test_files = sorted(tests_dir.glob("test_*.py"))
    ran = 0
    failed: list[str] = []
    loaded: list[str] = []

    for test_file in test_files:
        module_name = f"_local_test_{test_file.stem}"
        spec = importlib.util.spec_from_file_location(module_name, test_file)
        if spec is None or spec.loader is None:
            failed.append(f"{test_file.name}:cannot load module spec")
            continue
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        loaded.append(test_file.name)
        for name, fn in sorted(inspect.getmembers(module, inspect.isfunction)):
            if not name.startswith("test_"):
                continue
            ran += 1
            try:
                fn()
            except Exception:
                failed.append(f"{test_file.name}:{name}")
                traceback.print_exc()

    print(f"modules={loaded}")
    print(f"ran={ran} failed={len(failed)}")
    if failed:
        print("failed_tests=")
        for item in failed:
            print(item)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
