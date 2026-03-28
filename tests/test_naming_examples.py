from __future__ import annotations

from pathlib import Path
import re


FORBIDDEN_PATTERNS = [
    re.compile(r"\bwaoooo(?:-shell)?\b"),
]


def test_platform_name_is_not_used_as_app_id_in_test_examples() -> None:
    tests_dir = Path(__file__).resolve().parent
    python_files = sorted(tests_dir.glob("test_*.py"))

    violations: list[str] = []
    for file_path in python_files:
        if file_path.name == "test_naming_examples.py":
            continue
        content = file_path.read_text(encoding="utf-8")
        for pattern in FORBIDDEN_PATTERNS:
            if pattern.search(content):
                violations.append(f"{file_path.name}: {pattern.pattern}")

    assert violations == []
