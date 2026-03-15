from __future__ import annotations

from pathlib import Path

import pytest

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "contracts"
CANONICAL_ROOT = Path(__file__).resolve().parents[2] / "platform-contracts" / "jsonschema"


def _fixture_rel_paths() -> list[str]:
    files = sorted(FIXTURE_ROOT.rglob("*.json"))
    return [str(path.relative_to(FIXTURE_ROOT).as_posix()) for path in files]


@pytest.mark.parametrize("relative_path", _fixture_rel_paths())
def test_contract_fixture_matches_platform_contracts(relative_path: str) -> None:
    fixture_path = FIXTURE_ROOT / relative_path
    canonical_path = CANONICAL_ROOT / relative_path

    if not canonical_path.exists():
        pytest.skip(f"canonical contract missing: {canonical_path}")

    assert fixture_path.read_text(encoding="utf-8") == canonical_path.read_text(encoding="utf-8")
