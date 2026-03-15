from __future__ import annotations

from device_hub.code_terms import normalize_optional_code_term


def test_normalize_optional_code_term_accepts_snake_case() -> None:
    assert normalize_optional_code_term("dispatch_failed") == "dispatch_failed"


def test_normalize_optional_code_term_normalizes_mixed_case() -> None:
    assert normalize_optional_code_term("DispatchFailed") == "dispatch_failed"
    assert normalize_optional_code_term("dispatch-failed") == "dispatch_failed"
    assert normalize_optional_code_term("dispatch.failed") == "dispatch_failed"


def test_normalize_optional_code_term_rejects_empty() -> None:
    assert normalize_optional_code_term("") is None
    assert normalize_optional_code_term("   ") is None
