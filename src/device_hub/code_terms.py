"""Helpers for stable snake_case reason/failure code semantics."""

from __future__ import annotations

from functools import lru_cache
import json
from .contracts_catalog_runtime import resolve_catalog_data_path
from pathlib import Path
import re
from typing import Any

_DEFAULT_POLICY = {
    "snake_case_pattern": r"^[a-z0-9_]+$",
    "camel_boundary_pattern": r"([a-z0-9])([A-Z])",
    "non_alnum_pattern": r"[^A-Za-z0-9]+",
    "multi_underscore_pattern": r"_+",
}
_CODE_TERMS_POLICY_DATA_PATH = "catalog/runtime/code-terms.data.v1.json"

def _policy_data_path() -> Path:
    return resolve_catalog_data_path(
        anchor_file=__file__,
        relative_path=_CODE_TERMS_POLICY_DATA_PATH,
    )


@lru_cache(maxsize=1)
def _load_policy() -> dict[str, str]:
    path = _policy_data_path()
    if not path.exists():
        return dict(_DEFAULT_POLICY)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(_DEFAULT_POLICY)
    if not isinstance(payload, dict):
        return dict(_DEFAULT_POLICY)
    raw_policy = payload.get("policy")
    if not isinstance(raw_policy, dict):
        return dict(_DEFAULT_POLICY)
    policy = dict(_DEFAULT_POLICY)
    for key, fallback in _DEFAULT_POLICY.items():
        raw_value = raw_policy.get(key)
        if isinstance(raw_value, str) and raw_value.strip():
            policy[key] = raw_value
        else:
            policy[key] = fallback
    return policy


def _compile_pattern(pattern: str, *, fallback: str) -> re.Pattern[str]:
    try:
        return re.compile(pattern)
    except re.error:
        return re.compile(fallback)


_POLICY = _load_policy()
_CODE_TERM_PATTERN = _compile_pattern(
    _POLICY["snake_case_pattern"],
    fallback=_DEFAULT_POLICY["snake_case_pattern"],
)
_CAMEL_BOUNDARY_PATTERN = _compile_pattern(
    _POLICY["camel_boundary_pattern"],
    fallback=_DEFAULT_POLICY["camel_boundary_pattern"],
)
_NON_ALNUM_PATTERN = _compile_pattern(
    _POLICY["non_alnum_pattern"],
    fallback=_DEFAULT_POLICY["non_alnum_pattern"],
)
_MULTI_UNDERSCORE_PATTERN = _compile_pattern(
    _POLICY["multi_underscore_pattern"],
    fallback=_DEFAULT_POLICY["multi_underscore_pattern"],
)


def normalize_optional_code_term(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if _CODE_TERM_PATTERN.fullmatch(candidate):
        return candidate
    candidate = _CAMEL_BOUNDARY_PATTERN.sub(r"\1_\2", candidate)
    candidate = _NON_ALNUM_PATTERN.sub("_", candidate)
    candidate = _MULTI_UNDERSCORE_PATTERN.sub("_", candidate)
    candidate = candidate.strip("_").lower()
    if _CODE_TERM_PATTERN.fullmatch(candidate):
        return candidate
    return None
