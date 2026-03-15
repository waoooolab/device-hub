"""Helpers for stable snake_case reason/failure code semantics."""

from __future__ import annotations

import re
from typing import Any

_CODE_TERM_PATTERN = re.compile(r"^[a-z0-9_]+$")
_CAMEL_BOUNDARY_PATTERN = re.compile(r"([a-z0-9])([A-Z])")
_NON_ALNUM_PATTERN = re.compile(r"[^A-Za-z0-9]+")
_MULTI_UNDERSCORE_PATTERN = re.compile(r"_+")


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
