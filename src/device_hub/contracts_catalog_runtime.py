"""Shared helpers for runtime catalog path resolution."""

from __future__ import annotations

import os
from pathlib import Path

PLATFORM_CONTRACTS_DIR_ENV = "OWA_PLATFORM_CONTRACTS_DIR"
PLATFORM_CONTRACTS_DIR_ENV_LEGACY = "WAOOOOLAB_PLATFORM_CONTRACTS_DIR"


def normalized_env(name: str) -> str | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def resolve_platform_contracts_root(*, anchor_file: str) -> Path:
    configured = os.environ.get(PLATFORM_CONTRACTS_DIR_ENV)
    if configured is None:
        configured = os.environ.get(PLATFORM_CONTRACTS_DIR_ENV_LEGACY)
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(anchor_file).resolve().parents[3] / "platform-contracts"


def resolve_catalog_data_path(*, anchor_file: str, relative_path: str) -> Path:
    root = resolve_platform_contracts_root(anchor_file=anchor_file)
    if (root / "jsonschema").exists():
        return root / relative_path
    if root.name == "jsonschema":
        return root.parent / relative_path
    return root / relative_path
