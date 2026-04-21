"""platform-contracts validators for device-hub boundary."""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from ..contracts_catalog_runtime import (
    PLATFORM_CONTRACTS_DIR_ENV,
    PLATFORM_CONTRACTS_DIR_ENV_LEGACY,
    resolve_platform_contracts_root,
)


class ContractValidationError(ValueError):
    """Raised when payload violates canonical contract."""


def _candidate_contract_roots() -> list[Path]:
    explicit = os.environ.get(PLATFORM_CONTRACTS_DIR_ENV)
    if explicit is None:
        explicit = os.environ.get(PLATFORM_CONTRACTS_DIR_ENV_LEGACY)
    roots: list[Path] = []
    if explicit:
        explicit_path = Path(explicit).expanduser()
        if explicit_path.name == "jsonschema":
            roots.append(explicit_path)
        else:
            # Support direct fixture roots that already contain schema files.
            roots.append(explicit_path)
            roots.append(explicit_path / "jsonschema")

    package_anchor = Path(__file__).resolve().parents[1] / "__init__.py"
    roots.append(
        resolve_platform_contracts_root(anchor_file=str(package_anchor)) / "jsonschema"
    )
    roots.append(Path.cwd().parent / "platform-contracts" / "jsonschema")
    return roots


@lru_cache(maxsize=12)
def _load_schema(schema_relative_path: str) -> dict[str, Any]:
    for root in _candidate_contract_roots():
        path = root / schema_relative_path
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    raise ContractValidationError(
        "platform-contracts schema not found for "
        f"{schema_relative_path}; set {PLATFORM_CONTRACTS_DIR_ENV} "
        f"(legacy alias: {PLATFORM_CONTRACTS_DIR_ENV_LEGACY})"
    )


def _validate(schema_relative_path: str, payload: dict[str, Any]) -> None:
    schema = _load_schema(schema_relative_path)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(validator.iter_errors(payload), key=lambda err: list(err.path))
    if not errors:
        return

    first = errors[0]
    path = ".".join(str(x) for x in first.path)
    detail = f"{first.message} (at {path})" if path else first.message
    raise ContractValidationError(detail)


def validate_command_envelope_contract(payload: dict[str, Any]) -> None:
    _validate("command-envelope.v1.json", payload)


def validate_event_envelope_contract(payload: dict[str, Any]) -> None:
    _validate("event-envelope.v1.json", payload)


def validate_token_claims_contract(payload: dict[str, Any]) -> None:
    _validate("auth/token-claims.v1.json", payload)


def validate_runtime_device_status(status: str) -> None:
    _validate(
        "runtime/runtime-state.v1.json",
        {
            "run_status": "queued",
            "task_status": "pending",
            "device_status": status,
        },
    )


def validate_execution_profile_contract(payload: dict[str, Any]) -> None:
    _validate("runtime/execution-profile.v1.json", payload)


def validate_device_route_event_contract(payload: dict[str, Any]) -> None:
    _validate("runtime/device-route-event.v1.json", payload)
