"""platform-contracts validators for device-hub boundary."""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


class ContractValidationError(ValueError):
    """Raised when payload violates canonical contract."""


def _candidate_contract_roots() -> list[Path]:
    explicit = os.environ.get("WAOOOOLAB_PLATFORM_CONTRACTS_DIR")
    roots: list[Path] = []
    if explicit:
        roots.append(Path(explicit))

    here = Path(__file__).resolve()
    roots.append(here.parents[4] / "platform-contracts" / "jsonschema")
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
        f"{schema_relative_path}; set WAOOOOLAB_PLATFORM_CONTRACTS_DIR"
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
