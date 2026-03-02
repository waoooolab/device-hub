"""Shared helpers for device-hub API handlers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import HTTPException

from .contracts import (
    ContractValidationError,
    validate_command_envelope_contract,
    validate_event_envelope_contract,
    validate_token_claims_contract,
)


def extract_payload(envelope: dict[str, Any], *, required_fields: list[str]) -> dict[str, Any]:
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="payload must be object")

    missing = [field for field in required_fields if field not in payload]
    if missing:
        raise HTTPException(status_code=422, detail=f"payload missing fields: {', '.join(missing)}")
    return payload


def build_event(envelope: dict[str, Any], *, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "tenant_id": str(envelope["tenant_id"]),
        "app_id": str(envelope["app_id"]),
        "session_key": str(envelope["session_key"]),
        "trace_id": str(envelope["trace_id"]),
        "correlation_id": str(envelope.get("correlation_id") or envelope["command_id"]),
        "ts": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


def validate_write(envelope: dict[str, Any], claims: dict[str, Any]) -> None:
    try:
        validate_token_claims_contract(claims)
        validate_command_envelope_contract(envelope)
    except ContractValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def validate_read(claims: dict[str, Any]) -> None:
    try:
        validate_token_claims_contract(claims)
    except ContractValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def finalize_event(event: dict[str, Any]) -> dict[str, Any]:
    try:
        validate_event_envelope_contract(event)
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"invalid device event: {exc}") from exc
    return event


def resolve_placement_capability(payload: dict[str, Any]) -> str:
    explicit = payload.get("capability")
    if isinstance(explicit, str) and explicit:
        return explicit

    profile = payload.get("execution_profile")
    if isinstance(profile, dict):
        constraints = profile.get("placement_constraints")
        if isinstance(constraints, dict):
            required = constraints.get("required_capabilities")
            if isinstance(required, list):
                for capability in required:
                    if isinstance(capability, str) and capability:
                        return capability
    return "compute.comfyui.local"
