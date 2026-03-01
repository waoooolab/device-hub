"""FastAPI boundary for device-hub service."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException

from device_hub.devices.heartbeat import refresh_presence
from device_hub.service import DeviceHubService

from .auth import require_claims
from .contracts import (
    ContractValidationError,
    validate_command_envelope_contract,
    validate_event_envelope_contract,
    validate_runtime_device_status,
    validate_token_claims_contract,
)

SERVICE_AUDIENCE = "device-hub"
DEVICES_WRITE_SCOPE = "devices:write"
DEVICES_READ_SCOPE = "devices:read"


app = FastAPI(title="device-hub", version="0.1.0")
_hub = DeviceHubService()


def _extract_payload(envelope: dict[str, Any], *, required_fields: list[str]) -> dict[str, Any]:
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="payload must be object")

    missing = [f for f in required_fields if f not in payload]
    if missing:
        raise HTTPException(status_code=422, detail=f"payload missing fields: {', '.join(missing)}")
    return payload


def _build_event(envelope: dict[str, Any], *, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
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


def _validate_write(envelope: dict[str, Any], claims: dict[str, Any]) -> None:
    try:
        validate_token_claims_contract(claims)
        validate_command_envelope_contract(envelope)
    except ContractValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _validate_read(claims: dict[str, Any]) -> None:
    try:
        validate_token_claims_contract(claims)
    except ContractValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _finalize_event(event: dict[str, Any]) -> dict[str, Any]:
    try:
        validate_event_envelope_contract(event)
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"invalid device event: {exc}") from exc
    return event


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_AUDIENCE}


@app.post("/v1/devices/register")
def register_device(
    envelope: dict[str, Any],
    claims: dict[str, Any] = Depends(
        require_claims(audience=SERVICE_AUDIENCE, required_scope=DEVICES_WRITE_SCOPE)
    ),
) -> dict[str, Any]:
    _validate_write(envelope, claims)
    payload = _extract_payload(envelope, required_fields=["device_id", "capabilities"])

    device_id = payload.get("device_id")
    capabilities = payload.get("capabilities")
    if not isinstance(device_id, str) or not device_id:
        raise HTTPException(status_code=422, detail="payload.device_id must be non-empty string")
    if not isinstance(capabilities, list) or any(not isinstance(c, str) or not c for c in capabilities):
        raise HTTPException(status_code=422, detail="payload.capabilities must be non-empty string list")

    rec = _hub.register_device(device_id, capabilities)
    try:
        validate_runtime_device_status(rec.status)
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"invalid device status: {exc}") from exc

    event = _build_event(
        envelope,
        event_type="device.registered",
        payload={
            "device_id": rec.device_id,
            "status": rec.status,
            "paired": rec.paired,
            "capabilities": rec.capabilities,
        },
    )
    return _finalize_event(event)


@app.post("/v1/devices/pairing/request")
def request_pairing(
    envelope: dict[str, Any],
    claims: dict[str, Any] = Depends(
        require_claims(audience=SERVICE_AUDIENCE, required_scope=DEVICES_WRITE_SCOPE)
    ),
) -> dict[str, Any]:
    _validate_write(envelope, claims)
    payload = _extract_payload(envelope, required_fields=["device_id"])

    device_id = payload.get("device_id")
    ttl_seconds = payload.get("ttl_seconds", 300)
    if not isinstance(device_id, str) or not device_id:
        raise HTTPException(status_code=422, detail="payload.device_id must be non-empty string")
    if not isinstance(ttl_seconds, int) or ttl_seconds < 30 or ttl_seconds > 3600:
        raise HTTPException(status_code=422, detail="payload.ttl_seconds must be integer in [30, 3600]")

    try:
        req = _hub.request_pairing(device_id, ttl_seconds=ttl_seconds)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    event = _build_event(
        envelope,
        event_type="device.pairing.requested",
        payload={
            "device_id": req.device_id,
            "code": req.code,
            "expires_at": req.expires_at,
        },
    )
    return _finalize_event(event)


@app.post("/v1/devices/pairing/approve")
def approve_pairing(
    envelope: dict[str, Any],
    claims: dict[str, Any] = Depends(
        require_claims(audience=SERVICE_AUDIENCE, required_scope=DEVICES_WRITE_SCOPE)
    ),
) -> dict[str, Any]:
    _validate_write(envelope, claims)
    payload = _extract_payload(envelope, required_fields=["code"])

    code = payload.get("code")
    if not isinstance(code, str) or not code:
        raise HTTPException(status_code=422, detail="payload.code must be non-empty string")

    try:
        rec = _hub.approve_pairing(code)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    try:
        validate_runtime_device_status(rec.status)
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"invalid device status: {exc}") from exc

    event = _build_event(
        envelope,
        event_type="device.pairing.approved",
        payload={
            "device_id": rec.device_id,
            "status": rec.status,
            "paired": rec.paired,
        },
    )
    return _finalize_event(event)


@app.post("/v1/devices/heartbeat")
def ingest_heartbeat(
    envelope: dict[str, Any],
    claims: dict[str, Any] = Depends(
        require_claims(audience=SERVICE_AUDIENCE, required_scope=DEVICES_WRITE_SCOPE)
    ),
) -> dict[str, Any]:
    _validate_write(envelope, claims)
    payload = _extract_payload(envelope, required_fields=["device_id"])

    device_id = payload.get("device_id")
    if not isinstance(device_id, str) or not device_id:
        raise HTTPException(status_code=422, detail="payload.device_id must be non-empty string")

    try:
        rec = _hub.receive_heartbeat(device_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"device not found: {device_id}") from exc

    try:
        validate_runtime_device_status(rec.status)
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"invalid device status: {exc}") from exc

    event = _build_event(
        envelope,
        event_type="device.heartbeat.ingested",
        payload={
            "device_id": rec.device_id,
            "status": rec.status,
            "last_seen_at": rec.last_seen_at,
        },
    )
    return _finalize_event(event)


@app.post("/v1/devices/presence/refresh")
def refresh_device_presence(
    envelope: dict[str, Any],
    claims: dict[str, Any] = Depends(
        require_claims(audience=SERVICE_AUDIENCE, required_scope=DEVICES_WRITE_SCOPE)
    ),
) -> dict[str, Any]:
    _validate_write(envelope, claims)
    payload = _extract_payload(envelope, required_fields=["timeout_seconds"])

    timeout_seconds = payload.get("timeout_seconds")
    if not isinstance(timeout_seconds, int) or timeout_seconds < 1:
        raise HTTPException(status_code=422, detail="payload.timeout_seconds must be integer >= 1")

    before_offline = {
        did
        for did, rec in _hub.registry.devices.items()
        if rec.status == "offline"
    }
    refresh_presence(_hub.registry, timeout_seconds=timeout_seconds)
    after_offline = {
        did
        for did, rec in _hub.registry.devices.items()
        if rec.status == "offline"
    }
    changed_to_offline = sorted(after_offline - before_offline)

    for did in changed_to_offline:
        rec = _hub.registry.devices.get(did)
        if rec is not None:
            validate_runtime_device_status(rec.status)

    event = _build_event(
        envelope,
        event_type="device.presence.refreshed",
        payload={
            "timeout_seconds": timeout_seconds,
            "updated_to_offline": changed_to_offline,
            "updated_count": len(changed_to_offline),
        },
    )
    return _finalize_event(event)


@app.post("/v1/devices/route")
def route_command(
    envelope: dict[str, Any],
    claims: dict[str, Any] = Depends(
        require_claims(audience=SERVICE_AUDIENCE, required_scope=DEVICES_WRITE_SCOPE)
    ),
) -> dict[str, Any]:
    _validate_write(envelope, claims)
    payload = _extract_payload(
        envelope,
        required_fields=["capability", "command_type", "command_payload"],
    )

    capability = payload.get("capability")
    command_type = payload.get("command_type")
    command_payload = payload.get("command_payload")
    load_by_device = payload.get("load_by_device")

    if not isinstance(capability, str) or not capability:
        raise HTTPException(status_code=422, detail="payload.capability must be non-empty string")
    if not isinstance(command_type, str) or not command_type:
        raise HTTPException(status_code=422, detail="payload.command_type must be non-empty string")
    if not isinstance(command_payload, dict):
        raise HTTPException(status_code=422, detail="payload.command_payload must be object")
    if load_by_device is not None and (
        not isinstance(load_by_device, dict)
        or any(not isinstance(k, str) or not isinstance(v, int) for k, v in load_by_device.items())
    ):
        raise HTTPException(status_code=422, detail="payload.load_by_device must be {device_id: int}")

    routed = _hub.route_command(
        capability=capability,
        command_type=command_type,
        payload=command_payload,
        trace_id=str(envelope["trace_id"]),
        load_by_device=load_by_device,
    )
    if routed is None:
        event = _build_event(
            envelope,
            event_type="device.route.miss",
            payload={"capability": capability, "reason": "no_eligible_device"},
        )
        return _finalize_event(event)

    event = _build_event(
        envelope,
        event_type="device.route.selected",
        payload={"route": routed},
    )
    return _finalize_event(event)


@app.get("/v1/devices/{device_id}")
def get_device(
    device_id: str,
    claims: dict[str, Any] = Depends(
        require_claims(audience=SERVICE_AUDIENCE, required_scope=DEVICES_READ_SCOPE)
    ),
) -> dict[str, Any]:
    _validate_read(claims)

    rec = _hub.registry.devices.get(device_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"device not found: {device_id}")

    try:
        validate_runtime_device_status(rec.status)
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"invalid device status: {exc}") from exc

    tenant_id = str(claims.get("tenant_id", "unknown"))
    app_id = str(claims.get("app_id", "unknown"))
    session_key = str(claims.get("session_key", "tenant:unknown:app:unknown:channel:internal:actor:system:thread:default:agent:device-hub"))
    trace_id = str(claims.get("trace_id", "trace-missing"))

    event = {
        "event_id": str(uuid4()),
        "event_type": "device.status",
        "tenant_id": tenant_id,
        "app_id": app_id,
        "session_key": session_key,
        "trace_id": trace_id,
        "correlation_id": f"device:{device_id}",
        "ts": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "device_id": rec.device_id,
            "status": rec.status,
            "paired": rec.paired,
            "capabilities": rec.capabilities,
            "last_seen_at": rec.last_seen_at,
        },
    }
    return _finalize_event(event)
