"""FastAPI boundary for device-hub service."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException

from device_hub.service import DeviceHubService

from .auth import require_claims
from .contracts import (
    ContractValidationError,
    validate_runtime_device_status,
)
from .placements import (
    allocate_placement_response,
    expire_placement_response,
    release_placement_response,
)
from .routing import refresh_presence_response, route_command_response
from .support import (
    build_event as _build_event,
    extract_payload as _extract_payload,
    finalize_event as _finalize_event,
    validate_read as _validate_read,
    validate_write as _validate_write,
)

SERVICE_AUDIENCE = "device-hub"
DEVICES_WRITE_SCOPE = "devices:write"
DEVICES_READ_SCOPE = "devices:read"


app = FastAPI(title="device-hub", version="0.1.0")
_hub = DeviceHubService()


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
    return refresh_presence_response(envelope=envelope, claims=claims, hub=_hub)


@app.post("/v1/devices/route")
def route_command(
    envelope: dict[str, Any],
    claims: dict[str, Any] = Depends(
        require_claims(audience=SERVICE_AUDIENCE, required_scope=DEVICES_WRITE_SCOPE)
    ),
) -> dict[str, Any]:
    return route_command_response(envelope=envelope, claims=claims, hub=_hub)


@app.post("/v1/placements/allocate")
def allocate_placement(
    envelope: dict[str, Any],
    claims: dict[str, Any] = Depends(
        require_claims(audience=SERVICE_AUDIENCE, required_scope=DEVICES_WRITE_SCOPE)
    ),
) -> dict[str, Any]:
    return allocate_placement_response(envelope=envelope, claims=claims, hub=_hub)


@app.post("/v1/placements/release")
def release_placement(
    envelope: dict[str, Any],
    claims: dict[str, Any] = Depends(
        require_claims(audience=SERVICE_AUDIENCE, required_scope=DEVICES_WRITE_SCOPE)
    ),
) -> dict[str, Any]:
    return release_placement_response(envelope=envelope, claims=claims, hub=_hub)


@app.post("/v1/placements/expire")
def expire_placement(
    envelope: dict[str, Any],
    claims: dict[str, Any] = Depends(
        require_claims(audience=SERVICE_AUDIENCE, required_scope=DEVICES_WRITE_SCOPE)
    ),
) -> dict[str, Any]:
    return expire_placement_response(envelope=envelope, claims=claims, hub=_hub)


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
