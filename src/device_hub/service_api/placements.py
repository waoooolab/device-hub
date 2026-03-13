"""Placement lifecycle handlers for device-hub API."""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from device_hub.service import DeviceHubService

from .contracts import (
    ContractValidationError,
    validate_device_route_event_contract,
    validate_execution_profile_contract,
)
from .support import (
    build_event,
    extract_payload,
    finalize_event,
    resolve_placement_capability,
    validate_write,
)


def _validate_load_by_device(load_by_device: Any) -> None:
    if load_by_device is None:
        return
    if not isinstance(load_by_device, dict) or any(
        not isinstance(key, str) or not isinstance(value, int) for key, value in load_by_device.items()
    ):
        raise HTTPException(status_code=422, detail="payload.load_by_device must be {device_id: int}")


def _validate_route_event(event: dict[str, Any], *, error_message: str) -> None:
    try:
        validate_device_route_event_contract(event)
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"{error_message}: {exc}") from exc


def _allocation_decision_payload(decision: dict[str, Any], capability: str) -> dict[str, Any]:
    outcome = str(decision["outcome"])
    capability_match = list(decision.get("capability_match", [capability]))
    if outcome == "lease_acquired":
        payload: dict[str, Any] = {
            "outcome": outcome,
            "device_id": str(decision["device_id"]),
            "lease_id": str(decision["lease_id"]),
            "lease_expires_at": str(decision["lease_expires_at"]),
            "capability_match": capability_match,
        }
        score = decision.get("score")
        if isinstance(score, (int, float)) and not isinstance(score, bool):
            payload["score"] = float(score)
        resource_snapshot = decision.get("resource_snapshot")
        if isinstance(resource_snapshot, dict):
            queue_depth = resource_snapshot.get("queue_depth")
            if isinstance(queue_depth, int):
                payload["resource_snapshot"] = {"queue_depth": queue_depth}
        reason_code = decision.get("reason_code")
        if isinstance(reason_code, str) and reason_code.strip():
            payload["reason_code"] = reason_code
        reason = decision.get("reason")
        if isinstance(reason, str) and reason.strip():
            payload["reason"] = reason
        return payload
    payload = {
        "outcome": outcome,
        "reason_code": str(decision["reason_code"]),
        "reason": str(decision["reason"]),
        "capability_match": capability_match,
    }
    resource_snapshot = decision.get("resource_snapshot")
    if isinstance(resource_snapshot, dict):
        queue_depth = resource_snapshot.get("queue_depth")
        if isinstance(queue_depth, int):
            payload["resource_snapshot"] = {"queue_depth": queue_depth}
    return payload


def _placement_request(
    payload: dict[str, Any],
) -> tuple[str, str, dict[str, Any], dict[str, int] | None, int, str | None]:
    run_id = payload.get("run_id")
    task_id = payload.get("task_id", f"{run_id}:root")
    execution_profile = payload.get("execution_profile")
    load_by_device = payload.get("load_by_device")
    lease_ttl_seconds = payload.get("lease_ttl_seconds", 300)
    if not isinstance(run_id, str) or not run_id:
        raise HTTPException(status_code=422, detail="payload.run_id must be non-empty string")
    if not isinstance(task_id, str) or not task_id:
        raise HTTPException(status_code=422, detail="payload.task_id must be non-empty string")
    if not isinstance(execution_profile, dict):
        raise HTTPException(status_code=422, detail="payload.execution_profile must be object")
    _validate_load_by_device(load_by_device)
    if not isinstance(lease_ttl_seconds, int) or lease_ttl_seconds < 30 or lease_ttl_seconds > 3600:
        raise HTTPException(status_code=422, detail="payload.lease_ttl_seconds must be integer in [30, 3600]")
    tenant_id: str | None = None
    constraints = execution_profile.get("placement_constraints")
    if isinstance(constraints, dict):
        raw_tenant_id = constraints.get("tenant_id")
        if isinstance(raw_tenant_id, str):
            normalized = raw_tenant_id.strip()
            tenant_id = normalized or None
    return run_id, task_id, execution_profile, load_by_device, lease_ttl_seconds, tenant_id


def _validate_execution_profile(profile: dict[str, Any]) -> None:
    try:
        validate_execution_profile_contract(profile)
    except ContractValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _allocation_event(
    envelope: dict[str, Any],
    *,
    run_id: str,
    task_id: str,
    capability: str,
    decision: dict[str, Any],
) -> dict[str, Any]:
    is_success = decision.get("outcome") == "lease_acquired"
    return build_event(
        envelope,
        event_type="device.lease.acquired" if is_success else "device.route.rejected",
        payload={
            "run_id": run_id,
            "task_id": task_id,
            "placement_request_id": str(envelope.get("correlation_id") or envelope["command_id"]),
            "decision": _allocation_decision_payload(decision, capability),
        },
    )


def allocate_placement_response(
    *,
    envelope: dict[str, Any],
    claims: dict[str, Any],
    hub: DeviceHubService,
) -> dict[str, Any]:
    validate_write(envelope, claims)
    payload = extract_payload(envelope, required_fields=["run_id", "execution_profile"])
    run_id, task_id, execution_profile, load_by_device, lease_ttl_seconds, tenant_id = _placement_request(payload)
    if tenant_id is None:
        raw_envelope_tenant_id = envelope.get("tenant_id")
        if isinstance(raw_envelope_tenant_id, str):
            normalized = raw_envelope_tenant_id.strip()
            tenant_id = normalized or None
    _validate_execution_profile(execution_profile)
    capability = resolve_placement_capability(payload)
    decision = hub.allocate_placement(
        run_id=run_id,
        task_id=task_id,
        capability=capability,
        trace_id=str(envelope["trace_id"]),
        load_by_device=load_by_device,
        lease_ttl_seconds=lease_ttl_seconds,
        tenant_id=tenant_id,
        placement_constraints=execution_profile.get("placement_constraints")
        if isinstance(execution_profile.get("placement_constraints"), dict)
        else None,
    )
    event = _allocation_event(
        envelope,
        run_id=run_id,
        task_id=task_id,
        capability=capability,
        decision=decision,
    )
    _validate_route_event(event, error_message="invalid placement event")
    return finalize_event(event)


def release_placement_response(
    *,
    envelope: dict[str, Any],
    claims: dict[str, Any],
    hub: DeviceHubService,
) -> dict[str, Any]:
    validate_write(envelope, claims)
    payload = extract_payload(envelope, required_fields=["lease_id"])
    lease_id = payload.get("lease_id")
    if not isinstance(lease_id, str) or not lease_id:
        raise HTTPException(status_code=422, detail="payload.lease_id must be non-empty string")

    try:
        decision = hub.release_lease(lease_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    event = build_event(
        envelope,
        event_type="device.lease.released",
        payload={
            "run_id": str(decision["run_id"]),
            "task_id": str(decision["task_id"]),
            "placement_request_id": str(payload.get("placement_request_id") or f"lease:{lease_id}"),
            "decision": {
                "outcome": "lease_released",
                "device_id": str(decision["device_id"]),
                "lease_id": str(decision["lease_id"]),
            },
        },
    )
    _validate_route_event(event, error_message="invalid lease release event")
    return finalize_event(event)


def expire_placement_response(
    *,
    envelope: dict[str, Any],
    claims: dict[str, Any],
    hub: DeviceHubService,
) -> dict[str, Any]:
    validate_write(envelope, claims)
    payload = extract_payload(envelope, required_fields=["lease_id"])
    lease_id = payload.get("lease_id")
    reason_code = payload.get("reason_code", "ttl_expired")
    if not isinstance(lease_id, str) or not lease_id:
        raise HTTPException(status_code=422, detail="payload.lease_id must be non-empty string")
    if not isinstance(reason_code, str) or not reason_code:
        raise HTTPException(status_code=422, detail="payload.reason_code must be non-empty string")

    try:
        decision = hub.expire_lease(lease_id, reason_code=reason_code)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    event = build_event(
        envelope,
        event_type="device.lease.expired",
        payload={
            "run_id": str(decision["run_id"]),
            "task_id": str(decision["task_id"]),
            "placement_request_id": str(payload.get("placement_request_id") or f"lease:{lease_id}"),
            "decision": {
                "outcome": "lease_expired",
                "device_id": str(decision["device_id"]),
                "lease_id": str(decision["lease_id"]),
                "reason_code": str(decision["reason_code"]),
            },
        },
    )
    _validate_route_event(event, error_message="invalid lease expire event")
    return finalize_event(event)


def renew_placement_response(
    *,
    envelope: dict[str, Any],
    claims: dict[str, Any],
    hub: DeviceHubService,
) -> dict[str, Any]:
    validate_write(envelope, claims)
    payload = extract_payload(envelope, required_fields=["lease_id"])
    lease_id = payload.get("lease_id")
    lease_ttl_seconds = payload.get("lease_ttl_seconds", 300)
    if not isinstance(lease_id, str) or not lease_id:
        raise HTTPException(status_code=422, detail="payload.lease_id must be non-empty string")
    if (
        not isinstance(lease_ttl_seconds, int)
        or isinstance(lease_ttl_seconds, bool)
        or lease_ttl_seconds < 30
        or lease_ttl_seconds > 3600
    ):
        raise HTTPException(status_code=422, detail="payload.lease_ttl_seconds must be integer in [30, 3600]")

    try:
        decision = hub.renew_lease(lease_id, lease_ttl_seconds=lease_ttl_seconds)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    event = build_event(
        envelope,
        event_type="device.lease.renewed",
        payload={
            "run_id": str(decision["run_id"]),
            "task_id": str(decision["task_id"]),
            "placement_request_id": str(payload.get("placement_request_id") or f"lease:{lease_id}"),
            "decision": {
                "outcome": "lease_renewed",
                "device_id": str(decision["device_id"]),
                "lease_id": str(decision["lease_id"]),
                "lease_expires_at": str(decision["lease_expires_at"]),
            },
        },
    )
    _validate_route_event(event, error_message="invalid lease renew event")
    return finalize_event(event)
