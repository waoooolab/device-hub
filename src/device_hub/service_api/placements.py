"""Placement lifecycle handlers for device-hub API."""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from device_hub.code_terms import normalize_optional_code_term
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

_RESOURCE_SNAPSHOT_INT_FIELDS = (
    "gpu_memory_free_mb",
    "queue_depth",
    "eligible_devices",
    "active_leases",
    "available_slots",
    "tenant_active_leases",
    "tenant_limit",
)
_RESOURCE_SNAPSHOT_FLOAT_FIELDS = ("gpu_utilization_percent",)
_RESOURCE_SNAPSHOT_STR_FIELDS = ("tenant_id",)
_PLACEMENT_AUDIT_INT_FIELDS = ("candidate_device_count",)
_PLACEMENT_AUDIT_BOOL_FIELDS = ("fallback_applied",)
_PLACEMENT_AUDIT_STR_FIELDS = (
    "selected_device_id",
    "selected_execution_site",
    "selected_region",
    "selected_node_pool",
    "fallback_reason_code",
    "failure_domain",
)
_PLACEMENT_AUDIT_STR_LIST_FIELDS = ("candidate_execution_sites",)


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


def _filter_resource_snapshot(raw_snapshot: Any) -> dict[str, Any] | None:
    if not isinstance(raw_snapshot, dict):
        return None
    snapshot: dict[str, Any] = {}
    for key in _RESOURCE_SNAPSHOT_INT_FIELDS:
        value = raw_snapshot.get(key)
        if isinstance(value, int) and value >= 0:
            snapshot[key] = value
    for key in _RESOURCE_SNAPSHOT_FLOAT_FIELDS:
        value = raw_snapshot.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            snapshot[key] = float(value)
    for key in _RESOURCE_SNAPSHOT_STR_FIELDS:
        value = raw_snapshot.get(key)
        if isinstance(value, str) and value.strip():
            snapshot[key] = value
    if not snapshot:
        return None
    return snapshot


def _filter_placement_audit(raw_audit: Any) -> dict[str, Any] | None:
    if not isinstance(raw_audit, dict):
        return None
    audit: dict[str, Any] = {}
    for key in _PLACEMENT_AUDIT_INT_FIELDS:
        value = raw_audit.get(key)
        if isinstance(value, int) and value >= 0:
            audit[key] = value
    for key in _PLACEMENT_AUDIT_BOOL_FIELDS:
        value = raw_audit.get(key)
        if isinstance(value, bool):
            audit[key] = value
    for key in _PLACEMENT_AUDIT_STR_FIELDS:
        value = raw_audit.get(key)
        if isinstance(value, str) and value.strip():
            audit[key] = value
    for key in _PLACEMENT_AUDIT_STR_LIST_FIELDS:
        value = raw_audit.get(key)
        if isinstance(value, list):
            normalized = [item for item in value if isinstance(item, str) and item.strip()]
            if normalized:
                audit[key] = normalized
    if not audit:
        return None
    return audit


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
        filtered_snapshot = _filter_resource_snapshot(decision.get("resource_snapshot"))
        if isinstance(filtered_snapshot, dict):
            payload["resource_snapshot"] = filtered_snapshot
        filtered_audit = _filter_placement_audit(decision.get("placement_audit"))
        if isinstance(filtered_audit, dict):
            payload["placement_audit"] = filtered_audit
        reason_code = normalize_optional_code_term(decision.get("reason_code"))
        if reason_code:
            payload["reason_code"] = reason_code
        reason = decision.get("reason")
        if isinstance(reason, str) and reason.strip():
            payload["reason"] = reason
        return payload
    normalized_reason_code = normalize_optional_code_term(decision.get("reason_code"))
    if normalized_reason_code is None:
        normalized_reason_code = "unknown_reason"
    payload = {
        "outcome": outcome,
        "reason_code": normalized_reason_code,
        "reason": str(decision["reason"]),
        "capability_match": capability_match,
    }
    filtered_snapshot = _filter_resource_snapshot(decision.get("resource_snapshot"))
    if isinstance(filtered_snapshot, dict):
        payload["resource_snapshot"] = filtered_snapshot
    filtered_audit = _filter_placement_audit(decision.get("placement_audit"))
    if isinstance(filtered_audit, dict):
        payload["placement_audit"] = filtered_audit
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
    reason_code = normalize_optional_code_term(payload.get("reason_code", "ttl_expired"))
    if not isinstance(lease_id, str) or not lease_id:
        raise HTTPException(status_code=422, detail="payload.lease_id must be non-empty string")
    if reason_code is None:
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


def preempt_placement_response(
    *,
    envelope: dict[str, Any],
    claims: dict[str, Any],
    hub: DeviceHubService,
) -> dict[str, Any]:
    validate_write(envelope, claims)
    payload = extract_payload(envelope, required_fields=["lease_id"])
    lease_id = payload.get("lease_id")
    reason_code = normalize_optional_code_term(payload.get("reason_code", "preempted_by_policy"))
    if not isinstance(lease_id, str) or not lease_id:
        raise HTTPException(status_code=422, detail="payload.lease_id must be non-empty string")
    if reason_code is None:
        raise HTTPException(status_code=422, detail="payload.reason_code must be non-empty string")

    try:
        decision = hub.preempt_lease(lease_id, reason_code=reason_code)
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
    _validate_route_event(event, error_message="invalid lease preempt event")
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
