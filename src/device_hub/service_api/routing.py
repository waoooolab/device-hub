"""Routing and presence handlers for device-hub API."""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from device_hub.devices.heartbeat import refresh_presence
from device_hub.service import DeviceHubService

from .contracts import validate_runtime_device_status
from .support import build_event, extract_payload, finalize_event, validate_write


def _validate_load_by_device(load_by_device: Any) -> None:
    if load_by_device is None:
        return
    if not isinstance(load_by_device, dict) or any(
        not isinstance(key, str) or not isinstance(value, int) for key, value in load_by_device.items()
    ):
        raise HTTPException(status_code=422, detail="payload.load_by_device must be {device_id: int}")


def _offline_ids(hub: DeviceHubService) -> set[str]:
    return {device_id for device_id, record in hub.registry.devices.items() if record.status == "offline"}


def refresh_presence_response(
    *,
    envelope: dict[str, Any],
    claims: dict[str, Any],
    hub: DeviceHubService,
) -> dict[str, Any]:
    validate_write(envelope, claims)
    payload = extract_payload(envelope, required_fields=["timeout_seconds"])
    timeout_seconds = payload.get("timeout_seconds")
    if not isinstance(timeout_seconds, int) or timeout_seconds < 1:
        raise HTTPException(status_code=422, detail="payload.timeout_seconds must be integer >= 1")

    before_offline = _offline_ids(hub)
    refresh_presence(hub.registry, timeout_seconds=timeout_seconds)
    changed_to_offline = sorted(_offline_ids(hub) - before_offline)
    for device_id in changed_to_offline:
        record = hub.registry.devices.get(device_id)
        if record is not None:
            validate_runtime_device_status(record.status)
    event = build_event(
        envelope,
        event_type="device.presence.refreshed",
        payload={
            "timeout_seconds": timeout_seconds,
            "updated_to_offline": changed_to_offline,
            "updated_count": len(changed_to_offline),
        },
    )
    return finalize_event(event)


def _route_payload(payload: dict[str, Any]) -> tuple[str, str, dict[str, Any], dict[str, int] | None]:
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
    _validate_load_by_device(load_by_device)
    return capability, command_type, command_payload, load_by_device


def _route_event(envelope: dict[str, Any], capability: str, routed: dict[str, Any] | None) -> dict[str, Any]:
    if routed is None:
        return build_event(
            envelope,
            event_type="device.route.miss",
            payload={"capability": capability, "reason": "no_eligible_device"},
        )
    return build_event(
        envelope,
        event_type="device.route.selected",
        payload={"route": routed},
    )


def route_command_response(
    *,
    envelope: dict[str, Any],
    claims: dict[str, Any],
    hub: DeviceHubService,
) -> dict[str, Any]:
    validate_write(envelope, claims)
    payload = extract_payload(
        envelope,
        required_fields=["capability", "command_type", "command_payload"],
    )
    capability, command_type, command_payload, load_by_device = _route_payload(payload)
    routed = hub.route_command(
        capability=capability,
        command_type=command_type,
        payload=command_payload,
        trace_id=str(envelope["trace_id"]),
        load_by_device=load_by_device,
    )
    return finalize_event(_route_event(envelope, capability, routed))
