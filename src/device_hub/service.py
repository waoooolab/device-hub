"""Device-hub application service."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from .devices.pairing import PairingManager, PairingRequest
from .devices.registry import DeviceRecord, DeviceRegistry
from .resources.capability_registry import CapabilityRegistry
from .routing.device_router import choose_device


@dataclass
class LeaseRecord:
    lease_id: str
    run_id: str
    task_id: str
    device_id: str
    capability: str
    trace_id: str
    lease_expires_at: str
    status: str = "active"
    released_at: str | None = None
    expired_at: str | None = None
    expire_reason_code: str | None = None


@dataclass
class DeviceHubService:
    registry: DeviceRegistry = field(default_factory=DeviceRegistry)
    capabilities: CapabilityRegistry = field(default_factory=CapabilityRegistry)
    pairing: PairingManager = field(default_factory=PairingManager)
    leases: dict[str, LeaseRecord] = field(default_factory=dict)

    @staticmethod
    def _rejected_placement(run_id: str, task_id: str, capability: str) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "task_id": task_id,
            "outcome": "rejected",
            "reason_code": "no_eligible_device",
            "reason": f"no eligible device for capability '{capability}'",
            "capability_match": [capability],
        }

    @staticmethod
    def _lease_expiry(lease_ttl_seconds: int) -> str:
        ttl_seconds = max(30, lease_ttl_seconds)
        return (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()

    def _acquired_placement(
        self,
        *,
        run_id: str,
        task_id: str,
        capability: str,
        trace_id: str,
        device_id: str,
        lease_ttl_seconds: int,
    ) -> dict[str, Any]:
        self.registry.mark_busy(device_id)
        lease_id = f"lease-{uuid4()}"
        lease_expires_at = self._lease_expiry(lease_ttl_seconds)
        self.leases[lease_id] = LeaseRecord(
            lease_id=lease_id,
            run_id=run_id,
            task_id=task_id,
            device_id=device_id,
            capability=capability,
            trace_id=trace_id,
            lease_expires_at=lease_expires_at,
        )
        return {
            "run_id": run_id,
            "task_id": task_id,
            "outcome": "lease_acquired",
            "device_id": device_id,
            "lease_id": lease_id,
            "lease_expires_at": lease_expires_at,
            "capability_match": [capability],
            "trace_id": trace_id,
        }

    def register_device(self, device_id: str, capabilities: list[str]) -> DeviceRecord:
        rec = self.registry.register(device_id=device_id, capabilities=capabilities)
        for capability in capabilities:
            self.capabilities.bind(device_id, capability)
        return rec

    def request_pairing(self, device_id: str, ttl_seconds: int = 300) -> PairingRequest:
        if device_id not in self.registry.devices:
            raise ValueError("device not registered")
        return self.pairing.create_request(device_id=device_id, ttl_seconds=ttl_seconds)

    def approve_pairing(self, code: str) -> DeviceRecord:
        device_id = self.pairing.approve(code)
        return self.registry.approve_pairing(device_id)

    def receive_heartbeat(self, device_id: str) -> DeviceRecord:
        return self.registry.heartbeat(device_id)

    def revoke_device(self, device_id: str) -> DeviceRecord:
        self.capabilities.unbind_device(device_id)
        return self.registry.revoke(device_id)

    def route_capability(
        self, capability: str, load_by_device: dict[str, int] | None = None
    ) -> str | None:
        candidate_ids = self.capabilities.candidates(capability)
        active_ids: list[str] = []
        for device_id in candidate_ids:
            rec = self.registry.devices.get(device_id)
            if rec and rec.paired and rec.status in {"paired", "online", "busy", "degraded"}:
                active_ids.append(device_id)
        return choose_device(active_ids, load_by_device=load_by_device)

    def route_command(
        self,
        *,
        capability: str,
        command_type: str,
        payload: dict[str, Any],
        trace_id: str,
        load_by_device: dict[str, int] | None = None,
    ) -> dict[str, Any] | None:
        """Build a routed command envelope while preserving trace id."""
        device_id = self.route_capability(capability, load_by_device=load_by_device)
        if not device_id:
            return None
        return {
            "command_id": f"cmd-{uuid4()}",
            "command_type": command_type,
            "device_id": device_id,
            "capability": capability,
            "trace_id": trace_id,
            "payload": payload,
        }

    def allocate_placement(
        self,
        *,
        run_id: str,
        task_id: str,
        capability: str,
        trace_id: str,
        load_by_device: dict[str, int] | None = None,
        lease_ttl_seconds: int = 300,
    ) -> dict[str, Any]:
        """Select a device and mint a short-lived lease descriptor."""
        device_id = self.route_capability(capability, load_by_device=load_by_device)
        if not device_id:
            return self._rejected_placement(run_id, task_id, capability)
        return self._acquired_placement(
            run_id=run_id,
            task_id=task_id,
            capability=capability,
            trace_id=trace_id,
            device_id=device_id,
            lease_ttl_seconds=lease_ttl_seconds,
        )

    def release_lease(self, lease_id: str) -> dict[str, Any]:
        lease = self.leases.get(lease_id)
        if lease is None:
            raise KeyError(f"lease not found: {lease_id}")
        if lease.status == "released":
            return {
                "run_id": lease.run_id,
                "task_id": lease.task_id,
                "outcome": "lease_released",
                "device_id": lease.device_id,
                "lease_id": lease.lease_id,
            }
        if lease.status == "expired":
            raise ValueError("lease already expired")

        lease.status = "released"
        lease.released_at = datetime.now(timezone.utc).isoformat()
        if lease.device_id in self.registry.devices:
            self.registry.heartbeat(lease.device_id)
        return {
            "run_id": lease.run_id,
            "task_id": lease.task_id,
            "outcome": "lease_released",
            "device_id": lease.device_id,
            "lease_id": lease.lease_id,
        }

    def expire_lease(self, lease_id: str, *, reason_code: str = "ttl_expired") -> dict[str, Any]:
        lease = self.leases.get(lease_id)
        if lease is None:
            raise KeyError(f"lease not found: {lease_id}")
        if lease.status == "released":
            raise ValueError("lease already released")
        if lease.status == "expired":
            return {
                "run_id": lease.run_id,
                "task_id": lease.task_id,
                "outcome": "lease_expired",
                "device_id": lease.device_id,
                "lease_id": lease.lease_id,
                "reason_code": lease.expire_reason_code or reason_code,
            }

        lease.status = "expired"
        lease.expired_at = datetime.now(timezone.utc).isoformat()
        lease.expire_reason_code = reason_code
        if lease.device_id in self.registry.devices:
            self.registry.heartbeat(lease.device_id)
        return {
            "run_id": lease.run_id,
            "task_id": lease.task_id,
            "outcome": "lease_expired",
            "device_id": lease.device_id,
            "lease_id": lease.lease_id,
            "reason_code": reason_code,
        }
