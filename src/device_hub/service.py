"""Device-hub application service."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from .devices.pairing import PairingManager, PairingRequest
from .devices.registry import DeviceRecord, DeviceRegistry
from .resources.capability_registry import CapabilityRegistry
from .routing.device_router import choose_device


@dataclass
class DeviceHubService:
    registry: DeviceRegistry = field(default_factory=DeviceRegistry)
    capabilities: CapabilityRegistry = field(default_factory=CapabilityRegistry)
    pairing: PairingManager = field(default_factory=PairingManager)

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
