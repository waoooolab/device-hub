"""Device registration and heartbeat registry."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class DeviceRecord:
    device_id: str
    capabilities: list[str]
    status: str = "offline"
    paired: bool = False
    last_seen_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class DeviceRegistry:
    devices: dict[str, DeviceRecord] = field(default_factory=dict)

    def register(self, device_id: str, capabilities: list[str]) -> DeviceRecord:
        rec = DeviceRecord(device_id=device_id, capabilities=capabilities)
        self.devices[device_id] = rec
        return rec

    def approve_pairing(self, device_id: str) -> DeviceRecord:
        rec = self.devices[device_id]
        rec.paired = True
        rec.status = "paired"
        return rec

    def heartbeat(self, device_id: str) -> DeviceRecord:
        rec = self.devices[device_id]
        if rec.status != "revoked":
            rec.status = "online" if rec.paired else rec.status
        rec.last_seen_at = datetime.now(timezone.utc).isoformat()
        return rec

    def mark_offline(self, device_id: str) -> DeviceRecord:
        rec = self.devices[device_id]
        if rec.status != "revoked":
            rec.status = "offline"
        return rec

    def mark_busy(self, device_id: str) -> DeviceRecord:
        rec = self.devices[device_id]
        if rec.status != "revoked":
            rec.status = "busy"
        return rec

    def mark_degraded(self, device_id: str) -> DeviceRecord:
        rec = self.devices[device_id]
        if rec.status != "revoked":
            rec.status = "degraded"
        return rec

    def revoke(self, device_id: str) -> DeviceRecord:
        rec = self.devices[device_id]
        rec.status = "revoked"
        rec.paired = False
        return rec
