"""Device registration and heartbeat registry."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Literal, cast

from ..contracts_catalog_runtime import resolve_platform_contracts_root

DeviceStatus = Literal["paired", "online", "busy", "degraded", "offline", "revoked"]
_FALLBACK_DEVICE_STATUS_VALUES: tuple[DeviceStatus, ...] = (
    "paired",
    "online",
    "busy",
    "degraded",
    "offline",
    "revoked",
)


@lru_cache(maxsize=1)
def _contract_device_status_values() -> tuple[str, ...]:
    anchor = Path(__file__).resolve()
    root = resolve_platform_contracts_root(anchor_file=str(anchor))
    schema_path = root / "jsonschema" / "runtime" / "runtime-state.v1.json"
    if not schema_path.exists():
        return tuple(_FALLBACK_DEVICE_STATUS_VALUES)
    try:
        payload = json.loads(schema_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return tuple(_FALLBACK_DEVICE_STATUS_VALUES)
    if not isinstance(payload, dict):
        return tuple(_FALLBACK_DEVICE_STATUS_VALUES)
    properties = payload.get("properties")
    if not isinstance(properties, dict):
        return tuple(_FALLBACK_DEVICE_STATUS_VALUES)
    device_status = properties.get("device_status")
    if not isinstance(device_status, dict):
        return tuple(_FALLBACK_DEVICE_STATUS_VALUES)
    enum_values = device_status.get("enum")
    if not isinstance(enum_values, list):
        return tuple(_FALLBACK_DEVICE_STATUS_VALUES)
    values = tuple(str(item).strip().lower() for item in enum_values if isinstance(item, str))
    return values or tuple(_FALLBACK_DEVICE_STATUS_VALUES)


def normalize_device_status(value: str) -> DeviceStatus:
    candidate = str(value).strip().lower()
    if candidate not in _contract_device_status_values():
        raise ValueError(f"invalid device status: {value}")
    return cast(DeviceStatus, candidate)


def is_valid_device_status(value: str) -> bool:
    try:
        normalize_device_status(value)
    except ValueError:
        return False
    return True


@dataclass
class DeviceRecord:
    device_id: str
    capabilities: list[str]
    execution_site: str = "local"
    region: str | None = None
    cost_tier: str = "balanced"
    node_pool: str | None = None
    estimated_cost_usd: float | None = None
    status: DeviceStatus = "offline"
    paired: bool = False
    last_seen_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def __post_init__(self) -> None:
        self.status = normalize_device_status(self.status)


@dataclass
class DeviceRegistry:
    devices: dict[str, DeviceRecord] = field(default_factory=dict)

    def register(
        self,
        device_id: str,
        capabilities: list[str],
        *,
        execution_site: str = "local",
        region: str | None = None,
        cost_tier: str = "balanced",
        node_pool: str | None = None,
        estimated_cost_usd: float | None = None,
    ) -> DeviceRecord:
        rec = DeviceRecord(
            device_id=device_id,
            capabilities=capabilities,
            execution_site=execution_site,
            region=region,
            cost_tier=cost_tier,
            node_pool=node_pool,
            estimated_cost_usd=estimated_cost_usd,
        )
        self.devices[device_id] = rec
        return rec

    def approve_pairing(self, device_id: str) -> DeviceRecord:
        rec = self.devices[device_id]
        rec.paired = True
        rec.status = normalize_device_status("paired")
        return rec

    def heartbeat(self, device_id: str) -> DeviceRecord:
        rec = self.devices[device_id]
        if rec.status != "revoked":
            rec.status = normalize_device_status("online") if rec.paired else rec.status
        rec.last_seen_at = datetime.now(timezone.utc).isoformat()
        return rec

    def mark_offline(self, device_id: str) -> DeviceRecord:
        rec = self.devices[device_id]
        if rec.status != "revoked":
            rec.status = normalize_device_status("offline")
        return rec

    def mark_busy(self, device_id: str) -> DeviceRecord:
        rec = self.devices[device_id]
        if rec.status != "revoked":
            rec.status = normalize_device_status("busy")
        return rec

    def mark_degraded(self, device_id: str) -> DeviceRecord:
        rec = self.devices[device_id]
        if rec.status != "revoked":
            rec.status = normalize_device_status("degraded")
        return rec

    def revoke(self, device_id: str) -> DeviceRecord:
        rec = self.devices[device_id]
        rec.status = normalize_device_status("revoked")
        rec.paired = False
        return rec
