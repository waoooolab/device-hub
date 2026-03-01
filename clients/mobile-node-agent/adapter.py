"""Mobile node-agent baseline adapter."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class MobileNodeAgentAdapter:
    device_id: str
    platform: str = "ios"
    paired: bool = False

    def build_heartbeat(self) -> dict:
        return {
            "device_id": self.device_id,
            "platform": self.platform,
            "ts": datetime.now(timezone.utc).isoformat(),
        }

    def build_capability_snapshot(self, capabilities: list[str]) -> dict:
        return {
            "device_id": self.device_id,
            "platform": self.platform,
            "capabilities": sorted(capabilities),
            "ts": datetime.now(timezone.utc).isoformat(),
        }
