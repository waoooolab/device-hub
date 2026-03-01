"""Desktop node-agent baseline adapter."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class DesktopNodeAgentAdapter:
    device_id: str
    paired: bool = False

    def build_heartbeat(self) -> dict:
        return {
            "device_id": self.device_id,
            "platform": "macos",
            "ts": datetime.now(timezone.utc).isoformat(),
        }

    def build_command_result(self, command_id: str, success: bool, payload: dict) -> dict:
        return {
            "command_id": command_id,
            "device_id": self.device_id,
            "success": success,
            "payload": payload,
            "ts": datetime.now(timezone.utc).isoformat(),
        }
