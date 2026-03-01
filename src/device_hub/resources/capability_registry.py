"""Capability registry for device routing."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CapabilityRegistry:
    by_capability: dict[str, set[str]] = field(default_factory=dict)

    def bind(self, device_id: str, capability: str) -> None:
        if capability not in self.by_capability:
            self.by_capability[capability] = set()
        self.by_capability[capability].add(device_id)

    def candidates(self, capability: str) -> list[str]:
        return sorted(self.by_capability.get(capability, set()))

    def unbind_device(self, device_id: str) -> None:
        for capability, device_ids in self.by_capability.items():
            if device_id in device_ids:
                device_ids.remove(device_id)
