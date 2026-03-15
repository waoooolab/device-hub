from __future__ import annotations

import json
from pathlib import Path

from device_hub.devices.registry import DeviceRegistry


def _runtime_state_contract_path() -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "contracts" / "runtime" / "runtime-state.v1.json"


def _contract_device_statuses() -> set[str]:
    raw = json.loads(_runtime_state_contract_path().read_text(encoding="utf-8"))
    properties = raw.get("properties")
    assert isinstance(properties, dict)
    device_status = properties.get("device_status")
    assert isinstance(device_status, dict)
    enum_values = device_status.get("enum")
    assert isinstance(enum_values, list)
    return {str(item) for item in enum_values if isinstance(item, str)}


def test_device_registry_statuses_match_runtime_state_contract() -> None:
    registry = DeviceRegistry()

    observed = {registry.register("device-contract-alignment", ["compute.comfyui.local"]).status}
    observed.add(registry.approve_pairing("device-contract-alignment").status)
    observed.add(registry.heartbeat("device-contract-alignment").status)
    observed.add(registry.mark_busy("device-contract-alignment").status)
    observed.add(registry.mark_degraded("device-contract-alignment").status)
    observed.add(registry.mark_offline("device-contract-alignment").status)
    observed.add(registry.revoke("device-contract-alignment").status)

    assert observed == _contract_device_statuses()
