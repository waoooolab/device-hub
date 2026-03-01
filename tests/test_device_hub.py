from __future__ import annotations

from datetime import datetime, timedelta, timezone

from device_hub.devices.heartbeat import refresh_presence
from device_hub.service import DeviceHubService


def test_pairing_flow_and_heartbeat_online():
    svc = DeviceHubService()
    svc.register_device("desktop-1", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-1")
    rec = svc.approve_pairing(req.code)
    assert rec.paired is True
    assert rec.status == "paired"

    rec = svc.receive_heartbeat("desktop-1")
    assert rec.status == "online"


def test_presence_refresh_marks_offline():
    svc = DeviceHubService()
    svc.register_device("mobile-1", ["camera.capture"])
    req = svc.request_pairing("mobile-1")
    svc.approve_pairing(req.code)
    rec = svc.receive_heartbeat("mobile-1")
    assert rec.status == "online"

    old = (datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat()
    svc.registry.devices["mobile-1"].last_seen_at = old
    refresh_presence(svc.registry, timeout_seconds=30)
    assert svc.registry.devices["mobile-1"].status == "offline"


def test_route_capability_with_load():
    svc = DeviceHubService()
    svc.register_device("desktop-a", ["compute.comfyui.local"])
    svc.register_device("desktop-b", ["compute.comfyui.local"])
    req_a = svc.request_pairing("desktop-a")
    req_b = svc.request_pairing("desktop-b")
    svc.approve_pairing(req_a.code)
    svc.approve_pairing(req_b.code)
    svc.receive_heartbeat("desktop-a")
    svc.receive_heartbeat("desktop-b")

    chosen = svc.route_capability(
        "compute.comfyui.local", load_by_device={"desktop-a": 5, "desktop-b": 1}
    )
    assert chosen == "desktop-b"


def test_revoked_device_not_routable():
    svc = DeviceHubService()
    svc.register_device("desktop-revoked", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-revoked")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-revoked")
    assert svc.route_capability("compute.comfyui.local") == "desktop-revoked"

    svc.revoke_device("desktop-revoked")
    assert svc.route_capability("compute.comfyui.local") is None


def test_route_command_preserves_trace_id():
    svc = DeviceHubService()
    svc.register_device("desktop-trace", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-trace")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-trace")

    routed = svc.route_command(
        capability="compute.comfyui.local",
        command_type="tool.exec",
        payload={"x": 1},
        trace_id="trace-route-1",
    )
    assert routed is not None
    assert routed["device_id"] == "desktop-trace"
    assert routed["trace_id"] == "trace-route-1"
    assert routed["command_type"] == "tool.exec"
