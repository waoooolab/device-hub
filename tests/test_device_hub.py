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


def test_allocate_placement_returns_rejected_when_no_candidate():
    svc = DeviceHubService()
    decision = svc.allocate_placement(
        run_id="run-1",
        task_id="task-1",
        capability="compute.comfyui.local",
        trace_id="trace-1",
    )
    assert decision["outcome"] == "rejected"
    assert decision["reason_code"] == "no_eligible_device"


def test_release_and_expire_lease_lifecycle():
    svc = DeviceHubService()
    svc.register_device("desktop-lease", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-lease")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-lease")

    allocated = svc.allocate_placement(
        run_id="run-lease",
        task_id="task-lease",
        capability="compute.comfyui.local",
        trace_id="trace-lease",
    )
    assert allocated["outcome"] == "lease_acquired"
    lease_id = allocated["lease_id"]

    released = svc.release_lease(lease_id)
    assert released["outcome"] == "lease_released"
    assert released["lease_id"] == lease_id

    allocated2 = svc.allocate_placement(
        run_id="run-lease-2",
        task_id="task-lease-2",
        capability="compute.comfyui.local",
        trace_id="trace-lease-2",
    )
    lease_id2 = allocated2["lease_id"]
    expired = svc.expire_lease(lease_id2, reason_code="ttl_expired")
    assert expired["outcome"] == "lease_expired"
    assert expired["reason_code"] == "ttl_expired"


def test_allocate_placement_rejects_when_capacity_is_exhausted() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-capacity", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-capacity")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-capacity")

    first = svc.allocate_placement(
        run_id="run-capacity-1",
        task_id="task-capacity-1",
        capability="compute.comfyui.local",
        trace_id="trace-capacity-1",
    )
    assert first["outcome"] == "lease_acquired"

    second = svc.allocate_placement(
        run_id="run-capacity-2",
        task_id="task-capacity-2",
        capability="compute.comfyui.local",
        trace_id="trace-capacity-2",
    )
    assert second["outcome"] == "rejected"
    assert second["reason_code"] == "capacity_exhausted"


def test_capacity_snapshot_expires_stale_active_lease() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-stale-lease", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-stale-lease")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-stale-lease")

    allocated = svc.allocate_placement(
        run_id="run-stale-lease",
        task_id="task-stale-lease",
        capability="compute.comfyui.local",
        trace_id="trace-stale-lease",
    )
    lease_id = allocated["lease_id"]
    lease = svc.leases[lease_id]
    lease.lease_expires_at = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()

    snapshot = svc.placement_capacity_snapshot()
    assert snapshot["active_leases"] == 0
    assert snapshot["available_slots"] >= 1
    assert svc.leases[lease_id].status == "expired"
    assert svc.leases[lease_id].expire_reason_code == "ttl_expired"


def test_get_lease_snapshot_expires_stale_active_lease() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-stale-view", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-stale-view")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-stale-view")

    allocated = svc.allocate_placement(
        run_id="run-stale-view",
        task_id="task-stale-view",
        capability="compute.comfyui.local",
        trace_id="trace-stale-view",
    )
    lease_id = allocated["lease_id"]
    svc.leases[lease_id].lease_expires_at = (
        datetime.now(timezone.utc) - timedelta(seconds=5)
    ).isoformat()

    snapshot = svc.get_lease_snapshot(lease_id)
    assert snapshot["status"] == "expired"
    assert snapshot["expire_reason_code"] == "ttl_expired"
    assert isinstance(snapshot["expired_at"], str)


def test_release_lease_rejects_when_ttl_already_expired() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-stale-release", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-stale-release")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-stale-release")

    allocated = svc.allocate_placement(
        run_id="run-stale-release",
        task_id="task-stale-release",
        capability="compute.comfyui.local",
        trace_id="trace-stale-release",
    )
    lease_id = allocated["lease_id"]
    svc.leases[lease_id].lease_expires_at = (
        datetime.now(timezone.utc) - timedelta(seconds=5)
    ).isoformat()

    try:
        svc.release_lease(lease_id)
    except ValueError as exc:
        assert str(exc) == "lease already expired"
    else:
        raise AssertionError("release_lease should reject ttl-expired lease")
