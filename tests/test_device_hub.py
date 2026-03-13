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


def test_renew_lease_extends_expiry_for_active_lease() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-renew", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-renew")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-renew")

    allocated = svc.allocate_placement(
        run_id="run-renew-1",
        task_id="task-renew-1",
        capability="compute.comfyui.local",
        trace_id="trace-renew-1",
    )
    lease_id = allocated["lease_id"]
    previous_expires_at = allocated["lease_expires_at"]

    renewed = svc.renew_lease(lease_id, lease_ttl_seconds=600)
    assert renewed["outcome"] == "lease_renewed"
    assert renewed["lease_id"] == lease_id
    assert datetime.fromisoformat(renewed["lease_expires_at"]) > datetime.fromisoformat(
        previous_expires_at
    )
    assert svc.leases[lease_id].status == "active"


def test_renew_lease_rejects_released_or_expired_lease() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-renew-terminal", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-renew-terminal")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-renew-terminal")

    released_decision = svc.allocate_placement(
        run_id="run-renew-terminal-release",
        task_id="task-renew-terminal-release",
        capability="compute.comfyui.local",
        trace_id="trace-renew-terminal-release",
    )
    released_lease_id = released_decision["lease_id"]
    svc.release_lease(released_lease_id)
    try:
        svc.renew_lease(released_lease_id)
    except ValueError as exc:
        assert str(exc) == "lease already released"
    else:
        raise AssertionError("renew_lease should reject released lease")

    expired_decision = svc.allocate_placement(
        run_id="run-renew-terminal-expire",
        task_id="task-renew-terminal-expire",
        capability="compute.comfyui.local",
        trace_id="trace-renew-terminal-expire",
    )
    expired_lease_id = expired_decision["lease_id"]
    svc.expire_lease(expired_lease_id, reason_code="ttl_expired")
    try:
        svc.renew_lease(expired_lease_id)
    except ValueError as exc:
        assert str(exc) == "lease already expired"
    else:
        raise AssertionError("renew_lease should reject expired lease")


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


def test_allocate_placement_rejects_when_tenant_quota_is_exhausted() -> None:
    svc = DeviceHubService(max_active_leases_per_tenant=1)
    svc.register_device("desktop-quota-a", ["compute.comfyui.local"])
    svc.register_device("desktop-quota-b", ["compute.comfyui.local"])
    req_a = svc.request_pairing("desktop-quota-a")
    req_b = svc.request_pairing("desktop-quota-b")
    svc.approve_pairing(req_a.code)
    svc.approve_pairing(req_b.code)
    svc.receive_heartbeat("desktop-quota-a")
    svc.receive_heartbeat("desktop-quota-b")

    first = svc.allocate_placement(
        run_id="run-quota-1",
        task_id="task-quota-1",
        capability="compute.comfyui.local",
        trace_id="trace-quota-1",
        tenant_id="t1",
    )
    assert first["outcome"] == "lease_acquired"

    second = svc.allocate_placement(
        run_id="run-quota-2",
        task_id="task-quota-2",
        capability="compute.comfyui.local",
        trace_id="trace-quota-2",
        tenant_id="t1",
    )
    assert second["outcome"] == "rejected"
    assert second["reason_code"] == "tenant_quota_exhausted"
    assert second["resource_snapshot"]["tenant_id"] == "t1"
    assert second["resource_snapshot"]["tenant_active_leases"] == 1
    assert second["resource_snapshot"]["tenant_limit"] == 1

    third = svc.allocate_placement(
        run_id="run-quota-3",
        task_id="task-quota-3",
        capability="compute.comfyui.local",
        trace_id="trace-quota-3",
        tenant_id="t2",
    )
    assert third["outcome"] == "lease_acquired"


def test_allocate_placement_prefers_local_and_falls_back_to_cloud() -> None:
    svc = DeviceHubService()
    svc.register_device(
        "desktop-local",
        ["compute.comfyui.local"],
        execution_site="local",
        region="us-west",
        cost_tier="balanced",
    )
    svc.register_device(
        "desktop-cloud",
        ["compute.comfyui.local"],
        execution_site="cloud",
        region="us-west",
        cost_tier="balanced",
    )
    req_local = svc.request_pairing("desktop-local")
    req_cloud = svc.request_pairing("desktop-cloud")
    svc.approve_pairing(req_local.code)
    svc.approve_pairing(req_cloud.code)
    svc.receive_heartbeat("desktop-local")
    svc.receive_heartbeat("desktop-cloud")

    local_selected = svc.allocate_placement(
        run_id="run-pref-local-1",
        task_id="task-pref-local-1",
        capability="compute.comfyui.local",
        trace_id="trace-pref-local-1",
        placement_constraints={"prefer_local": True},
    )
    assert local_selected["outcome"] == "lease_acquired"
    assert local_selected["device_id"] == "desktop-local"
    assert local_selected.get("reason_code") is None

    cloud_fallback = svc.allocate_placement(
        run_id="run-pref-local-2",
        task_id="task-pref-local-2",
        capability="compute.comfyui.local",
        trace_id="trace-pref-local-2",
        placement_constraints={"prefer_local": True},
    )
    assert cloud_fallback["outcome"] == "lease_acquired"
    assert cloud_fallback["device_id"] == "desktop-cloud"
    assert cloud_fallback["reason_code"] == "local_preference_fallback"
    assert "fallback" in cloud_fallback["reason"]


def test_allocate_placement_rejects_when_region_or_cost_constraints_fail() -> None:
    svc = DeviceHubService()
    svc.register_device(
        "gpu-west-low",
        ["compute.comfyui.local"],
        execution_site="cloud",
        region="us-west",
        cost_tier="low",
        estimated_cost_usd=0.6,
    )
    req = svc.request_pairing("gpu-west-low")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("gpu-west-low")

    region_miss = svc.allocate_placement(
        run_id="run-region-miss",
        task_id="task-region-miss",
        capability="compute.comfyui.local",
        trace_id="trace-region-miss",
        placement_constraints={"region": "eu-central"},
    )
    assert region_miss["outcome"] == "rejected"
    assert region_miss["reason_code"] == "region_unavailable"

    cost_miss = svc.allocate_placement(
        run_id="run-cost-miss",
        task_id="task-cost-miss",
        capability="compute.comfyui.local",
        trace_id="trace-cost-miss",
        placement_constraints={"max_cost_usd_hard": 0.5},
    )
    assert cost_miss["outcome"] == "rejected"
    assert cost_miss["reason_code"] == "cost_limit_exceeded"


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
    assert snapshot["lease_status_counts"]["active"] == 0
    assert snapshot["lease_status_counts"]["expired"] >= 1
    assert snapshot["lease_status_counts"]["released"] == 0
    assert snapshot["lease_expired_total"] >= 1
    assert snapshot["lease_expire_sweeps_total"] >= 1
    assert snapshot["lease_expire_last_sweep_expired"] >= 1
    assert isinstance(snapshot["lease_expire_last_sweep_at"], str)
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
