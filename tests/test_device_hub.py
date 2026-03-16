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
    assert routed["outcome"] == "selected"
    assert routed["capability_match"] == ["compute.comfyui.local"]
    assert routed["resource_snapshot"]["eligible_devices"] >= 1


def test_route_command_decision_returns_rejected_when_no_eligible_device() -> None:
    svc = DeviceHubService()
    decision = svc.route_command_decision(
        capability="compute.comfyui.local",
        command_type="tool.exec",
        payload={"x": 1},
        trace_id="trace-route-none",
    )
    assert decision["outcome"] == "rejected"
    assert decision["reason_code"] == "no_eligible_device"
    snapshot = decision["resource_snapshot"]
    assert snapshot["eligible_devices"] == 0
    assert snapshot["available_slots"] == 0
    audit = decision["placement_audit"]
    assert audit["candidate_device_count"] == 0
    assert audit["fallback_applied"] is False
    assert audit["failure_domain"] == "capability"


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


def test_allocate_placement_returns_route_unavailable_when_selector_returns_none(monkeypatch) -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-route-none", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-route-none")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-route-none")

    monkeypatch.setattr(
        "device_hub.service.choose_device",
        lambda _candidate_ids, load_by_device=None: None,
    )

    decision = svc.allocate_placement(
        run_id="run-route-none",
        task_id="task-route-none",
        capability="compute.comfyui.local",
        trace_id="trace-route-none",
        tenant_id="t1",
    )
    assert decision["outcome"] == "rejected"
    assert decision["reason_code"] == "route_unavailable"
    assert "unable to select device" in decision["reason"]
    snapshot = decision["resource_snapshot"]
    assert snapshot["eligible_devices"] == 1
    assert snapshot["active_leases"] == 0
    assert snapshot["available_slots"] == 1
    assert snapshot["tenant_id"] == "t1"
    assert snapshot["tenant_active_leases"] == 0
    audit = decision["placement_audit"]
    assert audit["candidate_device_count"] == 1
    assert audit["fallback_applied"] is False
    assert audit["failure_domain"] == "route_selector"


def test_capacity_snapshot_zero_utilization_without_eligible_devices() -> None:
    svc = DeviceHubService()
    snapshot = svc.placement_capacity_snapshot()
    assert snapshot["eligible_devices"] == 0
    assert snapshot["active_leases"] == 0
    assert snapshot["available_slots"] == 0
    assert snapshot["lease_utilization"] == 0.0


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


def test_preempt_lease_marks_active_lease_expired_with_preempt_reason() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-preempt", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-preempt")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-preempt")

    allocated = svc.allocate_placement(
        run_id="run-preempt-1",
        task_id="task-preempt-1",
        capability="compute.comfyui.local",
        trace_id="trace-preempt-1",
    )
    lease_id = allocated["lease_id"]

    preempted = svc.preempt_lease(lease_id)
    assert preempted["outcome"] == "lease_expired"
    assert preempted["lease_id"] == lease_id
    assert preempted["reason_code"] == "preempted_by_policy"
    assert svc.leases[lease_id].status == "expired"
    assert svc.leases[lease_id].expire_reason_code == "preempted_by_policy"


def test_expire_lease_normalizes_reason_code_term() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-expire-normalize", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-expire-normalize")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-expire-normalize")

    allocated = svc.allocate_placement(
        run_id="run-expire-normalize-1",
        task_id="task-expire-normalize-1",
        capability="compute.comfyui.local",
        trace_id="trace-expire-normalize-1",
    )
    lease_id = allocated["lease_id"]

    expired = svc.expire_lease(lease_id, reason_code="TTL.Expired")
    assert expired["reason_code"] == "ttl_expired"
    assert svc.leases[lease_id].expire_reason_code == "ttl_expired"


def test_preempt_lease_normalizes_reason_code_term() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-preempt-normalize", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-preempt-normalize")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-preempt-normalize")

    allocated = svc.allocate_placement(
        run_id="run-preempt-normalize-1",
        task_id="task-preempt-normalize-1",
        capability="compute.comfyui.local",
        trace_id="trace-preempt-normalize-1",
    )
    lease_id = allocated["lease_id"]

    preempted = svc.preempt_lease(lease_id, reason_code="Tool.ContractViolation")
    assert preempted["reason_code"] == "tool_contract_violation"
    assert svc.leases[lease_id].expire_reason_code == "tool_contract_violation"


def test_expire_lease_rejects_empty_reason_code() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-expire-invalid-reason", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-expire-invalid-reason")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-expire-invalid-reason")

    allocated = svc.allocate_placement(
        run_id="run-expire-invalid-reason-1",
        task_id="task-expire-invalid-reason-1",
        capability="compute.comfyui.local",
        trace_id="trace-expire-invalid-reason-1",
    )
    lease_id = allocated["lease_id"]

    try:
        svc.expire_lease(lease_id, reason_code="   ")
    except ValueError as exc:
        assert str(exc) == "reason_code must be non-empty string"
    else:
        raise AssertionError("expire_lease should reject empty reason_code")


def test_preempt_lease_rejects_empty_reason_code() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-preempt-invalid-reason", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-preempt-invalid-reason")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-preempt-invalid-reason")

    allocated = svc.allocate_placement(
        run_id="run-preempt-invalid-reason-1",
        task_id="task-preempt-invalid-reason-1",
        capability="compute.comfyui.local",
        trace_id="trace-preempt-invalid-reason-1",
    )
    lease_id = allocated["lease_id"]

    try:
        svc.preempt_lease(lease_id, reason_code="   ")
    except ValueError as exc:
        assert str(exc) == "reason_code must be non-empty string"
    else:
        raise AssertionError("preempt_lease should reject empty reason_code")


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


def test_lease_policy_tick_auto_renews_expiring_active_leases() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-policy-renew", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-policy-renew")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-policy-renew")

    allocated = svc.allocate_placement(
        run_id="run-policy-renew-1",
        task_id="task-policy-renew-1",
        capability="compute.comfyui.local",
        trace_id="trace-policy-renew-1",
    )
    lease_id = allocated["lease_id"]
    original_expires_at = allocated["lease_expires_at"]
    svc.leases[lease_id].lease_expires_at = (
        datetime.now(timezone.utc) + timedelta(seconds=5)
    ).isoformat()

    signal = svc.lease_policy_tick(
        auto_renew_window_seconds=30,
        auto_renew_ttl_seconds=300,
        enforce_tenant_quota=False,
    )

    assert signal["expired_by_sweep"] == 0
    assert signal["renew_considered"] == 1
    assert signal["renewed"] == 1
    assert signal["preempted"] == 0
    assert signal["active_leases_after"] == 1
    assert svc.leases[lease_id].status == "active"
    assert datetime.fromisoformat(svc.leases[lease_id].lease_expires_at) > datetime.fromisoformat(
        original_expires_at
    )


def test_lease_policy_tick_preempts_over_quota_tenant_leases() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-policy-preempt-a", ["compute.comfyui.local"])
    svc.register_device("desktop-policy-preempt-b", ["compute.comfyui.local"])
    req_a = svc.request_pairing("desktop-policy-preempt-a")
    req_b = svc.request_pairing("desktop-policy-preempt-b")
    svc.approve_pairing(req_a.code)
    svc.approve_pairing(req_b.code)
    svc.receive_heartbeat("desktop-policy-preempt-a")
    svc.receive_heartbeat("desktop-policy-preempt-b")

    first = svc.allocate_placement(
        run_id="run-policy-preempt-1",
        task_id="task-policy-preempt-1",
        capability="compute.comfyui.local",
        trace_id="trace-policy-preempt-1",
        tenant_id="t1",
    )
    second = svc.allocate_placement(
        run_id="run-policy-preempt-2",
        task_id="task-policy-preempt-2",
        capability="compute.comfyui.local",
        trace_id="trace-policy-preempt-2",
        tenant_id="t1",
    )
    assert first["outcome"] == "lease_acquired"
    assert second["outcome"] == "lease_acquired"
    first_lease_id = first["lease_id"]
    second_lease_id = second["lease_id"]

    # Simulate runtime limit change and enforce it via policy tick.
    svc.max_active_leases_per_tenant = 1
    svc.leases[first_lease_id].lease_expires_at = (datetime.now(timezone.utc) + timedelta(minutes=3)).isoformat()
    svc.leases[second_lease_id].lease_expires_at = (datetime.now(timezone.utc) + timedelta(minutes=4)).isoformat()

    signal = svc.lease_policy_tick(
        enforce_tenant_quota=True,
        preempt_reason_code="tenant_quota_policy",
        max_preemptions=4,
    )

    assert signal["renewed"] == 0
    assert signal["preempted"] == 1
    assert signal["active_leases_before"] == 2
    assert signal["active_leases_after"] == 1
    assert signal["preempted_leases"][0]["tenant_id"] == "t1"
    assert signal["preempted_leases"][0]["reason_code"] == "tenant_quota_policy"
    preempted_lease_id = signal["preempted_leases"][0]["lease_id"]
    assert preempted_lease_id in {first_lease_id, second_lease_id}
    assert svc.leases[preempted_lease_id].status == "expired"
    assert svc.leases[preempted_lease_id].expire_reason_code == "tenant_quota_policy"


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
    assert second["resource_snapshot"]["eligible_devices"] == 1
    assert second["resource_snapshot"]["active_leases"] == 1
    assert second["resource_snapshot"]["available_slots"] == 0


def test_allocate_placement_reuses_active_lease_for_same_run_task_replay() -> None:
    svc = DeviceHubService(max_active_leases_per_tenant=1)
    svc.register_device("desktop-replay", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-replay")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-replay")

    first = svc.allocate_placement(
        run_id="run-replay-1",
        task_id="task-replay-1",
        capability="compute.comfyui.local",
        trace_id="trace-replay-1",
        tenant_id="t1",
    )
    assert first["outcome"] == "lease_acquired"
    first_lease_id = first["lease_id"]

    replay = svc.allocate_placement(
        run_id="run-replay-1",
        task_id="task-replay-1",
        capability="compute.comfyui.local",
        trace_id="trace-replay-1-retry",
        tenant_id="t1",
        load_by_device={"desktop-replay": 2},
    )
    assert replay["outcome"] == "lease_acquired"
    assert replay["lease_id"] == first_lease_id
    assert replay["device_id"] == first["device_id"]
    assert replay["lease_expires_at"] == first["lease_expires_at"]
    assert replay["reason_code"] == "idempotent_replay"
    assert replay["resource_snapshot"]["eligible_devices"] == 1
    assert replay["resource_snapshot"]["active_leases"] == 1
    assert replay["resource_snapshot"]["available_slots"] == 0
    assert replay["resource_snapshot"]["queue_depth"] == 2
    assert replay["resource_snapshot"]["tenant_id"] == "t1"
    assert replay["resource_snapshot"]["tenant_active_leases"] == 1
    assert replay["resource_snapshot"]["tenant_limit"] == 1

    active_lease_ids = [lease.lease_id for lease in svc.leases.values() if lease.status == "active"]
    assert active_lease_ids == [first_lease_id]

    quota_rejected = svc.allocate_placement(
        run_id="run-replay-2",
        task_id="task-replay-2",
        capability="compute.comfyui.local",
        trace_id="trace-replay-2",
        tenant_id="t1",
    )
    assert quota_rejected["outcome"] == "rejected"
    assert quota_rejected["reason_code"] == "tenant_quota_exhausted"


def test_allocate_placement_replay_rejects_cross_tenant_context_conflict() -> None:
    svc = DeviceHubService(max_active_leases_per_tenant=2)
    svc.register_device("desktop-replay-tenant", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-replay-tenant")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-replay-tenant")

    first = svc.allocate_placement(
        run_id="run-replay-tenant-1",
        task_id="task-replay-tenant-1",
        capability="compute.comfyui.local",
        trace_id="trace-replay-tenant-1",
        tenant_id="t1",
    )
    assert first["outcome"] == "lease_acquired"
    first_lease_id = first["lease_id"]

    conflict = svc.allocate_placement(
        run_id="run-replay-tenant-1",
        task_id="task-replay-tenant-1",
        capability="compute.comfyui.local",
        trace_id="trace-replay-tenant-1-retry",
        tenant_id="t2",
    )
    assert conflict["outcome"] == "rejected"
    assert conflict["reason_code"] == "tenant_context_conflict"
    assert conflict["resource_snapshot"]["tenant_id"] == "t2"
    assert conflict["resource_snapshot"]["tenant_active_leases"] == 0

    active_lease_ids = [lease.lease_id for lease in svc.leases.values() if lease.status == "active"]
    assert active_lease_ids == [first_lease_id]


def test_allocate_placement_replay_rejects_capability_context_conflict() -> None:
    svc = DeviceHubService(max_active_leases_per_tenant=2)
    svc.register_device("desktop-replay-capability", ["compute.comfyui.local", "compute.alt.local"])
    req = svc.request_pairing("desktop-replay-capability")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-replay-capability")

    first = svc.allocate_placement(
        run_id="run-replay-capability-1",
        task_id="task-replay-capability-1",
        capability="compute.comfyui.local",
        trace_id="trace-replay-capability-1",
        tenant_id="t1",
    )
    assert first["outcome"] == "lease_acquired"
    first_lease_id = first["lease_id"]

    conflict = svc.allocate_placement(
        run_id="run-replay-capability-1",
        task_id="task-replay-capability-1",
        capability="compute.alt.local",
        trace_id="trace-replay-capability-1-retry",
        tenant_id="t1",
    )
    assert conflict["outcome"] == "rejected"
    assert conflict["reason_code"] == "capability_context_conflict"
    assert conflict["resource_snapshot"]["tenant_id"] == "t1"
    assert conflict["resource_snapshot"]["tenant_active_leases"] == 1
    assert conflict["resource_snapshot"]["tenant_limit"] == 2

    active_lease_ids = [lease.lease_id for lease in svc.leases.values() if lease.status == "active"]
    assert active_lease_ids == [first_lease_id]


def test_allocate_placement_replay_index_clears_after_release() -> None:
    svc = DeviceHubService(max_active_leases_per_tenant=1)
    svc.register_device("desktop-replay-release", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-replay-release")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-replay-release")

    first = svc.allocate_placement(
        run_id="run-replay-release-1",
        task_id="task-replay-release-1",
        capability="compute.comfyui.local",
        trace_id="trace-replay-release-1",
        tenant_id="t1",
    )
    first_lease_id = first["lease_id"]
    replay_key = ("run-replay-release-1", "task-replay-release-1")
    assert svc.active_lease_index_by_run_task.get(replay_key) == first_lease_id

    replay = svc.allocate_placement(
        run_id="run-replay-release-1",
        task_id="task-replay-release-1",
        capability="compute.comfyui.local",
        trace_id="trace-replay-release-1-retry",
        tenant_id="t1",
    )
    assert replay["lease_id"] == first_lease_id
    assert replay["reason_code"] == "idempotent_replay"

    released = svc.release_lease(first_lease_id)
    assert released["outcome"] == "lease_released"
    assert replay_key not in svc.active_lease_index_by_run_task

    reacquired = svc.allocate_placement(
        run_id="run-replay-release-1",
        task_id="task-replay-release-1",
        capability="compute.comfyui.local",
        trace_id="trace-replay-release-1-new",
        tenant_id="t1",
    )
    assert reacquired["outcome"] == "lease_acquired"
    assert reacquired["lease_id"] != first_lease_id
    assert reacquired.get("reason_code") != "idempotent_replay"
    assert svc.active_lease_index_by_run_task.get(replay_key) == reacquired["lease_id"]


def test_allocate_placement_replay_index_clears_after_expire() -> None:
    svc = DeviceHubService(max_active_leases_per_tenant=1)
    svc.register_device("desktop-replay-expire", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-replay-expire")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-replay-expire")

    first = svc.allocate_placement(
        run_id="run-replay-expire-1",
        task_id="task-replay-expire-1",
        capability="compute.comfyui.local",
        trace_id="trace-replay-expire-1",
        tenant_id="t1",
    )
    first_lease_id = first["lease_id"]
    replay_key = ("run-replay-expire-1", "task-replay-expire-1")
    assert svc.active_lease_index_by_run_task.get(replay_key) == first_lease_id

    expired = svc.expire_lease(first_lease_id, reason_code="ttl_expired")
    assert expired["outcome"] == "lease_expired"
    assert replay_key not in svc.active_lease_index_by_run_task

    reacquired = svc.allocate_placement(
        run_id="run-replay-expire-1",
        task_id="task-replay-expire-1",
        capability="compute.comfyui.local",
        trace_id="trace-replay-expire-1-new",
        tenant_id="t1",
    )
    assert reacquired["outcome"] == "lease_acquired"
    assert reacquired["lease_id"] != first_lease_id
    assert reacquired.get("reason_code") != "idempotent_replay"
    assert svc.active_lease_index_by_run_task.get(replay_key) == reacquired["lease_id"]


def test_allocate_placement_replay_index_self_heals_stale_entry() -> None:
    svc = DeviceHubService(max_active_leases_per_tenant=1)
    svc.register_device("desktop-replay-heal", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-replay-heal")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-replay-heal")

    first = svc.allocate_placement(
        run_id="run-replay-heal-1",
        task_id="task-replay-heal-1",
        capability="compute.comfyui.local",
        trace_id="trace-replay-heal-1",
        tenant_id="t1",
    )
    first_lease_id = first["lease_id"]
    replay_key = ("run-replay-heal-1", "task-replay-heal-1")
    assert svc.active_lease_index_by_run_task.get(replay_key) == first_lease_id

    svc.active_lease_index_by_run_task[replay_key] = "lease-missing"
    replay = svc.allocate_placement(
        run_id="run-replay-heal-1",
        task_id="task-replay-heal-1",
        capability="compute.comfyui.local",
        trace_id="trace-replay-heal-1-retry",
        tenant_id="t1",
    )
    assert replay["outcome"] == "lease_acquired"
    assert replay["lease_id"] == first_lease_id
    assert replay["reason_code"] == "idempotent_replay"
    assert svc.active_lease_index_by_run_task.get(replay_key) == first_lease_id


def test_terminal_lease_actions_prune_stale_replay_index_entry() -> None:
    terminal_actions = ("release", "expire")
    for action in terminal_actions:
        svc = DeviceHubService(max_active_leases_per_tenant=1)
        device_id = f"desktop-replay-prune-{action}"
        run_id = f"run-replay-prune-{action}"
        task_id = f"task-replay-prune-{action}"
        svc.register_device(device_id, ["compute.comfyui.local"])
        req = svc.request_pairing(device_id)
        svc.approve_pairing(req.code)
        svc.receive_heartbeat(device_id)

        first = svc.allocate_placement(
            run_id=run_id,
            task_id=task_id,
            capability="compute.comfyui.local",
            trace_id=f"trace-replay-prune-{action}",
            tenant_id="t1",
        )
        first_lease_id = first["lease_id"]
        replay_key = (run_id, task_id)
        assert svc.active_lease_index_by_run_task.get(replay_key) == first_lease_id
        svc.active_lease_index_by_run_task[replay_key] = "lease-missing"

        if action == "release":
            released = svc.release_lease(first_lease_id)
            assert released["outcome"] == "lease_released"
        else:
            expired = svc.expire_lease(first_lease_id, reason_code="ttl_expired")
            assert expired["outcome"] == "lease_expired"

        assert replay_key not in svc.active_lease_index_by_run_task


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
    assert first["resource_snapshot"]["eligible_devices"] == 2
    assert first["resource_snapshot"]["active_leases"] == 0
    assert first["resource_snapshot"]["available_slots"] == 2
    assert first["resource_snapshot"]["tenant_id"] == "t1"
    assert first["resource_snapshot"]["tenant_active_leases"] == 0
    assert first["resource_snapshot"]["tenant_limit"] == 1

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
    assert second["resource_snapshot"]["eligible_devices"] == 2
    assert second["resource_snapshot"]["active_leases"] == 1
    assert second["resource_snapshot"]["available_slots"] == 1

    third = svc.allocate_placement(
        run_id="run-quota-3",
        task_id="task-quota-3",
        capability="compute.comfyui.local",
        trace_id="trace-quota-3",
        tenant_id="t2",
    )
    assert third["outcome"] == "lease_acquired"
    snapshot = svc.placement_capacity_snapshot()
    assert snapshot["tenant_quota"]["enabled"] is True
    assert snapshot["tenant_quota"]["max_active_leases_per_tenant"] == 1
    assert snapshot["tenant_quota"]["tenants_with_active_leases"] >= 1
    assert snapshot["tenant_quota"]["max_tenant_active_leases"] >= 1
    assert snapshot["tenant_quota"]["tenants_at_limit"] >= 1


def test_allocate_placement_tenant_quota_recovers_after_release() -> None:
    svc = DeviceHubService(max_active_leases_per_tenant=1)
    svc.register_device("desktop-quota-recover-a", ["compute.comfyui.local"])
    svc.register_device("desktop-quota-recover-b", ["compute.comfyui.local"])
    req_a = svc.request_pairing("desktop-quota-recover-a")
    req_b = svc.request_pairing("desktop-quota-recover-b")
    svc.approve_pairing(req_a.code)
    svc.approve_pairing(req_b.code)
    svc.receive_heartbeat("desktop-quota-recover-a")
    svc.receive_heartbeat("desktop-quota-recover-b")

    first = svc.allocate_placement(
        run_id="run-quota-recover-1",
        task_id="task-quota-recover-1",
        capability="compute.comfyui.local",
        trace_id="trace-quota-recover-1",
        tenant_id="t1",
    )
    assert first["outcome"] == "lease_acquired"

    second = svc.allocate_placement(
        run_id="run-quota-recover-2",
        task_id="task-quota-recover-2",
        capability="compute.comfyui.local",
        trace_id="trace-quota-recover-2",
        tenant_id="t1",
    )
    assert second["outcome"] == "rejected"
    assert second["reason_code"] == "tenant_quota_exhausted"
    assert second["resource_snapshot"]["tenant_active_leases"] == 1
    assert second["resource_snapshot"]["available_slots"] == 1

    lease_id = first["lease_id"]
    released = svc.release_lease(lease_id)
    assert released["outcome"] == "lease_released"

    snapshot_after_release = svc.placement_capacity_snapshot()
    assert snapshot_after_release["active_leases"] == 0
    assert snapshot_after_release["available_slots"] == 2
    assert snapshot_after_release["tenant_quota"]["tenants_at_limit"] == 0

    third = svc.allocate_placement(
        run_id="run-quota-recover-3",
        task_id="task-quota-recover-3",
        capability="compute.comfyui.local",
        trace_id="trace-quota-recover-3",
        tenant_id="t1",
    )
    assert third["outcome"] == "lease_acquired"
    assert third["resource_snapshot"]["tenant_active_leases"] == 0
    assert third["resource_snapshot"]["tenant_limit"] == 1


def test_allocate_placement_honors_tenant_quota_overrides_before_default() -> None:
    svc = DeviceHubService(
        max_active_leases_per_tenant=3,
        tenant_active_lease_limits={"t1": 1},
    )
    for device_id in ("desktop-quota-ovr-a", "desktop-quota-ovr-b", "desktop-quota-ovr-c"):
        svc.register_device(device_id, ["compute.comfyui.local"])
        req = svc.request_pairing(device_id)
        svc.approve_pairing(req.code)
        svc.receive_heartbeat(device_id)

    first_t1 = svc.allocate_placement(
        run_id="run-quota-ovr-t1-1",
        task_id="task-quota-ovr-t1-1",
        capability="compute.comfyui.local",
        trace_id="trace-quota-ovr-t1-1",
        tenant_id="t1",
    )
    assert first_t1["outcome"] == "lease_acquired"

    second_t1 = svc.allocate_placement(
        run_id="run-quota-ovr-t1-2",
        task_id="task-quota-ovr-t1-2",
        capability="compute.comfyui.local",
        trace_id="trace-quota-ovr-t1-2",
        tenant_id="t1",
    )
    assert second_t1["outcome"] == "rejected"
    assert second_t1["reason_code"] == "tenant_quota_exhausted"
    assert second_t1["resource_snapshot"]["tenant_limit"] == 1

    first_t2 = svc.allocate_placement(
        run_id="run-quota-ovr-t2-1",
        task_id="task-quota-ovr-t2-1",
        capability="compute.comfyui.local",
        trace_id="trace-quota-ovr-t2-1",
        tenant_id="t2",
    )
    assert first_t2["outcome"] == "lease_acquired"

    second_t2 = svc.allocate_placement(
        run_id="run-quota-ovr-t2-2",
        task_id="task-quota-ovr-t2-2",
        capability="compute.comfyui.local",
        trace_id="trace-quota-ovr-t2-2",
        tenant_id="t2",
    )
    assert second_t2["outcome"] == "lease_acquired"

    snapshot = svc.placement_capacity_snapshot()
    assert snapshot["tenant_quota"]["enabled"] is True
    assert snapshot["tenant_quota"]["max_active_leases_per_tenant"] == 3
    assert snapshot["tenant_quota"]["tenant_limit_overrides"]["t1"] == 1
    assert snapshot["tenant_quota"]["tenants_with_active_leases"] >= 2
    assert snapshot["tenant_quota"]["tenants_at_limit"] >= 1


def test_allocate_placement_supports_override_only_quota_without_default() -> None:
    svc = DeviceHubService(tenant_active_lease_limits={"vip": 2})
    for device_id in ("desktop-quota-vip-a", "desktop-quota-vip-b", "desktop-quota-vip-c"):
        svc.register_device(device_id, ["compute.comfyui.local"])
        req = svc.request_pairing(device_id)
        svc.approve_pairing(req.code)
        svc.receive_heartbeat(device_id)

    first = svc.allocate_placement(
        run_id="run-quota-vip-1",
        task_id="task-quota-vip-1",
        capability="compute.comfyui.local",
        trace_id="trace-quota-vip-1",
        tenant_id="vip",
    )
    second = svc.allocate_placement(
        run_id="run-quota-vip-2",
        task_id="task-quota-vip-2",
        capability="compute.comfyui.local",
        trace_id="trace-quota-vip-2",
        tenant_id="vip",
    )
    third = svc.allocate_placement(
        run_id="run-quota-vip-3",
        task_id="task-quota-vip-3",
        capability="compute.comfyui.local",
        trace_id="trace-quota-vip-3",
        tenant_id="vip",
    )
    assert first["outcome"] == "lease_acquired"
    assert second["outcome"] == "lease_acquired"
    assert third["outcome"] == "rejected"
    assert third["reason_code"] == "tenant_quota_exhausted"
    assert third["resource_snapshot"]["tenant_limit"] == 2

    snapshot = svc.placement_capacity_snapshot()
    assert snapshot["tenant_quota"]["enabled"] is True
    assert snapshot["tenant_quota"]["max_active_leases_per_tenant"] is None
    assert snapshot["tenant_quota"]["tenant_limit_overrides"]["vip"] == 2
    assert snapshot["tenant_quota"]["tenants_at_limit"] >= 1


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
    local_audit = local_selected["placement_audit"]
    assert local_audit["candidate_device_count"] == 1
    assert local_audit["selected_device_id"] == "desktop-local"
    assert local_audit["selected_execution_site"] == "local"
    assert local_audit["fallback_applied"] is False

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
    fallback_audit = cloud_fallback["placement_audit"]
    assert fallback_audit["candidate_device_count"] == 1
    assert fallback_audit["selected_device_id"] == "desktop-cloud"
    assert fallback_audit["selected_execution_site"] == "cloud"
    assert fallback_audit["fallback_applied"] is True
    assert fallback_audit["fallback_reason_code"] == "local_preference_fallback"
    assert fallback_audit["failure_domain"] == "execution_site"


def test_allocate_placement_policy_prefers_local_when_load_is_tied() -> None:
    svc = DeviceHubService()
    svc.register_device(
        "a-cloud-lex-first",
        ["compute.comfyui.local"],
        execution_site="cloud",
        region="us-west",
        cost_tier="balanced",
        estimated_cost_usd=0.3,
    )
    svc.register_device(
        "z-local-lex-last",
        ["compute.comfyui.local"],
        execution_site="local",
        region="us-west",
        cost_tier="balanced",
        estimated_cost_usd=0.3,
    )
    req_cloud = svc.request_pairing("a-cloud-lex-first")
    req_local = svc.request_pairing("z-local-lex-last")
    svc.approve_pairing(req_cloud.code)
    svc.approve_pairing(req_local.code)
    svc.receive_heartbeat("a-cloud-lex-first")
    svc.receive_heartbeat("z-local-lex-last")

    selected = svc.allocate_placement(
        run_id="run-policy-local-1",
        task_id="task-policy-local-1",
        capability="compute.comfyui.local",
        trace_id="trace-policy-local-1",
    )
    assert selected["outcome"] == "lease_acquired"
    assert selected["device_id"] == "z-local-lex-last"
    assert selected["resource_snapshot"]["queue_depth"] == 0
    assert selected["placement_audit"]["selected_execution_site"] == "local"


def test_allocate_placement_policy_prefers_lower_cost_when_load_is_tied() -> None:
    svc = DeviceHubService()
    svc.register_device(
        "a-expensive-lex-first",
        ["compute.comfyui.local"],
        execution_site="local",
        region="us-west",
        cost_tier="balanced",
        estimated_cost_usd=1.0,
    )
    svc.register_device(
        "z-cheap-lex-last",
        ["compute.comfyui.local"],
        execution_site="local",
        region="us-west",
        cost_tier="balanced",
        estimated_cost_usd=0.1,
    )
    req_expensive = svc.request_pairing("a-expensive-lex-first")
    req_cheap = svc.request_pairing("z-cheap-lex-last")
    svc.approve_pairing(req_expensive.code)
    svc.approve_pairing(req_cheap.code)
    svc.receive_heartbeat("a-expensive-lex-first")
    svc.receive_heartbeat("z-cheap-lex-last")

    selected = svc.allocate_placement(
        run_id="run-policy-cost-1",
        task_id="task-policy-cost-1",
        capability="compute.comfyui.local",
        trace_id="trace-policy-cost-1",
    )
    assert selected["outcome"] == "lease_acquired"
    assert selected["device_id"] == "z-cheap-lex-last"
    assert selected["resource_snapshot"]["queue_depth"] == 0
    assert selected["placement_audit"]["selected_device_id"] == "z-cheap-lex-last"


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
    assert region_miss["resource_snapshot"]["eligible_devices"] == 1
    assert region_miss["resource_snapshot"]["active_leases"] == 0
    assert region_miss["resource_snapshot"]["available_slots"] == 1

    cost_miss = svc.allocate_placement(
        run_id="run-cost-miss",
        task_id="task-cost-miss",
        capability="compute.comfyui.local",
        trace_id="trace-cost-miss",
        placement_constraints={"max_cost_usd_hard": 0.5},
    )
    assert cost_miss["outcome"] == "rejected"
    assert cost_miss["reason_code"] == "cost_limit_exceeded"
    assert cost_miss["resource_snapshot"]["eligible_devices"] == 1
    assert cost_miss["resource_snapshot"]["active_leases"] == 0
    assert cost_miss["resource_snapshot"]["available_slots"] == 1


def test_allocate_placement_fallbacks_to_alternate_node_pool_when_requested_pool_missing() -> None:
    svc = DeviceHubService()
    svc.register_device(
        "gpu-node-pool-b",
        ["compute.comfyui.local"],
        execution_site="cloud",
        region="us-west",
        cost_tier="balanced",
        node_pool="pool-b",
    )
    req = svc.request_pairing("gpu-node-pool-b")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("gpu-node-pool-b")

    decision = svc.allocate_placement(
        run_id="run-nodepool-fallback",
        task_id="task-nodepool-fallback",
        capability="compute.comfyui.local",
        trace_id="trace-nodepool-fallback",
        placement_constraints={
            "region": "us-west",
            "node_pool": "pool-a",
        },
    )
    assert decision["outcome"] == "lease_acquired"
    assert decision["device_id"] == "gpu-node-pool-b"
    assert decision["reason_code"] == "node_pool_fallback"
    assert "alternate node_pool" in decision["reason"]
    audit = decision["placement_audit"]
    assert audit["candidate_device_count"] == 1
    assert audit["selected_device_id"] == "gpu-node-pool-b"
    assert audit["selected_execution_site"] == "cloud"
    assert audit["selected_node_pool"] == "pool-b"
    assert audit["fallback_applied"] is True
    assert audit["fallback_reason_code"] == "node_pool_fallback"
    assert audit["failure_domain"] == "node_pool"


def test_allocate_placement_rejects_when_required_capabilities_not_fully_satisfied() -> None:
    svc = DeviceHubService()
    svc.register_device(
        "gpu-required-gap",
        ["compute.comfyui.local", "model.sd15"],
        execution_site="cloud",
        region="us-west",
        cost_tier="balanced",
    )
    req = svc.request_pairing("gpu-required-gap")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("gpu-required-gap")

    rejected = svc.allocate_placement(
        run_id="run-required-gap",
        task_id="task-required-gap",
        capability="compute.comfyui.local",
        trace_id="trace-required-gap",
        placement_constraints={
            "required_capabilities": ["compute.comfyui.local", "model.sdxl"],
        },
    )
    assert rejected["outcome"] == "rejected"
    assert rejected["reason_code"] == "required_capabilities_unavailable"
    assert rejected["resource_snapshot"]["eligible_devices"] == 1
    assert rejected["resource_snapshot"]["available_slots"] == 1


def test_allocate_placement_respects_avoid_capabilities_filter() -> None:
    svc = DeviceHubService()
    svc.register_device(
        "gpu-avoid-bad",
        ["compute.comfyui.local", "blocked.maintenance"],
        execution_site="cloud",
        region="us-west",
        cost_tier="balanced",
    )
    svc.register_device(
        "gpu-avoid-good",
        ["compute.comfyui.local", "model.sd15"],
        execution_site="cloud",
        region="us-west",
        cost_tier="balanced",
    )
    req_bad = svc.request_pairing("gpu-avoid-bad")
    req_good = svc.request_pairing("gpu-avoid-good")
    svc.approve_pairing(req_bad.code)
    svc.approve_pairing(req_good.code)
    svc.receive_heartbeat("gpu-avoid-bad")
    svc.receive_heartbeat("gpu-avoid-good")

    selected = svc.allocate_placement(
        run_id="run-avoid-1",
        task_id="task-avoid-1",
        capability="compute.comfyui.local",
        trace_id="trace-avoid-1",
        placement_constraints={"avoid_capabilities": ["blocked.maintenance"]},
    )
    assert selected["outcome"] == "lease_acquired"
    assert selected["device_id"] == "gpu-avoid-good"

    rejected = svc.allocate_placement(
        run_id="run-avoid-2",
        task_id="task-avoid-2",
        capability="compute.comfyui.local",
        trace_id="trace-avoid-2",
        placement_constraints={"avoid_capabilities": ["model.sd15", "blocked.maintenance"]},
    )
    assert rejected["outcome"] == "rejected"
    assert rejected["reason_code"] == "avoid_capabilities_excluded"
    assert rejected["resource_snapshot"]["eligible_devices"] == 2
    assert rejected["resource_snapshot"]["active_leases"] == 1
    assert rejected["resource_snapshot"]["available_slots"] == 1


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
    assert snapshot["tenant_quota"]["enabled"] is False
    assert snapshot["tenant_quota"]["max_active_leases_per_tenant"] is None
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


def test_capacity_snapshot_expires_active_lease_with_invalid_expiry_timestamp() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-invalid-expiry", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-invalid-expiry")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-invalid-expiry")

    allocated = svc.allocate_placement(
        run_id="run-invalid-expiry",
        task_id="task-invalid-expiry",
        capability="compute.comfyui.local",
        trace_id="trace-invalid-expiry",
    )
    lease_id = allocated["lease_id"]
    lease = svc.leases[lease_id]
    lease.lease_expires_at = "invalid-datetime"

    snapshot = svc.placement_capacity_snapshot()

    assert snapshot["active_leases"] == 0
    assert snapshot["lease_status_counts"]["active"] == 0
    assert snapshot["lease_status_counts"]["expired"] >= 1
    assert svc.leases[lease_id].status == "expired"
    assert svc.leases[lease_id].expire_reason_code == "ttl_expired"


def test_allocate_placement_recovers_capacity_when_existing_lease_expiry_is_invalid() -> None:
    svc = DeviceHubService()
    svc.register_device("desktop-invalid-expiry-recover", ["compute.comfyui.local"])
    req = svc.request_pairing("desktop-invalid-expiry-recover")
    svc.approve_pairing(req.code)
    svc.receive_heartbeat("desktop-invalid-expiry-recover")

    first = svc.allocate_placement(
        run_id="run-invalid-expiry-recover-1",
        task_id="task-invalid-expiry-recover-1",
        capability="compute.comfyui.local",
        trace_id="trace-invalid-expiry-recover-1",
    )
    assert first["outcome"] == "lease_acquired"
    first_lease_id = first["lease_id"]
    svc.leases[first_lease_id].lease_expires_at = "invalid-datetime"

    second = svc.allocate_placement(
        run_id="run-invalid-expiry-recover-2",
        task_id="task-invalid-expiry-recover-2",
        capability="compute.comfyui.local",
        trace_id="trace-invalid-expiry-recover-2",
    )

    assert second["outcome"] == "lease_acquired"
    assert second["lease_id"] != first_lease_id
    assert svc.leases[first_lease_id].status == "expired"


def test_concurrent_capacity_retries_recover_after_multiple_invalid_expiry_leases() -> None:
    svc = DeviceHubService()
    for device_id in (
        "desktop-concurrent-invalid-expiry-recover-a",
        "desktop-concurrent-invalid-expiry-recover-b",
    ):
        svc.register_device(device_id, ["compute.comfyui.local"])
        req = svc.request_pairing(device_id)
        svc.approve_pairing(req.code)
        svc.receive_heartbeat(device_id)

    first = svc.allocate_placement(
        run_id="run-concurrent-invalid-expiry-recover-1",
        task_id="task-concurrent-invalid-expiry-recover-1",
        capability="compute.comfyui.local",
        trace_id="trace-concurrent-invalid-expiry-recover-1",
    )
    second = svc.allocate_placement(
        run_id="run-concurrent-invalid-expiry-recover-2",
        task_id="task-concurrent-invalid-expiry-recover-2",
        capability="compute.comfyui.local",
        trace_id="trace-concurrent-invalid-expiry-recover-2",
    )
    assert first["outcome"] == "lease_acquired"
    assert second["outcome"] == "lease_acquired"
    first_lease_id = first["lease_id"]
    second_lease_id = second["lease_id"]
    assert first_lease_id != second_lease_id

    third_rejected = svc.allocate_placement(
        run_id="run-concurrent-invalid-expiry-recover-3",
        task_id="task-concurrent-invalid-expiry-recover-3",
        capability="compute.comfyui.local",
        trace_id="trace-concurrent-invalid-expiry-recover-3",
    )
    fourth_rejected = svc.allocate_placement(
        run_id="run-concurrent-invalid-expiry-recover-4",
        task_id="task-concurrent-invalid-expiry-recover-4",
        capability="compute.comfyui.local",
        trace_id="trace-concurrent-invalid-expiry-recover-4",
    )
    assert third_rejected["outcome"] == "rejected"
    assert fourth_rejected["outcome"] == "rejected"
    assert third_rejected["reason_code"] == "capacity_exhausted"
    assert fourth_rejected["reason_code"] == "capacity_exhausted"

    svc.leases[first_lease_id].lease_expires_at = "invalid-datetime"
    svc.leases[second_lease_id].lease_expires_at = "invalid-datetime"

    recover_a = svc.allocate_placement(
        run_id="run-concurrent-invalid-expiry-recover-5",
        task_id="task-concurrent-invalid-expiry-recover-5",
        capability="compute.comfyui.local",
        trace_id="trace-concurrent-invalid-expiry-recover-5",
    )
    recover_b = svc.allocate_placement(
        run_id="run-concurrent-invalid-expiry-recover-6",
        task_id="task-concurrent-invalid-expiry-recover-6",
        capability="compute.comfyui.local",
        trace_id="trace-concurrent-invalid-expiry-recover-6",
    )
    assert recover_a["outcome"] == "lease_acquired"
    assert recover_b["outcome"] == "lease_acquired"
    assert recover_a["lease_id"] not in {first_lease_id, second_lease_id}
    assert recover_b["lease_id"] not in {first_lease_id, second_lease_id}
    assert recover_a["lease_id"] != recover_b["lease_id"]

    for lease_id in (first_lease_id, second_lease_id):
        assert svc.leases[lease_id].status == "expired"
        assert svc.leases[lease_id].expire_reason_code == "ttl_expired"

    snapshot = svc.placement_capacity_snapshot()
    assert snapshot["active_leases"] == 2
    assert snapshot["lease_status_counts"]["expired"] >= 2
