from __future__ import annotations

from pathlib import Path

from device_hub.service import DeviceHubService


def _prepare_active_lease(service: DeviceHubService) -> dict[str, str]:
    service.register_device("desktop-persist-1", ["compute.comfyui.local"])
    request = service.request_pairing("desktop-persist-1")
    service.approve_pairing(request.code)
    service.receive_heartbeat("desktop-persist-1")
    decision = service.allocate_placement(
        run_id="run-persist-1",
        task_id="task-persist-1",
        capability="compute.comfyui.local",
        trace_id="trace-persist-1",
    )
    assert decision["outcome"] == "lease_acquired"
    return decision


def test_device_hub_restores_active_lease_after_restart(tmp_path: Path) -> None:
    db_path = tmp_path / "device-hub-state.sqlite"
    first = DeviceHubService(persistence_db_path=db_path.as_posix())
    first_decision = _prepare_active_lease(first)
    first_lease_id = first_decision["lease_id"]

    restarted = DeviceHubService(persistence_db_path=db_path.as_posix())
    assert "desktop-persist-1" in restarted.registry.devices
    assert first_lease_id in restarted.leases
    restored_lease = restarted.leases[first_lease_id]
    assert restored_lease.status == "active"
    assert restarted.active_lease_index_by_run_task.get(("run-persist-1", "task-persist-1")) == first_lease_id

    replay = restarted.allocate_placement(
        run_id="run-persist-1",
        task_id="task-persist-1",
        capability="compute.comfyui.local",
        trace_id="trace-persist-1-retry",
    )
    assert replay["outcome"] == "lease_acquired"
    assert replay["lease_id"] == first_lease_id
    assert replay["reason_code"] == "idempotent_replay"


def test_device_hub_persists_terminal_lease_state_and_index_cleanup(tmp_path: Path) -> None:
    db_path = tmp_path / "device-hub-state.sqlite"
    first = DeviceHubService(persistence_db_path=db_path.as_posix())
    first_decision = _prepare_active_lease(first)
    lease_id = first_decision["lease_id"]
    released = first.release_lease(lease_id)
    assert released["outcome"] == "lease_released"
    assert ("run-persist-1", "task-persist-1") not in first.active_lease_index_by_run_task

    restarted = DeviceHubService(persistence_db_path=db_path.as_posix())
    snapshot = restarted.get_lease_snapshot(lease_id)
    assert snapshot["status"] == "released"
    assert snapshot["released_at"] is not None
    assert ("run-persist-1", "task-persist-1") not in restarted.active_lease_index_by_run_task
