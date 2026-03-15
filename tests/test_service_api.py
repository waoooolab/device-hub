from __future__ import annotations

import base64
import hashlib
import hmac
import importlib
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient

from device_hub.service import DeviceHubService

app_module = importlib.import_module("device_hub.service_api.app")


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _issue_token(claims: dict) -> str:
    now = int(time.time())
    merged = dict(claims)
    merged.setdefault("iat", now)
    merged.setdefault("exp", now + 300)
    merged.setdefault("jti", str(uuid4()))

    payload = _b64url_encode(json.dumps(merged, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    secret = os.environ.get("RUNTIME_GATEWAY_TOKEN_SECRET", "dev-insecure-secret").encode("utf-8")
    signature = _b64url_encode(hmac.new(secret, payload.encode("utf-8"), hashlib.sha256).digest())
    return f"{payload}.{signature}"


def _command_envelope(payload: dict, command_type: str = "device.command") -> dict:
    return {
        "command_id": f"cmd-{uuid4()}",
        "command_type": command_type,
        "tenant_id": "t1",
        "app_id": "waoooo",
        "session_key": "tenant:t1:app:waoooo:channel:web:actor:u1:thread:main:agent:pm",
        "trace_id": "trace-device-1",
        "idempotency_key": "idem-device-1234",
        "retry_policy": {
            "max_attempts": 3,
            "backoff_ms": 100,
            "strategy": "fixed",
        },
        "ts": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


def _token(scope: list[str], audience: str = "device-hub") -> str:
    return _issue_token(
        {
            "iss": "runtime-gateway",
            "sub": "svc:runtime-gateway",
            "aud": audience,
            "tenant_id": "t1",
            "app_id": "waoooo",
            "scope": scope,
            "token_use": "service",
            "trace_id": "trace-device-1",
            "session_key": "tenant:t1:app:waoooo:channel:web:actor:u1:thread:main:agent:pm",
        }
    )


def _setup_test_env() -> TestClient:
    os.environ["WAOOOOLAB_PLATFORM_CONTRACTS_DIR"] = str(
        Path(__file__).resolve().parent / "fixtures" / "contracts"
    )
    app_module._hub = DeviceHubService()
    return TestClient(app_module.app)


def test_tenant_active_lease_limits_from_env_parses_valid_json(monkeypatch) -> None:
    monkeypatch.setenv(
        "WAOOOOLAB_DEVICE_HUB_TENANT_ACTIVE_LEASE_LIMITS",
        '{"t1": 1, "enterprise": 5}',
    )
    parsed = app_module._tenant_active_lease_limits_from_env()
    assert parsed == {"t1": 1, "enterprise": 5}


def test_tenant_active_lease_limits_from_env_rejects_invalid_json(monkeypatch) -> None:
    monkeypatch.setenv(
        "WAOOOOLAB_DEVICE_HUB_TENANT_ACTIVE_LEASE_LIMITS",
        '{"t1": ',
    )
    try:
        app_module._tenant_active_lease_limits_from_env()
    except RuntimeError as exc:
        assert "must be valid JSON object" in str(exc)
    else:
        raise AssertionError("expected RuntimeError for invalid tenant limits JSON")


def test_tenant_active_lease_limits_from_env_rejects_non_positive_limit(monkeypatch) -> None:
    monkeypatch.setenv(
        "WAOOOOLAB_DEVICE_HUB_TENANT_ACTIVE_LEASE_LIMITS",
        '{"t1": 0}',
    )
    try:
        app_module._tenant_active_lease_limits_from_env()
    except RuntimeError as exc:
        assert "limits must be positive integers" in str(exc)
    else:
        raise AssertionError("expected RuntimeError for non-positive tenant limit")


def test_register_requires_token() -> None:
    client = _setup_test_env()
    response = client.post(
        "/v1/devices/register",
        json=_command_envelope({"device_id": "d1", "capabilities": ["compute.comfyui.local"]}),
    )
    assert response.status_code == 401


def test_register_rejects_missing_token_use_claim() -> None:
    client = _setup_test_env()
    issued = _issue_token(
        {
            "iss": "runtime-gateway",
            "sub": "svc:runtime-gateway",
            "aud": "device-hub",
            "tenant_id": "t1",
            "app_id": "waoooo",
            "scope": ["devices:write"],
            "trace_id": "trace-device-1",
            "session_key": "tenant:t1:app:waoooo:channel:web:actor:u1:thread:main:agent:pm",
        }
    )
    response = client.post(
        "/v1/devices/register",
        json=_command_envelope({"device_id": "d1", "capabilities": ["compute.comfyui.local"]}),
        headers={"Authorization": f"Bearer {issued}"},
    )
    assert response.status_code == 401
    assert "missing token_use" in response.json()["detail"]


def test_register_rejects_unsupported_token_use_claim() -> None:
    client = _setup_test_env()
    issued = _issue_token(
        {
            "iss": "runtime-gateway",
            "sub": "svc:runtime-gateway",
            "aud": "device-hub",
            "tenant_id": "t1",
            "app_id": "waoooo",
            "scope": ["devices:write"],
            "token_use": "exchange",
            "trace_id": "trace-device-1",
            "session_key": "tenant:t1:app:waoooo:channel:web:actor:u1:thread:main:agent:pm",
        }
    )
    response = client.post(
        "/v1/devices/register",
        json=_command_envelope({"device_id": "d1", "capabilities": ["compute.comfyui.local"]}),
        headers={"Authorization": f"Bearer {issued}"},
    )
    assert response.status_code == 401
    assert "unsupported token_use" in response.json()["detail"]


def test_register_rejects_default_secret_in_strict_mode(monkeypatch) -> None:
    monkeypatch.setenv("WAOOOOLAB_STRICT_TOKEN_SECRET", "true")
    monkeypatch.delenv("RUNTIME_GATEWAY_TOKEN_SECRET", raising=False)
    client = _setup_test_env()
    issued = _token(["devices:write"])
    response = client.post(
        "/v1/devices/register",
        json=_command_envelope({"device_id": "d1", "capabilities": ["compute.comfyui.local"]}),
        headers={"Authorization": f"Bearer {issued}"},
    )
    assert response.status_code == 401
    assert "insecure default token secret" in response.json()["detail"]


def test_register_and_pair_and_heartbeat_flow() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    register = client.post(
        "/v1/devices/register",
        json=_command_envelope({"device_id": "desktop-1", "capabilities": ["compute.comfyui.local"]}),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert register.status_code == 200
    assert register.json()["event_type"] == "device.registered"

    request_pair = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "desktop-1"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert request_pair.status_code == 200
    code = request_pair.json()["payload"]["code"]

    approve = client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert approve.status_code == 200
    assert approve.json()["payload"]["status"] == "paired"

    heartbeat = client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "desktop-1"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert heartbeat.status_code == 200
    assert heartbeat.json()["payload"]["status"] == "online"


def test_register_device_accepts_runtime_metadata_fields() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    register = client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {
                "device_id": "gpu-node-meta",
                "capabilities": ["compute.comfyui.local"],
                "execution_site": "cloud",
                "region": "us-west",
                "cost_tier": "low",
                "node_pool": "spot-a",
                "estimated_cost_usd": 0.42,
            }
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert register.status_code == 200
    payload = register.json()["payload"]
    assert payload["execution_site"] == "cloud"
    assert payload["region"] == "us-west"
    assert payload["cost_tier"] == "low"
    assert payload["node_pool"] == "spot-a"
    assert float(payload["estimated_cost_usd"]) == 0.42


def test_presence_refresh_marks_device_offline() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope({"device_id": "mobile-1", "capabilities": ["camera.capture"]}),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "mobile-1"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "mobile-1"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    old = (datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat()
    app_module._hub.registry.devices["mobile-1"].last_seen_at = old

    refresh = client.post(
        "/v1/devices/presence/refresh",
        json=_command_envelope({"timeout_seconds": 30}),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert refresh.status_code == 200
    assert refresh.json()["payload"]["updated_count"] >= 1


def test_route_command_selects_low_load_device() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    for did in ["desktop-a", "desktop-b"]:
        client.post(
            "/v1/devices/register",
            json=_command_envelope({"device_id": did, "capabilities": ["compute.comfyui.local"]}),
            headers={"Authorization": f"Bearer {token}"},
        )
        pair_req = client.post(
            "/v1/devices/pairing/request",
            json=_command_envelope({"device_id": did}),
            headers={"Authorization": f"Bearer {token}"},
        )
        code = pair_req.json()["payload"]["code"]
        client.post(
            "/v1/devices/pairing/approve",
            json=_command_envelope({"code": code}),
            headers={"Authorization": f"Bearer {token}"},
        )
        client.post(
            "/v1/devices/heartbeat",
            json=_command_envelope({"device_id": did}),
            headers={"Authorization": f"Bearer {token}"},
        )

    route_envelope = _command_envelope(
        {
            "run_id": "run-route-1",
            "task_id": "task-route-1",
            "capability": "compute.comfyui.local",
            "command_type": "tool.exec",
            "command_payload": {"x": 1},
            "load_by_device": {"desktop-a": 5, "desktop-b": 1},
        }
    )
    route = client.post(
        "/v1/devices/route",
        json=route_envelope,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert route.status_code == 200
    event = route.json()
    assert event["event_type"] == "device.route.selected"
    assert event["payload"]["run_id"] == "run-route-1"
    assert event["payload"]["task_id"] == "task-route-1"
    assert event["payload"]["placement_request_id"] == route_envelope["command_id"]
    decision = event["payload"]["decision"]
    assert decision["outcome"] == "selected"
    assert decision["device_id"] == "desktop-b"
    assert decision["capability_match"] == ["compute.comfyui.local"]
    snapshot = decision["resource_snapshot"]
    assert snapshot["queue_depth"] == 1
    assert snapshot["eligible_devices"] == 2


def test_route_command_returns_route_rejected_with_structured_decision() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])
    route_envelope = _command_envelope(
        {
            "capability": "compute.comfyui.local",
            "command_type": "tool.exec",
            "command_payload": {"x": 1},
        }
    )
    response = client.post(
        "/v1/devices/route",
        json=route_envelope,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    event = response.json()
    assert event["event_type"] == "device.route.rejected"
    assert event["payload"]["run_id"] == f"route:{route_envelope['command_id']}"
    assert event["payload"]["task_id"] == f"route:{route_envelope['command_id']}:root"
    assert event["payload"]["placement_request_id"] == route_envelope["command_id"]
    decision = event["payload"]["decision"]
    assert decision["outcome"] == "rejected"
    assert decision["reason_code"] == "no_eligible_device"
    snapshot = decision["resource_snapshot"]
    assert snapshot["eligible_devices"] == 0
    assert snapshot["available_slots"] == 0


def test_allocate_placement_returns_lease_acquired_event() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {
                "device_id": "gpu-node-1",
                "capabilities": ["compute.comfyui.local"],
                "region": "us-west",
            }
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-1"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-1"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    response = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-alloc-1",
                "task_id": "task-alloc-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "region": "us-west",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
                "load_by_device": {"gpu-node-1": 1},
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    event = response.json()
    assert event["event_type"] == "device.lease.acquired"
    assert event["payload"]["run_id"] == "run-alloc-1"
    assert event["payload"]["task_id"] == "task-alloc-1"
    assert event["payload"]["decision"]["outcome"] == "lease_acquired"
    assert event["payload"]["decision"]["device_id"] == "gpu-node-1"
    assert isinstance(event["payload"]["decision"]["lease_id"], str)
    assert isinstance(event["payload"]["decision"]["lease_expires_at"], str)
    snapshot = event["payload"]["decision"]["resource_snapshot"]
    assert snapshot["queue_depth"] == 1
    assert snapshot["eligible_devices"] == 1
    assert snapshot["active_leases"] == 0
    assert snapshot["available_slots"] == 1
    assert snapshot["tenant_id"] == "t1"
    assert snapshot["tenant_active_leases"] == 0


def test_allocate_placement_prefers_local_then_fallbacks_to_cloud_with_trace_fields() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    for payload in (
        {
            "device_id": "gpu-local-pref",
            "capabilities": ["compute.comfyui.local"],
            "execution_site": "local",
            "region": "us-west",
            "cost_tier": "balanced",
            "node_pool": "local-main",
            "estimated_cost_usd": 0.0,
        },
        {
            "device_id": "gpu-cloud-pref",
            "capabilities": ["compute.comfyui.local"],
            "execution_site": "cloud",
            "region": "us-west",
            "cost_tier": "balanced",
            "node_pool": "cloud-main",
            "estimated_cost_usd": 0.9,
        },
    ):
        client.post(
            "/v1/devices/register",
            json=_command_envelope(payload),
            headers={"Authorization": f"Bearer {token}"},
        )
        pair_req = client.post(
            "/v1/devices/pairing/request",
            json=_command_envelope({"device_id": payload["device_id"]}),
            headers={"Authorization": f"Bearer {token}"},
        )
        code = pair_req.json()["payload"]["code"]
        client.post(
            "/v1/devices/pairing/approve",
            json=_command_envelope({"code": code}),
            headers={"Authorization": f"Bearer {token}"},
        )
        client.post(
            "/v1/devices/heartbeat",
            json=_command_envelope({"device_id": payload["device_id"]}),
            headers={"Authorization": f"Bearer {token}"},
        )

    first = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-pref-local-api-1",
                "task_id": "task-pref-local-api-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "region": "us-west",
                        "prefer_local": True,
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
                "load_by_device": {"gpu-local-pref": 2, "gpu-cloud-pref": 1},
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert first.status_code == 200
    first_decision = first.json()["payload"]["decision"]
    assert first_decision["outcome"] == "lease_acquired"
    assert first_decision["device_id"] == "gpu-local-pref"
    assert "score" in first_decision
    assert first_decision["resource_snapshot"]["queue_depth"] == 2

    second = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-pref-local-api-2",
                "task_id": "task-pref-local-api-2",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t2",
                        "region": "us-west",
                        "prefer_local": True,
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
                "load_by_device": {"gpu-local-pref": 9, "gpu-cloud-pref": 1},
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert second.status_code == 200
    second_decision = second.json()["payload"]["decision"]
    assert second_decision["outcome"] == "lease_acquired"
    assert second_decision["device_id"] == "gpu-cloud-pref"
    assert second_decision["reason_code"] == "local_preference_fallback"
    assert "fallback" in second_decision["reason"]
    assert "score" in second_decision
    assert second_decision["resource_snapshot"]["queue_depth"] == 1


def test_allocate_placement_returns_route_rejected_event_when_no_device() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    response = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-alloc-2",
                "task_id": "task-alloc-2",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "region": "us-west",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    event = response.json()
    assert event["event_type"] == "device.route.rejected"
    assert event["payload"]["decision"]["outcome"] == "rejected"
    assert event["payload"]["decision"]["reason_code"] == "no_eligible_device"
    assert "no eligible device" in event["payload"]["decision"]["reason"]
    snapshot = event["payload"]["decision"]["resource_snapshot"]
    assert snapshot["eligible_devices"] == 0
    assert snapshot["active_leases"] == 0
    assert snapshot["available_slots"] == 0
    assert snapshot["tenant_id"] == "t1"
    assert snapshot["tenant_active_leases"] == 0


def test_allocate_placement_returns_route_rejected_event_when_capacity_exhausted() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {
                "device_id": "gpu-node-capacity-reject",
                "capabilities": ["compute.comfyui.local"],
            }
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-capacity-reject"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-capacity-reject"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    first = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-capacity-reject-1",
                "task_id": "task-capacity-reject-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert first.status_code == 200
    assert first.json()["event_type"] == "device.lease.acquired"

    second = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-capacity-reject-2",
                "task_id": "task-capacity-reject-2",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert second.status_code == 200
    event = second.json()
    assert event["event_type"] == "device.route.rejected"
    assert event["payload"]["decision"]["reason_code"] == "capacity_exhausted"
    snapshot = event["payload"]["decision"]["resource_snapshot"]
    assert snapshot["eligible_devices"] == 1
    assert snapshot["active_leases"] == 1
    assert snapshot["available_slots"] == 0
    assert snapshot["tenant_id"] == "t1"
    assert snapshot["tenant_active_leases"] == 1


def test_allocate_placement_returns_route_rejected_event_when_selector_unavailable(
    monkeypatch,
) -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {
                "device_id": "gpu-node-route-unavailable",
                "capabilities": ["compute.comfyui.local"],
            }
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-route-unavailable"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-route-unavailable"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    monkeypatch.setattr(
        "device_hub.service.choose_device",
        lambda _candidate_ids, load_by_device=None: None,
    )

    response = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-route-unavailable-1",
                "task_id": "task-route-unavailable-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    event = response.json()
    assert event["event_type"] == "device.route.rejected"
    assert event["payload"]["decision"]["reason_code"] == "route_unavailable"
    assert "unable to select device" in event["payload"]["decision"]["reason"]
    snapshot = event["payload"]["decision"]["resource_snapshot"]
    assert snapshot["eligible_devices"] == 1
    assert snapshot["active_leases"] == 0
    assert snapshot["available_slots"] == 1
    assert snapshot["tenant_id"] == "t1"
    assert snapshot["tenant_active_leases"] == 0


def test_allocate_placement_recovers_concurrent_capacity_retries_after_invalid_expiry_reconciliation() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    for device_id in (
        "gpu-node-concurrent-invalid-expiry-a",
        "gpu-node-concurrent-invalid-expiry-b",
    ):
        client.post(
            "/v1/devices/register",
            json=_command_envelope(
                {
                    "device_id": device_id,
                    "capabilities": ["compute.comfyui.local"],
                }
            ),
            headers={"Authorization": f"Bearer {token}"},
        )
        pair_req = client.post(
            "/v1/devices/pairing/request",
            json=_command_envelope({"device_id": device_id}),
            headers={"Authorization": f"Bearer {token}"},
        )
        code = pair_req.json()["payload"]["code"]
        client.post(
            "/v1/devices/pairing/approve",
            json=_command_envelope({"code": code}),
            headers={"Authorization": f"Bearer {token}"},
        )
        client.post(
            "/v1/devices/heartbeat",
            json=_command_envelope({"device_id": device_id}),
            headers={"Authorization": f"Bearer {token}"},
        )

    def _allocate(run_suffix: str):
        return client.post(
            "/v1/placements/allocate",
            json=_command_envelope(
                {
                    "run_id": f"run-concurrent-invalid-expiry-{run_suffix}",
                    "task_id": f"task-concurrent-invalid-expiry-{run_suffix}",
                    "execution_profile": {
                        "execution_mode": "compute",
                        "inference_target": "none",
                        "resource_class": "gpu",
                        "placement_constraints": {
                            "tenant_id": "t1",
                            "required_capabilities": ["compute.comfyui.local"],
                        },
                    },
                },
                command_type="device.placement.allocate",
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    first = _allocate("1")
    second = _allocate("2")
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["event_type"] == "device.lease.acquired"
    assert second.json()["event_type"] == "device.lease.acquired"
    first_lease_id = first.json()["payload"]["decision"]["lease_id"]
    second_lease_id = second.json()["payload"]["decision"]["lease_id"]
    assert first_lease_id != second_lease_id

    third_rejected = _allocate("3")
    fourth_rejected = _allocate("4")
    assert third_rejected.status_code == 200
    assert fourth_rejected.status_code == 200
    assert third_rejected.json()["event_type"] == "device.route.rejected"
    assert fourth_rejected.json()["event_type"] == "device.route.rejected"
    assert third_rejected.json()["payload"]["decision"]["reason_code"] == "capacity_exhausted"
    assert fourth_rejected.json()["payload"]["decision"]["reason_code"] == "capacity_exhausted"

    app_module._hub.leases[first_lease_id].lease_expires_at = "invalid-datetime"
    app_module._hub.leases[second_lease_id].lease_expires_at = "invalid-datetime"

    recovered_a = _allocate("5")
    recovered_b = _allocate("6")
    assert recovered_a.status_code == 200
    assert recovered_b.status_code == 200
    assert recovered_a.json()["event_type"] == "device.lease.acquired"
    assert recovered_b.json()["event_type"] == "device.lease.acquired"
    recovered_lease_a = recovered_a.json()["payload"]["decision"]["lease_id"]
    recovered_lease_b = recovered_b.json()["payload"]["decision"]["lease_id"]
    assert recovered_lease_a not in {first_lease_id, second_lease_id}
    assert recovered_lease_b not in {first_lease_id, second_lease_id}
    assert recovered_lease_a != recovered_lease_b

    for lease_id in (first_lease_id, second_lease_id):
        lease = app_module._hub.leases[lease_id]
        assert lease.status == "expired"
        assert lease.expire_reason_code == "ttl_expired"

    capacity = client.get(
        "/v1/placements/capacity",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert capacity.status_code == 200
    payload = capacity.json()
    assert payload["active_leases"] == 2
    assert payload["lease_status_counts"]["expired"] >= 2


def test_allocate_placement_returns_route_rejected_event_when_tenant_quota_exhausted() -> None:
    client = _setup_test_env()
    app_module._hub = DeviceHubService(max_active_leases_per_tenant=1)
    token = _token(["devices:write", "devices:read"])

    for device_id in ("gpu-node-quota-a", "gpu-node-quota-b"):
        client.post(
            "/v1/devices/register",
            json=_command_envelope(
                {
                    "device_id": device_id,
                    "capabilities": ["compute.comfyui.local"],
                }
            ),
            headers={"Authorization": f"Bearer {token}"},
        )
        pair_req = client.post(
            "/v1/devices/pairing/request",
            json=_command_envelope({"device_id": device_id}),
            headers={"Authorization": f"Bearer {token}"},
        )
        code = pair_req.json()["payload"]["code"]
        client.post(
            "/v1/devices/pairing/approve",
            json=_command_envelope({"code": code}),
            headers={"Authorization": f"Bearer {token}"},
        )
        client.post(
            "/v1/devices/heartbeat",
            json=_command_envelope({"device_id": device_id}),
            headers={"Authorization": f"Bearer {token}"},
        )

    first = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-quota-reject-1",
                "task_id": "task-quota-reject-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert first.status_code == 200
    assert first.json()["event_type"] == "device.lease.acquired"

    second = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-quota-reject-2",
                "task_id": "task-quota-reject-2",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert second.status_code == 200
    event = second.json()
    assert event["event_type"] == "device.route.rejected"
    assert event["payload"]["decision"]["reason_code"] == "tenant_quota_exhausted"
    snapshot = event["payload"]["decision"]["resource_snapshot"]
    assert snapshot["tenant_id"] == "t1"
    assert snapshot["tenant_active_leases"] == 1
    assert snapshot["tenant_limit"] == 1
    assert snapshot["eligible_devices"] == 2
    assert snapshot["active_leases"] == 1
    assert snapshot["available_slots"] == 1


def test_allocate_placement_replays_existing_active_lease_for_duplicate_run_task() -> None:
    client = _setup_test_env()
    app_module._hub = DeviceHubService(max_active_leases_per_tenant=1)
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {
                "device_id": "gpu-node-replay",
                "capabilities": ["compute.comfyui.local"],
            }
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-replay"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-replay"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate_payload = {
        "run_id": "run-api-replay-1",
        "task_id": "task-api-replay-1",
        "execution_profile": {
            "execution_mode": "compute",
            "inference_target": "none",
            "resource_class": "gpu",
            "placement_constraints": {
                "tenant_id": "t1",
                "required_capabilities": ["compute.comfyui.local"],
            },
        },
    }

    first = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            allocate_payload,
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert first.status_code == 200
    first_event = first.json()
    assert first_event["event_type"] == "device.lease.acquired"
    first_decision = first_event["payload"]["decision"]
    assert first_decision["outcome"] == "lease_acquired"
    first_lease_id = first_decision["lease_id"]

    second = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                **allocate_payload,
                "load_by_device": {"gpu-node-replay": 3},
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert second.status_code == 200
    second_event = second.json()
    assert second_event["event_type"] == "device.lease.acquired"
    second_decision = second_event["payload"]["decision"]
    assert second_decision["outcome"] == "lease_acquired"
    assert second_decision["lease_id"] == first_lease_id
    assert second_decision["device_id"] == first_decision["device_id"]
    assert second_decision["reason_code"] == "idempotent_replay"
    assert second_decision["resource_snapshot"]["eligible_devices"] == 1
    assert second_decision["resource_snapshot"]["active_leases"] == 1
    assert second_decision["resource_snapshot"]["available_slots"] == 0
    assert second_decision["resource_snapshot"]["tenant_id"] == "t1"
    assert second_decision["resource_snapshot"]["tenant_active_leases"] == 1
    assert second_decision["resource_snapshot"]["tenant_limit"] == 1

    capacity = client.get(
        "/v1/placements/capacity",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert capacity.status_code == 200
    capacity_payload = capacity.json()
    assert capacity_payload["active_leases"] == 1
    assert capacity_payload["available_slots"] == 0

    quota_rejected = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-api-replay-2",
                "task_id": "task-api-replay-2",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert quota_rejected.status_code == 200
    quota_event = quota_rejected.json()
    assert quota_event["event_type"] == "device.route.rejected"
    assert quota_event["payload"]["decision"]["reason_code"] == "tenant_quota_exhausted"


def test_allocate_placement_tenant_quota_recovers_after_release() -> None:
    client = _setup_test_env()
    app_module._hub = DeviceHubService(max_active_leases_per_tenant=1)
    token = _token(["devices:write", "devices:read"])

    for device_id in ("gpu-node-quota-recover-a", "gpu-node-quota-recover-b"):
        client.post(
            "/v1/devices/register",
            json=_command_envelope(
                {
                    "device_id": device_id,
                    "capabilities": ["compute.comfyui.local"],
                }
            ),
            headers={"Authorization": f"Bearer {token}"},
        )
        pair_req = client.post(
            "/v1/devices/pairing/request",
            json=_command_envelope({"device_id": device_id}),
            headers={"Authorization": f"Bearer {token}"},
        )
        code = pair_req.json()["payload"]["code"]
        client.post(
            "/v1/devices/pairing/approve",
            json=_command_envelope({"code": code}),
            headers={"Authorization": f"Bearer {token}"},
        )
        client.post(
            "/v1/devices/heartbeat",
            json=_command_envelope({"device_id": device_id}),
            headers={"Authorization": f"Bearer {token}"},
        )

    first = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-quota-recover-api-1",
                "task_id": "task-quota-recover-api-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert first.status_code == 200
    assert first.json()["event_type"] == "device.lease.acquired"
    lease_id = first.json()["payload"]["decision"]["lease_id"]

    second = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-quota-recover-api-2",
                "task_id": "task-quota-recover-api-2",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert second.status_code == 200
    assert second.json()["event_type"] == "device.route.rejected"
    assert second.json()["payload"]["decision"]["reason_code"] == "tenant_quota_exhausted"

    release = client.post(
        "/v1/placements/release",
        json=_command_envelope(
            {"lease_id": lease_id, "placement_request_id": f"lease:{lease_id}"},
            command_type="device.placement.release",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert release.status_code == 200
    assert release.json()["event_type"] == "device.lease.released"

    capacity = client.get(
        "/v1/placements/capacity",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert capacity.status_code == 200
    capacity_payload = capacity.json()
    assert capacity_payload["active_leases"] == 0
    assert capacity_payload["available_slots"] == 2
    assert capacity_payload["tenant_quota"]["tenants_at_limit"] == 0

    third = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-quota-recover-api-3",
                "task_id": "task-quota-recover-api-3",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert third.status_code == 200
    assert third.json()["event_type"] == "device.lease.acquired"
    assert third.json()["payload"]["decision"]["resource_snapshot"]["tenant_active_leases"] == 0


def test_allocate_placement_tenant_quota_can_use_envelope_tenant_fallback() -> None:
    client = _setup_test_env()
    app_module._hub = DeviceHubService(max_active_leases_per_tenant=1)
    token = _token(["devices:write", "devices:read"])

    for device_id in ("gpu-node-quota-fallback-a", "gpu-node-quota-fallback-b"):
        client.post(
            "/v1/devices/register",
            json=_command_envelope(
                {
                    "device_id": device_id,
                    "capabilities": ["compute.comfyui.local"],
                }
            ),
            headers={"Authorization": f"Bearer {token}"},
        )
        pair_req = client.post(
            "/v1/devices/pairing/request",
            json=_command_envelope({"device_id": device_id}),
            headers={"Authorization": f"Bearer {token}"},
        )
        code = pair_req.json()["payload"]["code"]
        client.post(
            "/v1/devices/pairing/approve",
            json=_command_envelope({"code": code}),
            headers={"Authorization": f"Bearer {token}"},
        )
        client.post(
            "/v1/devices/heartbeat",
            json=_command_envelope({"device_id": device_id}),
            headers={"Authorization": f"Bearer {token}"},
        )

    first = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-quota-fallback-1",
                "task_id": "task-quota-fallback-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert first.status_code == 200
    assert first.json()["event_type"] == "device.lease.acquired"

    second = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-quota-fallback-2",
                "task_id": "task-quota-fallback-2",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert second.status_code == 200
    event = second.json()
    assert event["event_type"] == "device.route.rejected"
    assert event["payload"]["decision"]["reason_code"] == "tenant_quota_exhausted"
    snapshot = event["payload"]["decision"]["resource_snapshot"]
    assert snapshot["tenant_id"] == "t1"
    assert snapshot["tenant_active_leases"] == 1
    assert snapshot["tenant_limit"] == 1


def test_allocate_placement_returns_required_capabilities_unavailable_reason() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {
                "device_id": "gpu-node-required-gap-api",
                "capabilities": ["compute.comfyui.local", "model.sd15"],
            }
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-required-gap-api"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-required-gap-api"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    response = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-required-gap-api",
                "task_id": "task-required-gap-api",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": [
                            "compute.comfyui.local",
                            "model.sdxl",
                        ],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    event = response.json()
    assert event["event_type"] == "device.route.rejected"
    assert event["payload"]["decision"]["reason_code"] == "required_capabilities_unavailable"
    assert "required_capabilities" in event["payload"]["decision"]["reason"]
    snapshot = event["payload"]["decision"]["resource_snapshot"]
    assert snapshot["eligible_devices"] == 1
    assert snapshot["active_leases"] == 0
    assert snapshot["available_slots"] == 1
    assert snapshot["tenant_id"] == "t1"
    assert snapshot["tenant_active_leases"] == 0


def test_allocate_placement_returns_node_pool_fallback_when_requested_pool_missing() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {
                "device_id": "gpu-node-pool-fallback-api",
                "capabilities": ["compute.comfyui.local"],
                "region": "us-west",
                "node_pool": "pool-b",
            }
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-pool-fallback-api"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-pool-fallback-api"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    response = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-nodepool-fallback-api",
                "task_id": "task-nodepool-fallback-api",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "region": "us-west",
                        "node_pool": "pool-a",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    event = response.json()
    assert event["event_type"] == "device.lease.acquired"
    assert event["payload"]["decision"]["reason_code"] == "node_pool_fallback"
    assert "alternate node_pool" in event["payload"]["decision"]["reason"]


def test_allocate_placement_returns_avoid_capabilities_excluded_reason() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {
                "device_id": "gpu-node-avoid-excluded-api",
                "capabilities": ["compute.comfyui.local", "blocked.maintenance"],
            }
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-avoid-excluded-api"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-avoid-excluded-api"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    response = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-avoid-excluded-api",
                "task_id": "task-avoid-excluded-api",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                        "avoid_capabilities": ["blocked.maintenance"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    event = response.json()
    assert event["event_type"] == "device.route.rejected"
    assert event["payload"]["decision"]["reason_code"] == "avoid_capabilities_excluded"
    assert "avoid_capabilities" in event["payload"]["decision"]["reason"]
    snapshot = event["payload"]["decision"]["resource_snapshot"]
    assert snapshot["eligible_devices"] == 1
    assert snapshot["active_leases"] == 0
    assert snapshot["available_slots"] == 1
    assert snapshot["tenant_id"] == "t1"
    assert snapshot["tenant_active_leases"] == 0


def test_allocate_placement_rejects_invalid_execution_profile() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])
    response = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-alloc-3",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "hosted_api",
                    "resource_class": "llm_api",
                    "placement_constraints": {"tenant_id": "t1"},
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 422


def test_get_placement_capacity_requires_read_scope() -> None:
    client = _setup_test_env()
    token = _token(["devices:write"])
    response = client.get(
        "/v1/placements/capacity",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 403


def test_get_placement_capacity_returns_zero_utilization_when_no_eligible_devices() -> None:
    client = _setup_test_env()
    token = _token(["devices:read"])
    response = client.get(
        "/v1/placements/capacity",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["eligible_devices"] == 0
    assert payload["active_leases"] == 0
    assert payload["available_slots"] == 0
    assert payload["lease_utilization"] == 0.0


def test_get_placement_capacity_returns_snapshot() -> None:
    client = _setup_test_env()
    write_token = _token(["devices:write"])
    read_token = _token(["devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {
                "device_id": "gpu-node-capacity",
                "capabilities": ["compute.comfyui.local"],
            }
        ),
        headers={"Authorization": f"Bearer {write_token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-capacity"}),
        headers={"Authorization": f"Bearer {write_token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {write_token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-capacity"}),
        headers={"Authorization": f"Bearer {write_token}"},
    )
    client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-capacity-1",
                "task_id": "task-capacity-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {write_token}"},
    )

    response = client.get(
        "/v1/placements/capacity",
        headers={"Authorization": f"Bearer {read_token}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["eligible_devices"] >= 1
    assert payload["active_leases"] >= 1
    assert payload["lease_status_counts"]["active"] >= 1
    assert payload["lease_status_counts"]["released"] >= 0
    assert payload["lease_status_counts"]["expired"] >= 0
    assert payload["available_slots"] >= 0
    assert 0.0 <= payload["lease_utilization"] <= 1.0
    assert payload["lease_expire_sweeps_total"] >= 1
    assert payload["lease_expired_total"] >= 0
    assert payload["lease_expire_last_sweep_expired"] >= 0
    assert isinstance(payload["lease_expire_last_sweep_at"], str)
    assert payload["tenant_quota"]["enabled"] is False
    assert payload["tenant_quota"]["max_active_leases_per_tenant"] is None
    assert payload["tenant_quota"]["tenants_with_active_leases"] >= 1


def test_get_placement_capacity_exposes_ttl_expire_sweep_metrics() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {"device_id": "gpu-node-capacity-expire", "capabilities": ["compute.comfyui.local"]}
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-capacity-expire"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-capacity-expire"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-capacity-expire-1",
                "task_id": "task-capacity-expire-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert allocate.status_code == 200
    lease_id = allocate.json()["payload"]["decision"]["lease_id"]
    app_module._hub.leases[lease_id].lease_expires_at = (
        datetime.now(timezone.utc) - timedelta(seconds=5)
    ).isoformat()

    response = client.get(
        "/v1/placements/capacity",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["active_leases"] == 0
    assert payload["lease_status_counts"]["active"] == 0
    assert payload["lease_status_counts"]["expired"] >= 1
    assert payload["lease_expire_last_sweep_expired"] >= 1
    assert payload["lease_expired_total"] >= 1
    assert payload["tenant_quota"]["enabled"] is False


def test_get_placement_capacity_exposes_tenant_quota_snapshot_when_enabled() -> None:
    client = _setup_test_env()
    app_module._hub = DeviceHubService(max_active_leases_per_tenant=1)
    token = _token(["devices:write", "devices:read"])

    for device_id in ("gpu-node-capacity-quota-a", "gpu-node-capacity-quota-b"):
        client.post(
            "/v1/devices/register",
            json=_command_envelope({"device_id": device_id, "capabilities": ["compute.comfyui.local"]}),
            headers={"Authorization": f"Bearer {token}"},
        )
        pair_req = client.post(
            "/v1/devices/pairing/request",
            json=_command_envelope({"device_id": device_id}),
            headers={"Authorization": f"Bearer {token}"},
        )
        code = pair_req.json()["payload"]["code"]
        client.post(
            "/v1/devices/pairing/approve",
            json=_command_envelope({"code": code}),
            headers={"Authorization": f"Bearer {token}"},
        )
        client.post(
            "/v1/devices/heartbeat",
            json=_command_envelope({"device_id": device_id}),
            headers={"Authorization": f"Bearer {token}"},
        )

    allocate_first = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-capacity-quota-1",
                "task_id": "task-capacity-quota-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert allocate_first.status_code == 200

    allocate_second = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-capacity-quota-2",
                "task_id": "task-capacity-quota-2",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert allocate_second.status_code == 200
    assert allocate_second.json()["event_type"] == "device.route.rejected"
    assert allocate_second.json()["payload"]["decision"]["reason_code"] == "tenant_quota_exhausted"

    response = client.get(
        "/v1/placements/capacity",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["tenant_quota"]["enabled"] is True
    assert payload["tenant_quota"]["max_active_leases_per_tenant"] == 1
    assert payload["tenant_quota"]["tenants_with_active_leases"] >= 1
    assert payload["tenant_quota"]["max_tenant_active_leases"] >= 1
    assert payload["tenant_quota"]["tenants_at_limit"] >= 1


def test_get_placement_capacity_exposes_tenant_quota_overrides() -> None:
    client = _setup_test_env()
    app_module._hub = DeviceHubService(
        max_active_leases_per_tenant=3,
        tenant_active_lease_limits={"t1": 1},
    )
    token = _token(["devices:write", "devices:read"])

    for device_id in ("gpu-node-capacity-ovr-a", "gpu-node-capacity-ovr-b"):
        client.post(
            "/v1/devices/register",
            json=_command_envelope({"device_id": device_id, "capabilities": ["compute.comfyui.local"]}),
            headers={"Authorization": f"Bearer {token}"},
        )
        pair_req = client.post(
            "/v1/devices/pairing/request",
            json=_command_envelope({"device_id": device_id}),
            headers={"Authorization": f"Bearer {token}"},
        )
        code = pair_req.json()["payload"]["code"]
        client.post(
            "/v1/devices/pairing/approve",
            json=_command_envelope({"code": code}),
            headers={"Authorization": f"Bearer {token}"},
        )
        client.post(
            "/v1/devices/heartbeat",
            json=_command_envelope({"device_id": device_id}),
            headers={"Authorization": f"Bearer {token}"},
        )

    allocate_first = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-capacity-ovr-1",
                "task_id": "task-capacity-ovr-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert allocate_first.status_code == 200
    assert allocate_first.json()["event_type"] == "device.lease.acquired"

    allocate_second = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-capacity-ovr-2",
                "task_id": "task-capacity-ovr-2",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert allocate_second.status_code == 200
    assert allocate_second.json()["event_type"] == "device.route.rejected"
    assert allocate_second.json()["payload"]["decision"]["reason_code"] == "tenant_quota_exhausted"

    response = client.get(
        "/v1/placements/capacity",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["tenant_quota"]["enabled"] is True
    assert payload["tenant_quota"]["max_active_leases_per_tenant"] == 3
    assert payload["tenant_quota"]["tenant_limit_overrides"]["t1"] == 1
    assert payload["tenant_quota"]["tenants_at_limit"] >= 1


def test_get_placement_lease_requires_read_scope() -> None:
    client = _setup_test_env()
    token = _token(["devices:write"])
    response = client.get(
        "/v1/placements/leases/lease-missing",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 403


def test_get_placement_lease_returns_lifecycle_snapshot() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {"device_id": "gpu-node-lease-view", "capabilities": ["compute.comfyui.local"]}
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-lease-view"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-lease-view"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-lease-view-1",
                "task_id": "task-lease-view-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    lease_id = allocate.json()["payload"]["decision"]["lease_id"]

    first_read = client.get(
        f"/v1/placements/leases/{lease_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert first_read.status_code == 200
    first_payload = first_read.json()
    assert first_payload["lease_id"] == lease_id
    assert first_payload["status"] == "active"
    assert first_payload["released_at"] is None
    assert first_payload["expired_at"] is None

    release = client.post(
        "/v1/placements/release",
        json=_command_envelope(
            {"lease_id": lease_id, "placement_request_id": f"lease:{lease_id}"},
            command_type="device.placement.release",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert release.status_code == 200

    second_read = client.get(
        f"/v1/placements/leases/{lease_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert second_read.status_code == 200
    second_payload = second_read.json()
    assert second_payload["status"] == "released"
    assert isinstance(second_payload["released_at"], str)
    assert second_payload["expired_at"] is None


def test_get_placement_lease_marks_ttl_expired_snapshot() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {"device_id": "gpu-node-lease-expire-view", "capabilities": ["compute.comfyui.local"]}
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-lease-expire-view"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-lease-expire-view"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-lease-expire-view-1",
                "task_id": "task-lease-expire-view-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    lease_id = allocate.json()["payload"]["decision"]["lease_id"]
    app_module._hub.leases[lease_id].lease_expires_at = (
        datetime.now(timezone.utc) - timedelta(seconds=5)
    ).isoformat()

    read = client.get(
        f"/v1/placements/leases/{lease_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert read.status_code == 200
    payload = read.json()
    assert payload["lease_id"] == lease_id
    assert payload["status"] == "expired"
    assert payload["expire_reason_code"] == "ttl_expired"
    assert isinstance(payload["expired_at"], str)


def test_release_placement_returns_409_for_ttl_expired_lease() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {"device_id": "gpu-node-release-expired", "capabilities": ["compute.comfyui.local"]}
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-release-expired"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-release-expired"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-release-expired-1",
                "task_id": "task-release-expired-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    lease_id = allocate.json()["payload"]["decision"]["lease_id"]
    app_module._hub.leases[lease_id].lease_expires_at = (
        datetime.now(timezone.utc) - timedelta(seconds=5)
    ).isoformat()

    release = client.post(
        "/v1/placements/release",
        json=_command_envelope(
            {"lease_id": lease_id, "placement_request_id": f"lease:{lease_id}"},
            command_type="device.placement.release",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert release.status_code == 409
    assert "lease already expired" in release.text


def test_release_placement_emits_lease_released_event() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {"device_id": "gpu-node-release", "capabilities": ["compute.comfyui.local"]}
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-release"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-release"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-release-1",
                "task_id": "task-release-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    lease_id = allocate.json()["payload"]["decision"]["lease_id"]

    release = client.post(
        "/v1/placements/release",
        json=_command_envelope(
            {"lease_id": lease_id, "placement_request_id": f"lease:{lease_id}"},
            command_type="device.placement.release",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert release.status_code == 200
    event = release.json()
    assert event["event_type"] == "device.lease.released"
    assert event["payload"]["decision"]["outcome"] == "lease_released"
    assert event["payload"]["decision"]["lease_id"] == lease_id


def test_expire_placement_emits_lease_expired_event() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {"device_id": "gpu-node-expire", "capabilities": ["compute.comfyui.local"]}
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-expire"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-expire"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-expire-1",
                "task_id": "task-expire-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    lease_id = allocate.json()["payload"]["decision"]["lease_id"]

    expire = client.post(
        "/v1/placements/expire",
        json=_command_envelope(
            {"lease_id": lease_id, "reason_code": "ttl_expired"},
            command_type="device.placement.expire",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert expire.status_code == 200
    event = expire.json()
    assert event["event_type"] == "device.lease.expired"
    assert event["payload"]["decision"]["outcome"] == "lease_expired"
    assert event["payload"]["decision"]["lease_id"] == lease_id
    assert event["payload"]["decision"]["reason_code"] == "ttl_expired"


def test_expire_placement_normalizes_reason_code_term() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {"device_id": "gpu-node-expire-normalize", "capabilities": ["compute.comfyui.local"]}
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-expire-normalize"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-expire-normalize"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-expire-normalize-1",
                "task_id": "task-expire-normalize-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    lease_id = allocate.json()["payload"]["decision"]["lease_id"]

    expire = client.post(
        "/v1/placements/expire",
        json=_command_envelope(
            {"lease_id": lease_id, "reason_code": "TTL.Expired"},
            command_type="device.placement.expire",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert expire.status_code == 200
    event = expire.json()
    assert event["payload"]["decision"]["reason_code"] == "ttl_expired"


def test_preempt_placement_emits_lease_expired_event_with_preempt_reason() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {"device_id": "gpu-node-preempt", "capabilities": ["compute.comfyui.local"]}
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-preempt"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-preempt"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-preempt-1",
                "task_id": "task-preempt-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    lease_id = allocate.json()["payload"]["decision"]["lease_id"]

    preempt = client.post(
        "/v1/placements/preempt",
        json=_command_envelope(
            {"lease_id": lease_id, "reason_code": "preempted_by_policy"},
            command_type="device.placement.preempt",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert preempt.status_code == 200
    event = preempt.json()
    assert event["event_type"] == "device.lease.expired"
    assert event["payload"]["decision"]["outcome"] == "lease_expired"
    assert event["payload"]["decision"]["lease_id"] == lease_id
    assert event["payload"]["decision"]["reason_code"] == "preempted_by_policy"


def test_preempt_placement_normalizes_reason_code_term() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {"device_id": "gpu-node-preempt-normalize", "capabilities": ["compute.comfyui.local"]}
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-preempt-normalize"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-preempt-normalize"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-preempt-normalize-1",
                "task_id": "task-preempt-normalize-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    lease_id = allocate.json()["payload"]["decision"]["lease_id"]

    preempt = client.post(
        "/v1/placements/preempt",
        json=_command_envelope(
            {"lease_id": lease_id, "reason_code": "Run-Preempted"},
            command_type="device.placement.preempt",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert preempt.status_code == 200
    event = preempt.json()
    assert event["payload"]["decision"]["reason_code"] == "run_preempted"


def test_preempt_placement_returns_409_for_released_lease() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {"device_id": "gpu-node-preempt-released", "capabilities": ["compute.comfyui.local"]}
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-preempt-released"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-preempt-released"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-preempt-released-1",
                "task_id": "task-preempt-released-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    lease_id = allocate.json()["payload"]["decision"]["lease_id"]
    release = client.post(
        "/v1/placements/release",
        json=_command_envelope(
            {"lease_id": lease_id, "placement_request_id": f"lease:{lease_id}"},
            command_type="device.placement.release",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert release.status_code == 200

    preempt = client.post(
        "/v1/placements/preempt",
        json=_command_envelope(
            {"lease_id": lease_id, "reason_code": "preempted_by_policy"},
            command_type="device.placement.preempt",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert preempt.status_code == 409
    assert "lease already released" in preempt.text


def test_renew_placement_emits_lease_renewed_event() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {"device_id": "gpu-node-renew", "capabilities": ["compute.comfyui.local"]}
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-renew"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-renew"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-renew-1",
                "task_id": "task-renew-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    lease_id = allocate.json()["payload"]["decision"]["lease_id"]
    previous_expires_at = allocate.json()["payload"]["decision"]["lease_expires_at"]

    renew = client.post(
        "/v1/placements/renew",
        json=_command_envelope(
            {"lease_id": lease_id, "lease_ttl_seconds": 600},
            command_type="device.placement.renew",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert renew.status_code == 200
    event = renew.json()
    assert event["event_type"] == "device.lease.renewed"
    assert event["payload"]["decision"]["outcome"] == "lease_renewed"
    assert event["payload"]["decision"]["lease_id"] == lease_id
    assert datetime.fromisoformat(event["payload"]["decision"]["lease_expires_at"]) > datetime.fromisoformat(
        previous_expires_at
    )


def test_renew_placement_returns_409_for_ttl_expired_lease() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {"device_id": "gpu-node-renew-expired", "capabilities": ["compute.comfyui.local"]}
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-renew-expired"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-renew-expired"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-renew-expired-1",
                "task_id": "task-renew-expired-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    lease_id = allocate.json()["payload"]["decision"]["lease_id"]
    app_module._hub.leases[lease_id].lease_expires_at = (
        datetime.now(timezone.utc) - timedelta(seconds=5)
    ).isoformat()

    renew = client.post(
        "/v1/placements/renew",
        json=_command_envelope(
            {"lease_id": lease_id, "lease_ttl_seconds": 300},
            command_type="device.placement.renew",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert renew.status_code == 409
    assert "lease already expired" in renew.text


def test_renew_placement_rejects_invalid_ttl_range() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])
    response = client.post(
        "/v1/placements/renew",
        json=_command_envelope(
            {"lease_id": "lease-missing", "lease_ttl_seconds": 5},
            command_type="device.placement.renew",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 422
    assert "lease_ttl_seconds must be integer in [30, 3600]" in response.text


def test_release_placement_is_idempotent_for_repeated_release() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    _ = client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {"device_id": "gpu-node-release-idem", "capabilities": ["compute.comfyui.local"]}
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-release-idem"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    _ = client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    _ = client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-release-idem"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-release-idem-1",
                "task_id": "task-release-idem-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    lease_id = allocate.json()["payload"]["decision"]["lease_id"]

    first_release = client.post(
        "/v1/placements/release",
        json=_command_envelope(
            {"lease_id": lease_id, "placement_request_id": f"lease:{lease_id}"},
            command_type="device.placement.release",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    second_release = client.post(
        "/v1/placements/release",
        json=_command_envelope(
            {"lease_id": lease_id, "placement_request_id": f"lease:{lease_id}"},
            command_type="device.placement.release",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )

    assert first_release.status_code == 200
    assert second_release.status_code == 200
    assert first_release.json()["event_type"] == "device.lease.released"
    assert second_release.json()["event_type"] == "device.lease.released"
    assert first_release.json()["payload"]["decision"]["lease_id"] == lease_id
    assert second_release.json()["payload"]["decision"]["lease_id"] == lease_id


def test_expire_placement_is_idempotent_for_repeated_expire() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    _ = client.post(
        "/v1/devices/register",
        json=_command_envelope(
            {"device_id": "gpu-node-expire-idem", "capabilities": ["compute.comfyui.local"]}
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    pair_req = client.post(
        "/v1/devices/pairing/request",
        json=_command_envelope({"device_id": "gpu-node-expire-idem"}),
        headers={"Authorization": f"Bearer {token}"},
    )
    code = pair_req.json()["payload"]["code"]
    _ = client.post(
        "/v1/devices/pairing/approve",
        json=_command_envelope({"code": code}),
        headers={"Authorization": f"Bearer {token}"},
    )
    _ = client.post(
        "/v1/devices/heartbeat",
        json=_command_envelope({"device_id": "gpu-node-expire-idem"}),
        headers={"Authorization": f"Bearer {token}"},
    )

    allocate = client.post(
        "/v1/placements/allocate",
        json=_command_envelope(
            {
                "run_id": "run-expire-idem-1",
                "task_id": "task-expire-idem-1",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local"],
                    },
                },
            },
            command_type="device.placement.allocate",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    lease_id = allocate.json()["payload"]["decision"]["lease_id"]

    first_expire = client.post(
        "/v1/placements/expire",
        json=_command_envelope(
            {"lease_id": lease_id, "reason_code": "ttl_expired"},
            command_type="device.placement.expire",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    second_expire = client.post(
        "/v1/placements/expire",
        json=_command_envelope(
            {"lease_id": lease_id, "reason_code": "operator_retry"},
            command_type="device.placement.expire",
        ),
        headers={"Authorization": f"Bearer {token}"},
    )

    assert first_expire.status_code == 200
    assert second_expire.status_code == 200
    assert first_expire.json()["event_type"] == "device.lease.expired"
    assert second_expire.json()["event_type"] == "device.lease.expired"
    assert first_expire.json()["payload"]["decision"]["lease_id"] == lease_id
    assert second_expire.json()["payload"]["decision"]["lease_id"] == lease_id
    assert first_expire.json()["payload"]["decision"]["reason_code"] == "ttl_expired"
    assert second_expire.json()["payload"]["decision"]["reason_code"] == "ttl_expired"


def test_release_placement_returns_404_for_unknown_lease() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])
    response = client.post(
        "/v1/placements/release",
        json=_command_envelope({"lease_id": "lease-missing"}, command_type="device.placement.release"),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 404


def test_get_device_requires_read_scope() -> None:
    client = _setup_test_env()
    write_token = _token(["devices:write"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope({"device_id": "desktop-read", "capabilities": ["compute.comfyui.local"]}),
        headers={"Authorization": f"Bearer {write_token}"},
    )

    response = client.get(
        "/v1/devices/desktop-read",
        headers={"Authorization": f"Bearer {write_token}"},
    )
    assert response.status_code == 403


def test_get_device_returns_status_event() -> None:
    client = _setup_test_env()
    token = _token(["devices:write", "devices:read"])

    client.post(
        "/v1/devices/register",
        json=_command_envelope({"device_id": "desktop-status", "capabilities": ["compute.comfyui.local"]}),
        headers={"Authorization": f"Bearer {token}"},
    )

    response = client.get(
        "/v1/devices/desktop-status",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    event = response.json()
    assert event["event_type"] == "device.status"
    assert event["payload"]["device_id"] == "desktop-status"
