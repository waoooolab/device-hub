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


def test_register_requires_token() -> None:
    client = _setup_test_env()
    response = client.post(
        "/v1/devices/register",
        json=_command_envelope({"device_id": "d1", "capabilities": ["compute.comfyui.local"]}),
    )
    assert response.status_code == 401


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

    route = client.post(
        "/v1/devices/route",
        json=_command_envelope(
            {
                "capability": "compute.comfyui.local",
                "command_type": "tool.exec",
                "command_payload": {"x": 1},
                "load_by_device": {"desktop-a": 5, "desktop-b": 1},
            }
        ),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert route.status_code == 200
    assert route.json()["event_type"] == "device.route.selected"
    assert route.json()["payload"]["route"]["device_id"] == "desktop-b"
    assert route.json()["payload"]["route"]["trace_id"] == "trace-device-1"


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
