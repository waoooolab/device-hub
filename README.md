# device-hub
[![CI](https://github.com/waoooolab/device-hub/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/waoooolab/device-hub/actions/workflows/ci.yml)

Device and resource management service.

P0 scope:
- Device registration and heartbeat
- Capability registration
- Basic device routing strategy

Current baseline additions:
- pairing request and approval flow
- heartbeat freshness and offline refresh helper
- load-aware routing baseline (min load, lexical tiebreak)
- device revoke flow removing routing eligibility
- routed command envelope baseline preserving `trace_id`
- service boundary API for registration/pairing/heartbeat/routing/status
- runtime boundary auth/contract gates (`aud=device-hub`, `devices:write|devices:read`)

Testing:
- `scripts/run_tests.py` runs all `tests/test_*.py` function tests.
- `scripts/check_code_shape.py` reports file/function size guardrails
  (target fail: file>300/function>40; CI enforces this threshold).
- `.github/workflows/ci.yml` runs tests on PR/push across
  Ubuntu/Windows/macOS with Python 3.11 and 3.12.

Service mode:
- run server:
  `uvicorn device_hub.service_api.app:app --host 0.0.0.0 --port 8004`
- `POST /v1/devices/register`
- `POST /v1/devices/pairing/request`
- `POST /v1/devices/pairing/approve`
- `POST /v1/devices/heartbeat`
- `POST /v1/devices/presence/refresh`
- `POST /v1/devices/route`
- `GET /v1/devices/{device_id}`
