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

Testing:
- `scripts/run_tests.py` runs all `tests/test_*.py` function tests.
- `.github/workflows/ci.yml` runs tests on PR/push across
  Ubuntu/Windows/macOS with Python 3.11 and 3.12.
