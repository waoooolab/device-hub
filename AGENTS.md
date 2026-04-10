# device-hub AGENTS.md

## Service Description
Device hub service that manages device registry/state, placement and routing contracts, and service APIs for runtime device interactions.

## Entrypoints
- `src/device_hub/service_api/app.py` (`device_hub.service_api.app:app`) - FastAPI service API entry.
- `src/device_hub/service.py` - core service orchestration and device state handling.
- `src/device_hub/routing/device_router.py` - device route resolution boundary.

## Key Modules
- `src/device_hub/devices/registry.py` and `src/device_hub/devices/heartbeat.py` - device registry and liveness tracking.
- `src/device_hub/state_store.py` - persistent state contract boundary.
- `src/device_hub/service_api/placements.py` - placement policy and API mapping.
- `src/device_hub/resources/capability_registry.py` - device capability surface and catalog mapping.

## Common Commands
- `uv run pytest -q`
- `python3 scripts/run_tests.py`
- `uv run uvicorn device_hub.service_api.app:app --host 0.0.0.0 --port 8005`
- `python3 scripts/check_boundary_imports.py --own-package device_hub --src src/device_hub`

## ACP Provider Playbook

### Claude Code (claude)
- Startup/Connection: Configure provider id `claude` in device-hub integration profile and load Anthropic credentials securely.
- Best Practices: Keep device control prompts explicit about safety and state transition constraints.
- Known Limitations/Notes: Long prompts can impact control-path latency; limit context to device-relevant facts.
- Suitable Scenarios: Safety-critical decision support, incident triage, and policy-sensitive diagnostics.

### Codex
- Startup/Connection: Configure provider id `codex` via OpenAI-compatible routing in device-hub service config.
- Best Practices: Enforce strict structured responses and bounded action lists for device operations.
- Known Limitations/Notes: Ambiguous tool descriptions increase execution noise; keep command surfaces narrow.
- Suitable Scenarios: Device workflow automation, code-centric adaptation, and deterministic transformation tasks.

### Gemini
- Startup/Connection: Configure provider id `gemini` and inject Google AI credentials from environment profiles.
- Best Practices: Use sectioned prompt contracts and explicit routing/output schema instructions.
- Known Limitations/Notes: Model feature behavior can vary by tier/region; validate fallback behavior continuously.
- Suitable Scenarios: Long-context telemetry summarization, anomaly classification, and knowledge synthesis.

### OpenCode / Droid
- Startup/Connection: Configure provider id `droid` and attach ACP session metadata for workspace and operator scope.
- Best Practices: Apply least-privilege tool policy, strict timeout limits, and idempotent command handling.
- Known Limitations/Notes: ACP session drops require retry-safe design and explicit state reconciliation.
- Suitable Scenarios: Live device-debug workflows, terminal-driven operations, and human-in-the-loop control.

### Other Mainstream ACP Providers
- Startup/Connection: Integrate via OpenAI-compatible adapters (Azure OpenAI, enterprise gateways, local inference).
- Best Practices: Normalize provider responses to contract schemas and gate writes on validation success.
- Known Limitations/Notes: Context limits and tool-call semantics vary by provider/version.
- Suitable Scenarios: Multi-provider resilience, cost-aware routing, and compliance-driven deployment topologies.
