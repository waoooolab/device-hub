# device-hub

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
