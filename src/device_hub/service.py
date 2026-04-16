"""Device-hub application service."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from .code_terms import normalize_optional_code_term
from .devices.pairing import PairingManager, PairingRequest
from .devices.registry import (
    DeviceRecord,
    DeviceRegistry,
    is_valid_device_status,
)
from .resources.capability_registry import CapabilityRegistry
from .routing.device_router import choose_device
from .state_store import DeviceHubStateStore


@dataclass
class LeaseRecord:
    lease_id: str
    run_id: str
    task_id: str
    device_id: str
    capability: str
    trace_id: str
    lease_expires_at: str
    tenant_id: str | None = None
    status: str = "active"
    released_at: str | None = None
    expired_at: str | None = None
    expire_reason_code: str | None = None


@dataclass
class DeviceHubService:
    registry: DeviceRegistry = field(default_factory=DeviceRegistry)
    capabilities: CapabilityRegistry = field(default_factory=CapabilityRegistry)
    pairing: PairingManager = field(default_factory=PairingManager)
    leases: dict[str, LeaseRecord] = field(default_factory=dict)
    max_active_leases_per_tenant: int | None = None
    tenant_active_lease_limits: dict[str, int] = field(default_factory=dict)
    lease_expire_sweeps_total: int = 0
    lease_expired_total: int = 0
    lease_expire_last_sweep_at: str | None = None
    lease_expire_last_sweep_expired: int = 0
    lease_policy_ticks_total: int = 0
    lease_policy_last_tick_at: str | None = None
    lease_policy_last_preempted: int = 0
    lease_policy_last_renewed: int = 0
    active_lease_index_by_run_task: dict[tuple[str, str], str] = field(default_factory=dict)
    persistence_db_path: str | None = None
    _state_store: DeviceHubStateStore | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_active_leases_per_tenant is not None:
            if (
                not isinstance(self.max_active_leases_per_tenant, int)
                or isinstance(self.max_active_leases_per_tenant, bool)
                or self.max_active_leases_per_tenant <= 0
            ):
                raise ValueError("max_active_leases_per_tenant must be positive integer when provided")
        self.tenant_active_lease_limits = _normalize_tenant_active_lease_limits(
            self.tenant_active_lease_limits
        )
        if isinstance(self.persistence_db_path, str):
            normalized_path = self.persistence_db_path.strip()
            self.persistence_db_path = normalized_path or None
        if self.persistence_db_path is not None:
            self._state_store = DeviceHubStateStore(self.persistence_db_path)
            self._restore_persisted_state()

    @staticmethod
    def _as_nonnegative_int(value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            return 0
        return max(0, value)

    @staticmethod
    def _normalize_optional_str(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        normalized = value.strip()
        return normalized or None

    @staticmethod
    def _restore_device_record(value: Any) -> DeviceRecord | None:
        if not isinstance(value, dict):
            return None
        device_id = DeviceHubService._normalize_optional_str(value.get("device_id"))
        if device_id is None:
            return None
        raw_capabilities = value.get("capabilities")
        if not isinstance(raw_capabilities, list):
            return None
        capabilities = [
            capability.strip()
            for capability in raw_capabilities
            if isinstance(capability, str) and capability.strip()
        ]
        execution_site = DeviceHubService._normalize_optional_str(value.get("execution_site")) or "local"
        if execution_site not in {"local", "cloud"}:
            execution_site = "local"
        cost_tier = DeviceHubService._normalize_optional_str(value.get("cost_tier")) or "balanced"
        if cost_tier not in {"low", "balanced", "high"}:
            cost_tier = "balanced"
        estimated_cost_usd_raw = value.get("estimated_cost_usd")
        estimated_cost_usd = None
        if (
            isinstance(estimated_cost_usd_raw, (int, float))
            and not isinstance(estimated_cost_usd_raw, bool)
            and float(estimated_cost_usd_raw) >= 0
        ):
            estimated_cost_usd = float(estimated_cost_usd_raw)
        status = DeviceHubService._normalize_optional_str(value.get("status")) or "offline"
        if not is_valid_device_status(status):
            status = "offline"
        paired = value.get("paired") is True
        last_seen_at = DeviceHubService._normalize_optional_str(value.get("last_seen_at"))
        if last_seen_at is None:
            last_seen_at = datetime.now(timezone.utc).isoformat()
        region = DeviceHubService._normalize_optional_str(value.get("region"))
        node_pool = DeviceHubService._normalize_optional_str(value.get("node_pool"))
        return DeviceRecord(
            device_id=device_id,
            capabilities=capabilities,
            execution_site=execution_site,
            region=region,
            cost_tier=cost_tier,
            node_pool=node_pool,
            estimated_cost_usd=estimated_cost_usd,
            status=status,
            paired=paired,
            last_seen_at=last_seen_at,
        )

    @staticmethod
    def _restore_pairing_request(value: Any) -> PairingRequest | None:
        if not isinstance(value, dict):
            return None
        code = DeviceHubService._normalize_optional_str(value.get("code"))
        device_id = DeviceHubService._normalize_optional_str(value.get("device_id"))
        expires_at = DeviceHubService._normalize_optional_str(value.get("expires_at"))
        if code is None or device_id is None or expires_at is None:
            return None
        return PairingRequest(code=code, device_id=device_id, expires_at=expires_at)

    @staticmethod
    def _restore_lease_record(value: Any) -> LeaseRecord | None:
        if not isinstance(value, dict):
            return None
        lease_id = DeviceHubService._normalize_optional_str(value.get("lease_id"))
        run_id = DeviceHubService._normalize_optional_str(value.get("run_id"))
        task_id = DeviceHubService._normalize_optional_str(value.get("task_id"))
        device_id = DeviceHubService._normalize_optional_str(value.get("device_id"))
        capability = DeviceHubService._normalize_optional_str(value.get("capability"))
        trace_id = DeviceHubService._normalize_optional_str(value.get("trace_id"))
        lease_expires_at = DeviceHubService._normalize_optional_str(value.get("lease_expires_at"))
        if None in {lease_id, run_id, task_id, device_id, capability, trace_id, lease_expires_at}:
            return None
        status = DeviceHubService._normalize_optional_str(value.get("status")) or "active"
        if status not in {"active", "released", "expired"}:
            status = "active"
        tenant_id = DeviceHubService._normalize_optional_str(value.get("tenant_id"))
        released_at = DeviceHubService._normalize_optional_str(value.get("released_at"))
        expired_at = DeviceHubService._normalize_optional_str(value.get("expired_at"))
        expire_reason_code = normalize_optional_code_term(value.get("expire_reason_code"))
        return LeaseRecord(
            lease_id=lease_id,
            run_id=run_id,
            task_id=task_id,
            device_id=device_id,
            capability=capability,
            trace_id=trace_id,
            lease_expires_at=lease_expires_at,
            tenant_id=tenant_id,
            status=status,
            released_at=released_at,
            expired_at=expired_at,
            expire_reason_code=expire_reason_code,
        )

    def _rebuild_capability_registry(self) -> None:
        self.capabilities = CapabilityRegistry()
        for record in self.registry.devices.values():
            for capability in record.capabilities:
                if isinstance(capability, str) and capability.strip():
                    self.capabilities.bind(record.device_id, capability.strip())

    def _rebuild_active_lease_index(self) -> None:
        self.active_lease_index_by_run_task = {}
        for lease in self.leases.values():
            self._index_active_lease(lease)

    def _restore_persisted_state(self) -> None:
        if self._state_store is None:
            return
        snapshot = self._state_store.load_snapshot()
        if not isinstance(snapshot, dict):
            return
        devices: dict[str, DeviceRecord] = {}
        for item in snapshot.get("devices", []):
            record = self._restore_device_record(item)
            if record is None:
                continue
            devices[record.device_id] = record
        self.registry = DeviceRegistry(devices=devices)
        self._rebuild_capability_registry()

        pairings: dict[str, PairingRequest] = {}
        for item in snapshot.get("pairings", []):
            request = self._restore_pairing_request(item)
            if request is None:
                continue
            if request.device_id not in self.registry.devices:
                continue
            pairings[request.code] = request
        self.pairing = PairingManager(by_code=pairings)

        leases: dict[str, LeaseRecord] = {}
        for item in snapshot.get("leases", []):
            lease = self._restore_lease_record(item)
            if lease is None:
                continue
            if lease.device_id not in self.registry.devices:
                continue
            leases[lease.lease_id] = lease
        self.leases = leases
        self._rebuild_active_lease_index()
        for lease in self.leases.values():
            if lease.status == "active" and lease.device_id in self.registry.devices:
                self.registry.mark_busy(lease.device_id)

        self.lease_expire_sweeps_total = self._as_nonnegative_int(
            snapshot.get("lease_expire_sweeps_total")
        )
        self.lease_expired_total = self._as_nonnegative_int(snapshot.get("lease_expired_total"))
        self.lease_expire_last_sweep_at = self._normalize_optional_str(
            snapshot.get("lease_expire_last_sweep_at")
        )
        self.lease_expire_last_sweep_expired = self._as_nonnegative_int(
            snapshot.get("lease_expire_last_sweep_expired")
        )
        self.lease_policy_ticks_total = self._as_nonnegative_int(snapshot.get("lease_policy_ticks_total"))
        self.lease_policy_last_tick_at = self._normalize_optional_str(
            snapshot.get("lease_policy_last_tick_at")
        )
        self.lease_policy_last_preempted = self._as_nonnegative_int(
            snapshot.get("lease_policy_last_preempted")
        )
        self.lease_policy_last_renewed = self._as_nonnegative_int(
            snapshot.get("lease_policy_last_renewed")
        )

    def _state_snapshot_payload(self) -> dict[str, Any]:
        return {
            "version": 1,
            "devices": [asdict(record) for record in self.registry.devices.values()],
            "pairings": [asdict(request) for request in self.pairing.by_code.values()],
            "leases": [asdict(lease) for lease in self.leases.values()],
            "lease_expire_sweeps_total": self.lease_expire_sweeps_total,
            "lease_expired_total": self.lease_expired_total,
            "lease_expire_last_sweep_at": self.lease_expire_last_sweep_at,
            "lease_expire_last_sweep_expired": self.lease_expire_last_sweep_expired,
            "lease_policy_ticks_total": self.lease_policy_ticks_total,
            "lease_policy_last_tick_at": self.lease_policy_last_tick_at,
            "lease_policy_last_preempted": self.lease_policy_last_preempted,
            "lease_policy_last_renewed": self.lease_policy_last_renewed,
        }

    def _persist_state_snapshot(self) -> None:
        if self._state_store is None:
            return
        self._state_store.save_snapshot(self._state_snapshot_payload())

    @staticmethod
    def _rejected_placement(
        run_id: str,
        task_id: str,
        capability: str,
        *,
        reason_code: str = "no_eligible_device",
        reason: str | None = None,
        resource_snapshot: dict[str, Any] | None = None,
        placement_audit: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        message = reason or f"no eligible device for capability '{capability}'"
        decision: dict[str, Any] = {
            "run_id": run_id,
            "task_id": task_id,
            "outcome": "rejected",
            "reason_code": reason_code,
            "reason": message,
            "capability_match": [capability],
        }
        if isinstance(resource_snapshot, dict) and resource_snapshot:
            decision["resource_snapshot"] = dict(resource_snapshot)
        if isinstance(placement_audit, dict) and placement_audit:
            decision["placement_audit"] = dict(placement_audit)
        return decision

    @staticmethod
    def _rejected_capacity(
        run_id: str,
        task_id: str,
        capability: str,
        *,
        eligible_devices: int,
        active_leases: int,
        available_slots: int,
        tenant_id: str | None = None,
        tenant_active_leases: int = 0,
        tenant_limit: int | None = None,
        placement_audit: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        decision: dict[str, Any] = {
            "run_id": run_id,
            "task_id": task_id,
            "outcome": "rejected",
            "reason_code": "capacity_exhausted",
            "reason": "all eligible devices are occupied by active leases",
            "capability_match": [capability],
            "resource_snapshot": _build_allocation_resource_snapshot(
                queue_depth=0,
                eligible_devices=eligible_devices,
                active_leases=active_leases,
                available_slots=available_slots,
                tenant_id=tenant_id,
                tenant_active_leases=tenant_active_leases,
                tenant_limit=tenant_limit,
            ),
        }
        if isinstance(placement_audit, dict) and placement_audit:
            decision["placement_audit"] = dict(placement_audit)
        return decision

    @staticmethod
    def _rejected_tenant_quota(
        run_id: str,
        task_id: str,
        capability: str,
        *,
        tenant_id: str,
        tenant_active_leases: int,
        tenant_limit: int,
        eligible_devices: int,
        active_leases: int,
        available_slots: int,
        placement_audit: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        decision: dict[str, Any] = {
            "run_id": run_id,
            "task_id": task_id,
            "outcome": "rejected",
            "reason_code": "tenant_quota_exhausted",
            "reason": f"tenant '{tenant_id}' reached active lease limit {tenant_limit}",
            "capability_match": [capability],
            "resource_snapshot": _build_allocation_resource_snapshot(
                queue_depth=0,
                eligible_devices=eligible_devices,
                active_leases=active_leases,
                available_slots=available_slots,
                tenant_id=tenant_id,
                tenant_active_leases=tenant_active_leases,
                tenant_limit=tenant_limit,
            ),
        }
        if isinstance(placement_audit, dict) and placement_audit:
            decision["placement_audit"] = dict(placement_audit)
        return decision

    @staticmethod
    def _lease_expiry(lease_ttl_seconds: int) -> str:
        ttl_seconds = max(30, lease_ttl_seconds)
        return (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()

    @staticmethod
    def _parse_iso_datetime(raw: str) -> datetime | None:
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _acquired_placement(
        self,
        *,
        run_id: str,
        task_id: str,
        capability: str,
        trace_id: str,
        device_id: str,
        lease_ttl_seconds: int,
        tenant_id: str | None = None,
        score: float | None = None,
        resource_snapshot: dict[str, Any] | None = None,
        route_reason_code: str | None = None,
        route_reason: str | None = None,
        placement_audit: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.registry.mark_busy(device_id)
        lease_id = f"lease-{uuid4()}"
        lease_expires_at = self._lease_expiry(lease_ttl_seconds)
        self.leases[lease_id] = LeaseRecord(
            lease_id=lease_id,
            run_id=run_id,
            task_id=task_id,
            device_id=device_id,
            capability=capability,
            trace_id=trace_id,
            lease_expires_at=lease_expires_at,
            tenant_id=tenant_id,
        )
        self._index_active_lease(self.leases[lease_id])
        self._persist_state_snapshot()
        decision: dict[str, Any] = {
            "run_id": run_id,
            "task_id": task_id,
            "outcome": "lease_acquired",
            "device_id": device_id,
            "lease_id": lease_id,
            "lease_expires_at": lease_expires_at,
            "capability_match": [capability],
            "trace_id": trace_id,
        }
        if score is not None:
            decision["score"] = score
        if isinstance(resource_snapshot, dict) and resource_snapshot:
            decision["resource_snapshot"] = dict(resource_snapshot)
        if isinstance(placement_audit, dict) and placement_audit:
            decision["placement_audit"] = dict(placement_audit)
        normalized_route_reason_code = normalize_optional_code_term(route_reason_code)
        if normalized_route_reason_code:
            decision["reason_code"] = normalized_route_reason_code
        if route_reason:
            decision["reason"] = route_reason
        return decision

    def register_device(
        self,
        device_id: str,
        capabilities: list[str],
        *,
        execution_site: str = "local",
        region: str | None = None,
        cost_tier: str = "balanced",
        node_pool: str | None = None,
        estimated_cost_usd: float | None = None,
    ) -> DeviceRecord:
        rec = self.registry.register(
            device_id=device_id,
            capabilities=capabilities,
            execution_site=execution_site,
            region=region,
            cost_tier=cost_tier,
            node_pool=node_pool,
            estimated_cost_usd=estimated_cost_usd,
        )
        for capability in capabilities:
            self.capabilities.bind(device_id, capability)
        self._persist_state_snapshot()
        return rec

    def request_pairing(self, device_id: str, ttl_seconds: int = 300) -> PairingRequest:
        if device_id not in self.registry.devices:
            raise ValueError("device not registered")
        request = self.pairing.create_request(device_id=device_id, ttl_seconds=ttl_seconds)
        self._persist_state_snapshot()
        return request

    def approve_pairing(self, code: str) -> DeviceRecord:
        device_id = self.pairing.approve(code)
        record = self.registry.approve_pairing(device_id)
        self._persist_state_snapshot()
        return record

    def receive_heartbeat(self, device_id: str) -> DeviceRecord:
        record = self.registry.heartbeat(device_id)
        self._persist_state_snapshot()
        return record

    def revoke_device(self, device_id: str) -> DeviceRecord:
        self.capabilities.unbind_device(device_id)
        record = self.registry.revoke(device_id)
        self._persist_state_snapshot()
        return record

    def route_capability(
        self, capability: str, load_by_device: dict[str, int] | None = None
    ) -> str | None:
        candidate_ids = self._eligible_devices_for_capability(capability)
        return choose_device(candidate_ids, load_by_device=load_by_device)

    def _eligible_devices_for_capability(self, capability: str) -> list[str]:
        candidate_ids = self.capabilities.candidates(capability)
        active_ids: list[str] = []
        for device_id in candidate_ids:
            rec = self.registry.devices.get(device_id)
            if rec and rec.paired and rec.status in {"paired", "online", "busy", "degraded"}:
                active_ids.append(device_id)
        return active_ids

    def _device_supports_all_capabilities(
        self,
        *,
        device_id: str,
        required_capabilities: set[str],
    ) -> bool:
        rec = self.registry.devices.get(device_id)
        if rec is None:
            return False
        device_capabilities = {cap.strip() for cap in rec.capabilities if isinstance(cap, str) and cap.strip()}
        return required_capabilities.issubset(device_capabilities)

    def _device_has_any_avoided_capability(
        self,
        *,
        device_id: str,
        avoid_capabilities: set[str],
    ) -> bool:
        rec = self.registry.devices.get(device_id)
        if rec is None:
            return False
        device_capabilities = {cap.strip() for cap in rec.capabilities if isinstance(cap, str) and cap.strip()}
        return len(device_capabilities.intersection(avoid_capabilities)) > 0

    def _filter_candidates_by_constraints(
        self,
        *,
        candidate_ids: list[str],
        placement_constraints: dict[str, Any] | None,
    ) -> tuple[list[str], str | None]:
        if not placement_constraints:
            return list(candidate_ids), None
        filtered = list(candidate_ids)
        region = placement_constraints.get("region")
        if isinstance(region, str) and region.strip():
            expected = region.strip().lower()
            filtered = [
                device_id
                for device_id in filtered
                if (self.registry.devices.get(device_id) and (self.registry.devices[device_id].region or "").lower() == expected)
            ]
            if not filtered:
                return [], "region_unavailable"
        node_pool = placement_constraints.get("node_pool")
        if isinstance(node_pool, str) and node_pool.strip():
            expected_pool = node_pool.strip().lower()
            filtered = [
                device_id
                for device_id in filtered
                if (self.registry.devices.get(device_id) and (self.registry.devices[device_id].node_pool or "").lower() == expected_pool)
            ]
            if not filtered:
                return [], "node_pool_unavailable"
        cost_tier = placement_constraints.get("cost_tier")
        if isinstance(cost_tier, str) and cost_tier.strip():
            expected_tier = cost_tier.strip().lower()
            filtered = [
                device_id
                for device_id in filtered
                if (self.registry.devices.get(device_id) and self.registry.devices[device_id].cost_tier.lower() == expected_tier)
            ]
            if not filtered:
                return [], "cost_tier_unavailable"
        max_cost_usd_hard = placement_constraints.get("max_cost_usd_hard")
        if isinstance(max_cost_usd_hard, (int, float)) and not isinstance(max_cost_usd_hard, bool):
            hard_limit = float(max_cost_usd_hard)
            filtered = [
                device_id
                for device_id in filtered
                if (
                    self.registry.devices.get(device_id)
                    and self.registry.devices[device_id].estimated_cost_usd is not None
                    and float(self.registry.devices[device_id].estimated_cost_usd or 0.0) <= hard_limit
                )
            ]
            if not filtered:
                return [], "cost_limit_exceeded"

        required_capabilities = _normalize_constraint_capabilities(
            placement_constraints.get("required_capabilities")
        )
        if required_capabilities:
            filtered = [
                device_id
                for device_id in filtered
                if self._device_supports_all_capabilities(
                    device_id=device_id,
                    required_capabilities=required_capabilities,
                )
            ]
            if not filtered:
                return [], "required_capabilities_unavailable"

        avoid_capabilities = _normalize_constraint_capabilities(
            placement_constraints.get("avoid_capabilities")
        )
        if avoid_capabilities:
            filtered = [
                device_id
                for device_id in filtered
                if not self._device_has_any_avoided_capability(
                    device_id=device_id,
                    avoid_capabilities=avoid_capabilities,
                )
            ]
            if not filtered:
                return [], "avoid_capabilities_excluded"
        return filtered, None

    def _apply_locality_preference(
        self,
        *,
        candidate_ids: list[str],
        placement_constraints: dict[str, Any] | None,
    ) -> tuple[list[str], str | None, str | None]:
        if not candidate_ids:
            return [], None, None
        prefer_local = False
        if isinstance(placement_constraints, dict):
            prefer_local = placement_constraints.get("prefer_local") is True
        if not prefer_local:
            return list(candidate_ids), None, None

        local_ids: list[str] = []
        remote_ids: list[str] = []
        for device_id in candidate_ids:
            rec = self.registry.devices.get(device_id)
            if rec is None:
                continue
            if rec.execution_site == "local":
                local_ids.append(device_id)
            else:
                remote_ids.append(device_id)
        if local_ids:
            return local_ids, None, None
        if remote_ids:
            return (
                remote_ids,
                "local_preference_fallback",
                "no local device available; fallback to non-local device",
            )
        return [], None, None

    def _resolve_capacity(
        self,
        *,
        candidate_ids: list[str],
    ) -> tuple[list[str], int]:
        active_lease_devices = self._active_lease_device_ids()
        active_candidate_devices = {device_id for device_id in candidate_ids if device_id in active_lease_devices}
        available_ids = [device_id for device_id in candidate_ids if device_id not in active_lease_devices]
        return available_ids, len(active_candidate_devices)

    def _device_selection_score(
        self,
        *,
        device_id: str,
        load_by_device: dict[str, int] | None,
        had_fallback: bool,
    ) -> float:
        rec = self.registry.devices.get(device_id)
        queue_depth = 0
        if load_by_device and device_id in load_by_device:
            queue_depth = max(0, int(load_by_device.get(device_id, 0)))
        load_component = 1.0 / (1.0 + float(queue_depth))
        locality_component = 1.0 if rec and rec.execution_site == "local" and not had_fallback else 0.7
        return round(max(0.0, min(1.0, load_component * locality_component)), 6)

    def _policy_selection_load(
        self,
        *,
        device_id: str,
        observed_load: int,
        had_fallback: bool,
    ) -> int:
        rec = self.registry.devices.get(device_id)
        normalized_load = max(0, observed_load)
        locality_penalty = 0
        if rec is not None and rec.execution_site != "local":
            locality_penalty = 5 if had_fallback else 15
        health_penalty = 0
        if rec is not None and rec.status == "degraded":
            health_penalty = 10
        cost_penalty = 0
        if rec is not None and rec.estimated_cost_usd is not None:
            cost_penalty = min(int(round(float(rec.estimated_cost_usd) * 20.0)), 200)
        return normalized_load * 100 + locality_penalty + health_penalty + cost_penalty

    def _policy_load_by_device_for_allocation(
        self,
        *,
        candidate_ids: list[str],
        load_by_device: dict[str, int] | None,
        had_fallback: bool,
    ) -> dict[str, int]:
        policy_load: dict[str, int] = {}
        for device_id in candidate_ids:
            observed = 0
            if load_by_device and device_id in load_by_device:
                observed = max(0, int(load_by_device.get(device_id, 0)))
            policy_load[device_id] = self._policy_selection_load(
                device_id=device_id,
                observed_load=observed,
                had_fallback=had_fallback,
            )
        return policy_load

    @staticmethod
    def _resolve_failure_domain(reason_code: str | None) -> str | None:
        normalized_reason_code = normalize_optional_code_term(reason_code)
        if normalized_reason_code is None:
            return None
        reason_domain_map = {
            "local_preference_fallback": "execution_site",
            "node_pool_fallback": "node_pool",
            "region_unavailable": "region",
            "node_pool_unavailable": "node_pool",
            "cost_tier_unavailable": "cost_tier",
            "cost_limit_exceeded": "cost_limit",
            "required_capabilities_unavailable": "capability",
            "avoid_capabilities_excluded": "capability",
            "no_eligible_device": "capability",
            "capacity_exhausted": "capacity",
            "tenant_quota_exhausted": "tenant_quota",
            "route_unavailable": "route_selector",
            "tenant_context_conflict": "tenant_context",
            "capability_context_conflict": "capability_context",
            "idempotent_replay": "replay",
        }
        return reason_domain_map.get(normalized_reason_code)

    def _build_placement_audit(
        self,
        *,
        candidate_ids: list[str],
        selected_device_id: str | None = None,
        reason_code: str | None = None,
    ) -> dict[str, Any]:
        normalized_reason_code = normalize_optional_code_term(reason_code)
        candidate_execution_sites: set[str] = set()
        for device_id in candidate_ids:
            rec = self.registry.devices.get(device_id)
            if rec is None:
                continue
            if rec.execution_site in {"local", "cloud"}:
                candidate_execution_sites.add(rec.execution_site)
        audit: dict[str, Any] = {
            "candidate_device_count": max(0, len(candidate_ids)),
            "fallback_applied": normalized_reason_code in {"local_preference_fallback", "node_pool_fallback"},
        }
        if candidate_execution_sites:
            audit["candidate_execution_sites"] = sorted(candidate_execution_sites)
        if selected_device_id is not None:
            selected = self.registry.devices.get(selected_device_id)
            if selected is not None:
                audit["selected_device_id"] = selected.device_id
                audit["selected_execution_site"] = selected.execution_site
                if isinstance(selected.region, str) and selected.region:
                    audit["selected_region"] = selected.region
                if isinstance(selected.node_pool, str) and selected.node_pool:
                    audit["selected_node_pool"] = selected.node_pool
        if audit["fallback_applied"] and normalized_reason_code is not None:
            audit["fallback_reason_code"] = normalized_reason_code
        failure_domain = self._resolve_failure_domain(normalized_reason_code)
        if failure_domain is not None:
            audit["failure_domain"] = failure_domain
        return audit

    def _active_lease_device_ids(self) -> set[str]:
        return {lease.device_id for lease in self.leases.values() if lease.status == "active"}

    @staticmethod
    def _run_task_index_key(*, run_id: str, task_id: str) -> tuple[str, str]:
        return run_id, task_id

    def _index_active_lease(self, lease: LeaseRecord) -> None:
        if lease.status != "active":
            return
        key = self._run_task_index_key(run_id=lease.run_id, task_id=lease.task_id)
        self.active_lease_index_by_run_task[key] = lease.lease_id

    def _unindex_active_lease(self, lease: LeaseRecord) -> None:
        key = self._run_task_index_key(run_id=lease.run_id, task_id=lease.task_id)
        current_lease_id = self.active_lease_index_by_run_task.get(key)
        if not isinstance(current_lease_id, str):
            return
        if current_lease_id == lease.lease_id:
            self.active_lease_index_by_run_task.pop(key, None)
            return
        current_lease = self.leases.get(current_lease_id)
        if (
            current_lease is None
            or current_lease.status != "active"
            or current_lease.run_id != lease.run_id
            or current_lease.task_id != lease.task_id
        ):
            self.active_lease_index_by_run_task.pop(key, None)

    def _find_active_lease_for_run_task(self, *, run_id: str, task_id: str) -> LeaseRecord | None:
        key = self._run_task_index_key(run_id=run_id, task_id=task_id)
        indexed_lease_id = self.active_lease_index_by_run_task.get(key)
        if isinstance(indexed_lease_id, str):
            indexed_lease = self.leases.get(indexed_lease_id)
            if (
                indexed_lease is not None
                and indexed_lease.status == "active"
                and indexed_lease.run_id == run_id
                and indexed_lease.task_id == task_id
            ):
                return indexed_lease
            self.active_lease_index_by_run_task.pop(key, None)
        for lease in self.leases.values():
            if lease.status != "active":
                continue
            if lease.run_id == run_id and lease.task_id == task_id:
                self.active_lease_index_by_run_task[key] = lease.lease_id
                return lease
        return None

    def _active_lease_count_for_tenant(self, tenant_id: str) -> int:
        normalized = tenant_id.strip()
        if not normalized:
            return 0
        return sum(
            1
            for lease in self.leases.values()
            if lease.status == "active" and lease.tenant_id == normalized
        )

    def _active_lease_counts_by_tenant(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for lease in self.leases.values():
            if lease.status != "active":
                continue
            tenant_id = lease.tenant_id
            if not isinstance(tenant_id, str):
                continue
            normalized = tenant_id.strip()
            if not normalized:
                continue
            counts[normalized] = counts.get(normalized, 0) + 1
        return counts

    def _resolve_tenant_active_lease_limit(self, tenant_id: str) -> int | None:
        normalized = tenant_id.strip()
        if not normalized:
            return None
        if normalized in self.tenant_active_lease_limits:
            return self.tenant_active_lease_limits[normalized]
        return self.max_active_leases_per_tenant

    def _build_rejection_resource_snapshot(
        self,
        *,
        candidate_ids: list[str],
        tenant_id: str | None,
    ) -> dict[str, Any]:
        normalized_tenant_id = tenant_id.strip() if isinstance(tenant_id, str) else ""
        active_lease_device_ids = self._active_lease_device_ids()
        active_candidate_leases = sum(1 for device_id in candidate_ids if device_id in active_lease_device_ids)
        tenant_active_leases = 0
        tenant_limit = None
        if normalized_tenant_id:
            tenant_active_leases = self._active_lease_count_for_tenant(normalized_tenant_id)
            tenant_limit = self._resolve_tenant_active_lease_limit(normalized_tenant_id)
        return _build_allocation_resource_snapshot(
            queue_depth=0,
            eligible_devices=len(candidate_ids),
            active_leases=active_candidate_leases,
            available_slots=max(len(candidate_ids) - active_candidate_leases, 0),
            tenant_id=normalized_tenant_id or None,
            tenant_active_leases=tenant_active_leases,
            tenant_limit=tenant_limit,
        )

    def _expire_due_leases(self) -> int:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        expired_count = 0
        for lease in self.leases.values():
            if lease.status != "active":
                continue
            expires_at = self._parse_iso_datetime(lease.lease_expires_at)
            if expires_at is None:
                # Defensive recovery: malformed expiry should never pin capacity.
                self._unindex_active_lease(lease)
                lease.status = "expired"
                lease.expired_at = now_iso
                lease.expire_reason_code = "ttl_expired"
                if lease.device_id in self.registry.devices:
                    self.registry.heartbeat(lease.device_id)
                expired_count += 1
                continue
            if expires_at > now:
                continue
            self._unindex_active_lease(lease)
            lease.status = "expired"
            lease.expired_at = now_iso
            lease.expire_reason_code = "ttl_expired"
            if lease.device_id in self.registry.devices:
                self.registry.heartbeat(lease.device_id)
            expired_count += 1
        self.lease_expire_sweeps_total += 1
        self.lease_expire_last_sweep_at = now_iso
        self.lease_expire_last_sweep_expired = expired_count
        if expired_count > 0:
            self.lease_expired_total += expired_count
            self._persist_state_snapshot()
        return expired_count

    def route_command(
        self,
        *,
        capability: str,
        command_type: str,
        payload: dict[str, Any],
        trace_id: str,
        load_by_device: dict[str, int] | None = None,
        decision: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Build a routed command envelope while preserving trace id."""
        if decision is None:
            decision = self.route_command_decision(
                capability=capability,
                command_type=command_type,
                payload=payload,
                trace_id=trace_id,
                load_by_device=load_by_device,
            )
        if decision.get("outcome") != "selected":
            return None
        device_id = str(decision["device_id"])
        score = decision.get("score")
        resource_snapshot = decision.get("resource_snapshot")
        return {
            "command_id": f"cmd-{uuid4()}",
            "command_type": command_type,
            "device_id": device_id,
            "capability": capability,
            "trace_id": trace_id,
            "payload": payload,
            "outcome": "selected",
            "capability_match": [capability],
            "score": score,
            "resource_snapshot": resource_snapshot,
        }

    def route_command_decision(
        self,
        *,
        capability: str,
        command_type: str,
        payload: dict[str, Any],
        trace_id: str,
        load_by_device: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        """Compute structured route decision with traceable resource snapshot."""
        _ = (command_type, payload, trace_id)
        self._expire_due_leases()
        candidate_ids = self._eligible_devices_for_capability(capability)
        if not candidate_ids:
            return {
                "outcome": "rejected",
                "reason_code": "no_eligible_device",
                "reason": f"no eligible device for capability '{capability}'",
                "capability_match": [capability],
                "resource_snapshot": {
                    "eligible_devices": 0,
                    "active_leases": 0,
                    "available_slots": 0,
                    "queue_depth": 0,
                },
                "placement_audit": self._build_placement_audit(
                    candidate_ids=[],
                    reason_code="no_eligible_device",
                ),
            }

        active_lease_devices = self._active_lease_device_ids()
        active_candidate_leases = sum(1 for device_id in candidate_ids if device_id in active_lease_devices)
        available_ids = [device_id for device_id in candidate_ids if device_id not in active_lease_devices]
        device_id = choose_device(candidate_ids, load_by_device=load_by_device)
        if not device_id:
            return {
                "outcome": "rejected",
                "reason_code": "route_unavailable",
                "reason": "unable to select device from eligible route set",
                "capability_match": [capability],
                "resource_snapshot": {
                    "eligible_devices": len(candidate_ids),
                    "active_leases": active_candidate_leases,
                    "available_slots": len(available_ids),
                    "queue_depth": 0,
                },
                "placement_audit": self._build_placement_audit(
                    candidate_ids=candidate_ids,
                    reason_code="route_unavailable",
                ),
            }
        queue_depth = 0
        if load_by_device and device_id in load_by_device:
            queue_depth = max(0, int(load_by_device.get(device_id, 0)))
        score = self._device_selection_score(
            device_id=device_id,
            load_by_device=load_by_device,
            had_fallback=False,
        )
        return {
            "outcome": "selected",
            "device_id": device_id,
            "capability_match": [capability],
            "score": score,
            "resource_snapshot": {
                "eligible_devices": len(candidate_ids),
                "active_leases": active_candidate_leases,
                "available_slots": len(available_ids),
                "queue_depth": queue_depth,
            },
            "placement_audit": self._build_placement_audit(
                candidate_ids=candidate_ids,
                selected_device_id=device_id,
            ),
        }

    def placement_capacity_snapshot(self) -> dict[str, Any]:
        self._expire_due_leases()
        total_devices = len(self.registry.devices)
        eligible_devices = 0
        for rec in self.registry.devices.values():
            if rec.paired and rec.status in {"paired", "online", "busy", "degraded"}:
                eligible_devices += 1

        active_leases = 0
        released_leases = 0
        expired_leases = 0
        for lease in self.leases.values():
            if lease.status == "active":
                active_leases += 1
            elif lease.status == "released":
                released_leases += 1
            elif lease.status == "expired":
                expired_leases += 1

        available_slots = max(eligible_devices - active_leases, 0)
        if eligible_devices > 0:
            lease_utilization = min(1.0, active_leases / eligible_devices)
        else:
            lease_utilization = 0.0
        tenant_active_counts = self._active_lease_counts_by_tenant()
        default_tenant_limit = self.max_active_leases_per_tenant
        tenants_at_limit = 0
        for tenant_id, count in tenant_active_counts.items():
            tenant_limit = self._resolve_tenant_active_lease_limit(tenant_id)
            if tenant_limit is not None and count >= tenant_limit:
                tenants_at_limit += 1
        return {
            "total_devices": total_devices,
            "eligible_devices": eligible_devices,
            "active_leases": active_leases,
            "lease_status_counts": {
                "active": active_leases,
                "released": released_leases,
                "expired": expired_leases,
            },
            "available_slots": available_slots,
            "lease_utilization": lease_utilization,
            "lease_expire_sweeps_total": self.lease_expire_sweeps_total,
            "lease_expired_total": self.lease_expired_total,
            "lease_expire_last_sweep_at": self.lease_expire_last_sweep_at,
            "lease_expire_last_sweep_expired": self.lease_expire_last_sweep_expired,
            "lease_policy": {
                "ticks_total": self.lease_policy_ticks_total,
                "last_tick_at": self.lease_policy_last_tick_at,
                "last_preempted": self.lease_policy_last_preempted,
                "last_renewed": self.lease_policy_last_renewed,
            },
            "tenant_quota": {
                "enabled": (
                    default_tenant_limit is not None or len(self.tenant_active_lease_limits) > 0
                ),
                "max_active_leases_per_tenant": default_tenant_limit,
                "tenant_limit_overrides": dict(self.tenant_active_lease_limits),
                "tenants_with_active_leases": len(tenant_active_counts),
                "max_tenant_active_leases": max(tenant_active_counts.values(), default=0),
                "tenants_at_limit": tenants_at_limit,
            },
            "ts": datetime.now(timezone.utc).isoformat(),
        }

    def allocate_placement(
        self,
        *,
        run_id: str,
        task_id: str,
        capability: str,
        trace_id: str,
        load_by_device: dict[str, int] | None = None,
        lease_ttl_seconds: int = 300,
        tenant_id: str | None = None,
        placement_constraints: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Select a device and mint a short-lived lease descriptor."""
        self._expire_due_leases()
        normalized_tenant_id = tenant_id.strip() if isinstance(tenant_id, str) else ""
        existing_active_lease = self._find_active_lease_for_run_task(run_id=run_id, task_id=task_id)
        if existing_active_lease is not None:
            snapshot_capability = existing_active_lease.capability or capability
            snapshot_candidate_ids = self._eligible_devices_for_capability(snapshot_capability)
            snapshot_available_ids, snapshot_active_candidate_leases = self._resolve_capacity(
                candidate_ids=snapshot_candidate_ids
            )
            snapshot_queue_depth = 0
            if load_by_device and existing_active_lease.device_id in load_by_device:
                snapshot_queue_depth = max(0, int(load_by_device.get(existing_active_lease.device_id, 0)))
            snapshot_tenant_id = normalized_tenant_id or (existing_active_lease.tenant_id or "")
            snapshot_tenant_active_leases = 0
            snapshot_tenant_limit = None
            if snapshot_tenant_id:
                snapshot_tenant_active_leases = self._active_lease_count_for_tenant(snapshot_tenant_id)
                snapshot_tenant_limit = self._resolve_tenant_active_lease_limit(snapshot_tenant_id)
            replay_resource_snapshot = _build_allocation_resource_snapshot(
                queue_depth=snapshot_queue_depth,
                eligible_devices=len(snapshot_candidate_ids),
                active_leases=snapshot_active_candidate_leases,
                available_slots=len(snapshot_available_ids),
                tenant_id=snapshot_tenant_id or None,
                tenant_active_leases=snapshot_tenant_active_leases,
                tenant_limit=snapshot_tenant_limit,
            )
            requested_capability = capability.strip()
            existing_capability = snapshot_capability.strip()
            if requested_capability and existing_capability and requested_capability != existing_capability:
                return self._rejected_placement(
                    run_id,
                    task_id,
                    capability,
                    reason_code="capability_context_conflict",
                    reason=(
                        "active lease capability mismatch for identical run/task allocation request"
                    ),
                    resource_snapshot=replay_resource_snapshot,
                    placement_audit=self._build_placement_audit(
                        candidate_ids=snapshot_candidate_ids,
                        selected_device_id=existing_active_lease.device_id,
                        reason_code="capability_context_conflict",
                    ),
                )
            existing_tenant_id = (
                existing_active_lease.tenant_id.strip()
                if isinstance(existing_active_lease.tenant_id, str)
                else ""
            )
            if normalized_tenant_id and existing_tenant_id and normalized_tenant_id != existing_tenant_id:
                return self._rejected_placement(
                    run_id,
                    task_id,
                    capability,
                    reason_code="tenant_context_conflict",
                    reason=(
                        "active lease tenant context mismatch for identical run/task allocation request"
                    ),
                    resource_snapshot=replay_resource_snapshot,
                    placement_audit=self._build_placement_audit(
                        candidate_ids=snapshot_candidate_ids,
                        selected_device_id=existing_active_lease.device_id,
                        reason_code="tenant_context_conflict",
                    ),
                )
            replay_decision: dict[str, Any] = {
                "run_id": run_id,
                "task_id": task_id,
                "outcome": "lease_acquired",
                "device_id": existing_active_lease.device_id,
                "lease_id": existing_active_lease.lease_id,
                "lease_expires_at": existing_active_lease.lease_expires_at,
                "capability_match": [snapshot_capability],
                "trace_id": existing_active_lease.trace_id,
                "resource_snapshot": replay_resource_snapshot,
                "reason_code": "idempotent_replay",
                "reason": "active lease reused for identical run/task allocation request",
                "placement_audit": self._build_placement_audit(
                    candidate_ids=snapshot_candidate_ids,
                    selected_device_id=existing_active_lease.device_id,
                    reason_code="idempotent_replay",
                ),
            }
            return replay_decision

        eligible_ids = self._eligible_devices_for_capability(capability)
        if not eligible_ids:
            return self._rejected_placement(
                run_id,
                task_id,
                capability,
                resource_snapshot=self._build_rejection_resource_snapshot(
                    candidate_ids=[],
                    tenant_id=normalized_tenant_id,
                ),
                placement_audit=self._build_placement_audit(
                    candidate_ids=[],
                    reason_code="no_eligible_device",
                ),
            )

        fallback_reason_code: str | None = None
        fallback_reason: str | None = None
        constrained_ids, constraint_reason_code = self._filter_candidates_by_constraints(
            candidate_ids=eligible_ids,
            placement_constraints=placement_constraints,
        )
        if not constrained_ids:
            if constraint_reason_code == "region_unavailable":
                return self._rejected_placement(
                    run_id,
                    task_id,
                    capability,
                    reason_code="region_unavailable",
                    reason="no eligible device matches placement_constraints.region",
                    resource_snapshot=self._build_rejection_resource_snapshot(
                        candidate_ids=eligible_ids,
                        tenant_id=normalized_tenant_id,
                    ),
                    placement_audit=self._build_placement_audit(
                        candidate_ids=eligible_ids,
                        reason_code="region_unavailable",
                    ),
                )
            if constraint_reason_code == "node_pool_unavailable":
                fallback_ids, _ = self._filter_candidates_by_constraints(
                    candidate_ids=eligible_ids,
                    placement_constraints=_placement_constraints_without_node_pool(
                        placement_constraints
                    ),
                )
                if fallback_ids:
                    constrained_ids = fallback_ids
                    fallback_reason_code = "node_pool_fallback"
                    fallback_reason = (
                        "no eligible device matches placement_constraints.node_pool; "
                        "fallback to alternate node_pool"
                    )
                else:
                    return self._rejected_placement(
                        run_id,
                        task_id,
                        capability,
                        reason_code="node_pool_unavailable",
                        reason="no eligible device matches placement_constraints.node_pool",
                        resource_snapshot=self._build_rejection_resource_snapshot(
                            candidate_ids=eligible_ids,
                            tenant_id=normalized_tenant_id,
                        ),
                        placement_audit=self._build_placement_audit(
                            candidate_ids=eligible_ids,
                            reason_code="node_pool_unavailable",
                        ),
                    )
            if constraint_reason_code == "cost_tier_unavailable":
                return self._rejected_placement(
                    run_id,
                    task_id,
                    capability,
                    reason_code="cost_tier_unavailable",
                    reason="no eligible device matches placement_constraints.cost_tier",
                    resource_snapshot=self._build_rejection_resource_snapshot(
                        candidate_ids=eligible_ids,
                        tenant_id=normalized_tenant_id,
                    ),
                    placement_audit=self._build_placement_audit(
                        candidate_ids=eligible_ids,
                        reason_code="cost_tier_unavailable",
                    ),
                )
            if constraint_reason_code == "cost_limit_exceeded":
                return self._rejected_placement(
                    run_id,
                    task_id,
                    capability,
                    reason_code="cost_limit_exceeded",
                    reason="no eligible device satisfies placement_constraints.max_cost_usd_hard",
                    resource_snapshot=self._build_rejection_resource_snapshot(
                        candidate_ids=eligible_ids,
                        tenant_id=normalized_tenant_id,
                    ),
                    placement_audit=self._build_placement_audit(
                        candidate_ids=eligible_ids,
                        reason_code="cost_limit_exceeded",
                    ),
                )
            if constraint_reason_code == "required_capabilities_unavailable":
                return self._rejected_placement(
                    run_id,
                    task_id,
                    capability,
                    reason_code="required_capabilities_unavailable",
                    reason="no eligible device satisfies placement_constraints.required_capabilities",
                    resource_snapshot=self._build_rejection_resource_snapshot(
                        candidate_ids=eligible_ids,
                        tenant_id=normalized_tenant_id,
                    ),
                    placement_audit=self._build_placement_audit(
                        candidate_ids=eligible_ids,
                        reason_code="required_capabilities_unavailable",
                    ),
                )
            if constraint_reason_code == "avoid_capabilities_excluded":
                return self._rejected_placement(
                    run_id,
                    task_id,
                    capability,
                    reason_code="avoid_capabilities_excluded",
                    reason="all eligible devices are excluded by placement_constraints.avoid_capabilities",
                    resource_snapshot=self._build_rejection_resource_snapshot(
                        candidate_ids=eligible_ids,
                        tenant_id=normalized_tenant_id,
                    ),
                    placement_audit=self._build_placement_audit(
                        candidate_ids=eligible_ids,
                        reason_code="avoid_capabilities_excluded",
                    ),
                )
            if not constrained_ids:
                return self._rejected_placement(
                    run_id,
                    task_id,
                    capability,
                    resource_snapshot=self._build_rejection_resource_snapshot(
                        candidate_ids=eligible_ids,
                        tenant_id=normalized_tenant_id,
                    ),
                    placement_audit=self._build_placement_audit(
                        candidate_ids=eligible_ids,
                        reason_code="no_eligible_device",
                    ),
                )

        constrained_pool_ids = list(constrained_ids)
        constrained_ids, locality_reason_code, locality_reason = self._apply_locality_preference(
            candidate_ids=constrained_ids,
            placement_constraints=placement_constraints,
        )
        if fallback_reason_code is None and locality_reason_code is not None:
            fallback_reason_code = locality_reason_code
            fallback_reason = locality_reason
        if not constrained_ids:
            return self._rejected_placement(
                run_id,
                task_id,
                capability,
                resource_snapshot=self._build_rejection_resource_snapshot(
                    candidate_ids=constrained_pool_ids,
                    tenant_id=normalized_tenant_id,
                ),
                placement_audit=self._build_placement_audit(
                    candidate_ids=constrained_pool_ids,
                    reason_code="no_eligible_device",
                ),
            )

        tenant_limit = self._resolve_tenant_active_lease_limit(normalized_tenant_id)
        tenant_active_leases = 0
        if normalized_tenant_id:
            tenant_active_leases = self._active_lease_count_for_tenant(normalized_tenant_id)
        if tenant_limit is not None:
            if tenant_active_leases >= tenant_limit:
                quota_available_ids, quota_active_candidate_leases = self._resolve_capacity(
                    candidate_ids=constrained_ids
                )
                return self._rejected_tenant_quota(
                    run_id,
                    task_id,
                    capability,
                    tenant_id=normalized_tenant_id,
                    tenant_active_leases=tenant_active_leases,
                    tenant_limit=tenant_limit,
                    eligible_devices=len(constrained_ids),
                    active_leases=quota_active_candidate_leases,
                    available_slots=len(quota_available_ids),
                    placement_audit=self._build_placement_audit(
                        candidate_ids=constrained_ids,
                        reason_code="tenant_quota_exhausted",
                    ),
                )
        available_ids, active_candidate_leases = self._resolve_capacity(candidate_ids=constrained_ids)
        prefer_local = (
            isinstance(placement_constraints, dict)
            and placement_constraints.get("prefer_local") is True
        )
        if not available_ids and prefer_local and fallback_reason_code is None:
            remote_candidate_ids = [
                device_id
                for device_id in constrained_pool_ids
                if (
                    self.registry.devices.get(device_id)
                    and self.registry.devices[device_id].execution_site != "local"
                )
            ]
            if remote_candidate_ids:
                remote_available_ids, remote_active_leases = self._resolve_capacity(
                    candidate_ids=remote_candidate_ids
                )
                if remote_available_ids:
                    available_ids = remote_available_ids
                    active_candidate_leases = remote_active_leases
                    fallback_reason_code = "local_preference_fallback"
                    fallback_reason = (
                        "no local device has free capacity; fallback to non-local device"
                    )
        if not available_ids:
            capacity_candidate_ids = constrained_pool_ids if prefer_local else constrained_ids
            capacity_active_lease_devices = self._active_lease_device_ids()
            capacity_active_leases = sum(
                1 for device_id in capacity_candidate_ids if device_id in capacity_active_lease_devices
            )
            return self._rejected_capacity(
                run_id,
                task_id,
                capability,
                eligible_devices=len(capacity_candidate_ids),
                active_leases=capacity_active_leases,
                available_slots=max(len(capacity_candidate_ids) - capacity_active_leases, 0),
                tenant_id=normalized_tenant_id or None,
                tenant_active_leases=tenant_active_leases,
                tenant_limit=tenant_limit,
                placement_audit=self._build_placement_audit(
                    candidate_ids=capacity_candidate_ids,
                    reason_code="capacity_exhausted",
                ),
            )
        policy_load_by_device = self._policy_load_by_device_for_allocation(
            candidate_ids=available_ids,
            load_by_device=load_by_device,
            had_fallback=fallback_reason_code is not None,
        )
        device_id = choose_device(available_ids, load_by_device=policy_load_by_device)
        if not device_id:
            return self._rejected_placement(
                run_id,
                task_id,
                capability,
                reason_code="route_unavailable",
                reason="unable to select device from eligible route set",
                resource_snapshot=self._build_rejection_resource_snapshot(
                    candidate_ids=constrained_ids,
                    tenant_id=normalized_tenant_id,
                ),
                placement_audit=self._build_placement_audit(
                    candidate_ids=constrained_ids,
                    reason_code="route_unavailable",
                ),
            )
        queue_depth = 0
        if load_by_device and device_id in load_by_device:
            queue_depth = max(0, int(load_by_device.get(device_id, 0)))
        score = self._device_selection_score(
            device_id=device_id,
            load_by_device=policy_load_by_device,
            had_fallback=fallback_reason_code is not None,
        )
        resource_snapshot = _build_allocation_resource_snapshot(
            queue_depth=queue_depth,
            eligible_devices=len(constrained_ids),
            active_leases=active_candidate_leases,
            available_slots=len(available_ids),
            tenant_id=normalized_tenant_id or None,
            tenant_active_leases=tenant_active_leases,
            tenant_limit=tenant_limit,
        )
        return self._acquired_placement(
            run_id=run_id,
            task_id=task_id,
            capability=capability,
            trace_id=trace_id,
            device_id=device_id,
            lease_ttl_seconds=lease_ttl_seconds,
            tenant_id=normalized_tenant_id or None,
            score=score,
            resource_snapshot=resource_snapshot,
            route_reason_code=fallback_reason_code,
            route_reason=fallback_reason,
            placement_audit=self._build_placement_audit(
                candidate_ids=constrained_ids,
                selected_device_id=device_id,
                reason_code=fallback_reason_code,
            ),
        )

    def release_lease(self, lease_id: str) -> dict[str, Any]:
        self._expire_due_leases()
        lease = self.leases.get(lease_id)
        if lease is None:
            raise KeyError(f"lease not found: {lease_id}")
        if lease.status == "released":
            self._unindex_active_lease(lease)
            self._persist_state_snapshot()
            return {
                "run_id": lease.run_id,
                "task_id": lease.task_id,
                "outcome": "lease_released",
                "device_id": lease.device_id,
                "lease_id": lease.lease_id,
            }
        if lease.status == "expired":
            self._unindex_active_lease(lease)
            self._persist_state_snapshot()
            raise ValueError("lease already expired")

        self._unindex_active_lease(lease)
        lease.status = "released"
        lease.released_at = datetime.now(timezone.utc).isoformat()
        if lease.device_id in self.registry.devices:
            self.registry.heartbeat(lease.device_id)
        self._persist_state_snapshot()
        return {
            "run_id": lease.run_id,
            "task_id": lease.task_id,
            "outcome": "lease_released",
            "device_id": lease.device_id,
            "lease_id": lease.lease_id,
        }

    def expire_lease(self, lease_id: str, *, reason_code: str = "ttl_expired") -> dict[str, Any]:
        normalized_reason_code = normalize_optional_code_term(reason_code)
        if normalized_reason_code is None:
            raise ValueError("reason_code must be non-empty string")
        self._expire_due_leases()
        lease = self.leases.get(lease_id)
        if lease is None:
            raise KeyError(f"lease not found: {lease_id}")
        if lease.status == "released":
            self._unindex_active_lease(lease)
            self._persist_state_snapshot()
            raise ValueError("lease already released")
        if lease.status == "expired":
            self._unindex_active_lease(lease)
            existing_reason_code = normalize_optional_code_term(lease.expire_reason_code)
            self._persist_state_snapshot()
            return {
                "run_id": lease.run_id,
                "task_id": lease.task_id,
                "outcome": "lease_expired",
                "device_id": lease.device_id,
                "lease_id": lease.lease_id,
                "reason_code": existing_reason_code or normalized_reason_code,
            }

        self._unindex_active_lease(lease)
        lease.status = "expired"
        lease.expired_at = datetime.now(timezone.utc).isoformat()
        lease.expire_reason_code = normalized_reason_code
        if lease.device_id in self.registry.devices:
            self.registry.heartbeat(lease.device_id)
        self._persist_state_snapshot()
        return {
            "run_id": lease.run_id,
            "task_id": lease.task_id,
            "outcome": "lease_expired",
            "device_id": lease.device_id,
            "lease_id": lease.lease_id,
            "reason_code": normalized_reason_code,
        }

    def preempt_lease(
        self,
        lease_id: str,
        *,
        reason_code: str = "preempted_by_policy",
    ) -> dict[str, Any]:
        normalized_reason_code = normalize_optional_code_term(reason_code)
        if normalized_reason_code is None:
            raise ValueError("reason_code must be non-empty string")
        return self.expire_lease(lease_id, reason_code=normalized_reason_code)

    def renew_lease(self, lease_id: str, *, lease_ttl_seconds: int = 300) -> dict[str, Any]:
        self._expire_due_leases()
        if (
            not isinstance(lease_ttl_seconds, int)
            or isinstance(lease_ttl_seconds, bool)
            or lease_ttl_seconds < 30
            or lease_ttl_seconds > 3600
        ):
            raise ValueError("lease_ttl_seconds must be integer in [30, 3600]")
        lease = self.leases.get(lease_id)
        if lease is None:
            raise KeyError(f"lease not found: {lease_id}")
        if lease.status == "released":
            raise ValueError("lease already released")
        if lease.status == "expired":
            raise ValueError("lease already expired")

        lease.lease_expires_at = self._lease_expiry(lease_ttl_seconds)
        self._persist_state_snapshot()
        return {
            "run_id": lease.run_id,
            "task_id": lease.task_id,
            "outcome": "lease_renewed",
            "device_id": lease.device_id,
            "lease_id": lease.lease_id,
            "lease_expires_at": lease.lease_expires_at,
        }

    def lease_policy_tick(
        self,
        *,
        auto_renew_window_seconds: int = 0,
        auto_renew_ttl_seconds: int = 300,
        enforce_tenant_quota: bool = True,
        preempt_reason_code: str = "preempted_by_policy",
        max_preemptions: int = 256,
    ) -> dict[str, Any]:
        if (
            not isinstance(auto_renew_window_seconds, int)
            or isinstance(auto_renew_window_seconds, bool)
            or auto_renew_window_seconds < 0
            or auto_renew_window_seconds > 3600
        ):
            raise ValueError("auto_renew_window_seconds must be integer in [0, 3600]")
        if (
            not isinstance(auto_renew_ttl_seconds, int)
            or isinstance(auto_renew_ttl_seconds, bool)
            or auto_renew_ttl_seconds < 30
            or auto_renew_ttl_seconds > 3600
        ):
            raise ValueError("auto_renew_ttl_seconds must be integer in [30, 3600]")
        if (
            not isinstance(max_preemptions, int)
            or isinstance(max_preemptions, bool)
            or max_preemptions < 0
            or max_preemptions > 1024
        ):
            raise ValueError("max_preemptions must be integer in [0, 1024]")
        normalized_preempt_reason = normalize_optional_code_term(preempt_reason_code)
        if normalized_preempt_reason is None:
            raise ValueError("preempt_reason_code must be non-empty string")

        active_leases_before = sum(1 for lease in self.leases.values() if lease.status == "active")
        expired_by_sweep = self._expire_due_leases()

        renew_considered = 0
        renewed = 0
        if auto_renew_window_seconds > 0:
            renew_cutoff = datetime.now(timezone.utc) + timedelta(seconds=auto_renew_window_seconds)
            for lease in self.leases.values():
                if lease.status != "active":
                    continue
                expires_at = self._parse_iso_datetime(lease.lease_expires_at)
                if expires_at is None or expires_at > renew_cutoff:
                    continue
                renew_considered += 1
                lease.lease_expires_at = self._lease_expiry(auto_renew_ttl_seconds)
                renewed += 1

        preempted_leases: list[dict[str, Any]] = []
        if enforce_tenant_quota and max_preemptions > 0:
            preemption_budget = max_preemptions
            tenant_counts = self._active_lease_counts_by_tenant()
            for tenant_id in sorted(tenant_counts):
                if preemption_budget <= 0:
                    break
                tenant_limit = self._resolve_tenant_active_lease_limit(tenant_id)
                if tenant_limit is None:
                    continue
                active_count = tenant_counts.get(tenant_id, 0)
                if active_count <= tenant_limit:
                    continue
                overflow = min(active_count - tenant_limit, preemption_budget)
                tenant_leases = [
                    lease
                    for lease in self.leases.values()
                    if lease.status == "active" and lease.tenant_id == tenant_id
                ]
                tenant_leases.sort(
                    key=lambda lease: (
                        self._parse_iso_datetime(lease.lease_expires_at) or datetime.max.replace(tzinfo=timezone.utc),
                        lease.lease_id,
                    )
                )
                for lease in tenant_leases[:overflow]:
                    self._unindex_active_lease(lease)
                    lease.status = "expired"
                    lease.expired_at = datetime.now(timezone.utc).isoformat()
                    lease.expire_reason_code = normalized_preempt_reason
                    if lease.device_id in self.registry.devices:
                        self.registry.heartbeat(lease.device_id)
                    preempted_leases.append(
                        {
                            "tenant_id": tenant_id,
                            "run_id": lease.run_id,
                            "task_id": lease.task_id,
                            "device_id": lease.device_id,
                            "lease_id": lease.lease_id,
                            "reason_code": normalized_preempt_reason,
                        }
                    )
                    preemption_budget -= 1
                    if preemption_budget <= 0:
                        break

        active_leases_after = sum(1 for lease in self.leases.values() if lease.status == "active")
        self.lease_policy_ticks_total += 1
        self.lease_policy_last_tick_at = datetime.now(timezone.utc).isoformat()
        self.lease_policy_last_preempted = len(preempted_leases)
        self.lease_policy_last_renewed = renewed
        self._persist_state_snapshot()

        return {
            "expired_by_sweep": expired_by_sweep,
            "auto_renew_window_seconds": auto_renew_window_seconds,
            "auto_renew_ttl_seconds": auto_renew_ttl_seconds,
            "renew_considered": renew_considered,
            "renewed": renewed,
            "enforce_tenant_quota": enforce_tenant_quota,
            "preempt_reason_code": normalized_preempt_reason,
            "max_preemptions": max_preemptions,
            "preempted": len(preempted_leases),
            "preempted_leases": preempted_leases,
            "active_leases_before": active_leases_before,
            "active_leases_after": active_leases_after,
            "tenant_active_counts_after": self._active_lease_counts_by_tenant(),
            "policy_ticks_total": self.lease_policy_ticks_total,
            "policy_last_tick_at": self.lease_policy_last_tick_at,
        }

    def get_lease_snapshot(self, lease_id: str) -> dict[str, Any]:
        self._expire_due_leases()
        lease = self.leases.get(lease_id)
        if lease is None:
            raise KeyError(f"lease not found: {lease_id}")
        return {
            "lease_id": lease.lease_id,
            "run_id": lease.run_id,
            "task_id": lease.task_id,
            "device_id": lease.device_id,
            "capability": lease.capability,
            "trace_id": lease.trace_id,
            "lease_expires_at": lease.lease_expires_at,
            "tenant_id": lease.tenant_id,
            "status": lease.status,
            "released_at": lease.released_at,
            "expired_at": lease.expired_at,
            "expire_reason_code": lease.expire_reason_code,
        }


def _normalize_tenant_active_lease_limits(raw: dict[str, int] | None) -> dict[str, int]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("tenant_active_lease_limits must be object mapping tenant_id to positive integer")
    normalized_limits: dict[str, int] = {}
    for tenant_id, limit in raw.items():
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise ValueError("tenant_active_lease_limits keys must be non-empty strings")
        if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
            raise ValueError(
                "tenant_active_lease_limits values must be positive integers"
            )
        normalized_limits[tenant_id.strip()] = limit
    return normalized_limits


def _normalize_constraint_capabilities(raw: Any) -> set[str]:
    if not isinstance(raw, list):
        return set()
    normalized: set[str] = set()
    for capability in raw:
        if isinstance(capability, str) and capability.strip():
            normalized.add(capability.strip())
    return normalized


def _build_allocation_resource_snapshot(
    *,
    queue_depth: int,
    eligible_devices: int,
    active_leases: int,
    available_slots: int,
    tenant_id: str | None,
    tenant_active_leases: int,
    tenant_limit: int | None,
) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "queue_depth": max(0, int(queue_depth)),
        "eligible_devices": max(0, int(eligible_devices)),
        "active_leases": max(0, int(active_leases)),
        "available_slots": max(0, int(available_slots)),
    }
    if isinstance(tenant_id, str) and tenant_id.strip():
        snapshot["tenant_id"] = tenant_id.strip()
        snapshot["tenant_active_leases"] = max(0, int(tenant_active_leases))
        if isinstance(tenant_limit, int) and tenant_limit > 0:
            snapshot["tenant_limit"] = tenant_limit
    return snapshot


def _placement_constraints_without_node_pool(
    placement_constraints: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(placement_constraints, dict):
        return None
    stripped = dict(placement_constraints)
    stripped.pop("node_pool", None)
    return stripped
