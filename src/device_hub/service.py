"""Device-hub application service."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from .devices.pairing import PairingManager, PairingRequest
from .devices.registry import DeviceRecord, DeviceRegistry
from .resources.capability_registry import CapabilityRegistry
from .routing.device_router import choose_device


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
    lease_expire_sweeps_total: int = 0
    lease_expired_total: int = 0
    lease_expire_last_sweep_at: str | None = None
    lease_expire_last_sweep_expired: int = 0

    @staticmethod
    def _rejected_placement(
        run_id: str,
        task_id: str,
        capability: str,
        *,
        reason_code: str = "no_eligible_device",
        reason: str | None = None,
    ) -> dict[str, Any]:
        message = reason or f"no eligible device for capability '{capability}'"
        return {
            "run_id": run_id,
            "task_id": task_id,
            "outcome": "rejected",
            "reason_code": reason_code,
            "reason": message,
            "capability_match": [capability],
        }

    @staticmethod
    def _rejected_capacity(
        run_id: str,
        task_id: str,
        capability: str,
        *,
        eligible_devices: int,
        active_leases: int,
    ) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "task_id": task_id,
            "outcome": "rejected",
            "reason_code": "capacity_exhausted",
            "reason": "all eligible devices are occupied by active leases",
            "capability_match": [capability],
            "resource_snapshot": {
                "eligible_devices": eligible_devices,
                "active_leases": active_leases,
                "available_slots": max(eligible_devices - active_leases, 0),
            },
        }

    @staticmethod
    def _rejected_tenant_quota(
        run_id: str,
        task_id: str,
        capability: str,
        *,
        tenant_id: str,
        tenant_active_leases: int,
        tenant_limit: int,
    ) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "task_id": task_id,
            "outcome": "rejected",
            "reason_code": "tenant_quota_exhausted",
            "reason": f"tenant '{tenant_id}' reached active lease limit {tenant_limit}",
            "capability_match": [capability],
            "resource_snapshot": {
                "tenant_id": tenant_id,
                "tenant_active_leases": tenant_active_leases,
                "tenant_limit": tenant_limit,
            },
        }

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
        if route_reason_code:
            decision["reason_code"] = route_reason_code
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
        return rec

    def request_pairing(self, device_id: str, ttl_seconds: int = 300) -> PairingRequest:
        if device_id not in self.registry.devices:
            raise ValueError("device not registered")
        return self.pairing.create_request(device_id=device_id, ttl_seconds=ttl_seconds)

    def approve_pairing(self, code: str) -> DeviceRecord:
        device_id = self.pairing.approve(code)
        return self.registry.approve_pairing(device_id)

    def receive_heartbeat(self, device_id: str) -> DeviceRecord:
        return self.registry.heartbeat(device_id)

    def revoke_device(self, device_id: str) -> DeviceRecord:
        self.capabilities.unbind_device(device_id)
        return self.registry.revoke(device_id)

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

    def _active_lease_device_ids(self) -> set[str]:
        return {lease.device_id for lease in self.leases.values() if lease.status == "active"}

    def _active_lease_count_for_tenant(self, tenant_id: str) -> int:
        normalized = tenant_id.strip()
        if not normalized:
            return 0
        return sum(
            1
            for lease in self.leases.values()
            if lease.status == "active" and lease.tenant_id == normalized
        )

    def _expire_due_leases(self) -> int:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        expired_count = 0
        for lease in self.leases.values():
            if lease.status != "active":
                continue
            expires_at = self._parse_iso_datetime(lease.lease_expires_at)
            if expires_at is None or expires_at > now:
                continue
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
        return expired_count

    def route_command(
        self,
        *,
        capability: str,
        command_type: str,
        payload: dict[str, Any],
        trace_id: str,
        load_by_device: dict[str, int] | None = None,
    ) -> dict[str, Any] | None:
        """Build a routed command envelope while preserving trace id."""
        device_id = self.route_capability(capability, load_by_device=load_by_device)
        if not device_id:
            return None
        return {
            "command_id": f"cmd-{uuid4()}",
            "command_type": command_type,
            "device_id": device_id,
            "capability": capability,
            "trace_id": trace_id,
            "payload": payload,
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
            lease_utilization = 1.0
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
        eligible_ids = self._eligible_devices_for_capability(capability)
        if not eligible_ids:
            return self._rejected_placement(run_id, task_id, capability)

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
                )
            if constraint_reason_code == "node_pool_unavailable":
                return self._rejected_placement(
                    run_id,
                    task_id,
                    capability,
                    reason_code="node_pool_unavailable",
                    reason="no eligible device matches placement_constraints.node_pool",
                )
            if constraint_reason_code == "cost_tier_unavailable":
                return self._rejected_placement(
                    run_id,
                    task_id,
                    capability,
                    reason_code="cost_tier_unavailable",
                    reason="no eligible device matches placement_constraints.cost_tier",
                )
            if constraint_reason_code == "cost_limit_exceeded":
                return self._rejected_placement(
                    run_id,
                    task_id,
                    capability,
                    reason_code="cost_limit_exceeded",
                    reason="no eligible device satisfies placement_constraints.max_cost_usd_hard",
                )
            return self._rejected_placement(run_id, task_id, capability)

        constrained_pool_ids = list(constrained_ids)
        constrained_ids, fallback_reason_code, fallback_reason = self._apply_locality_preference(
            candidate_ids=constrained_ids,
            placement_constraints=placement_constraints,
        )
        if not constrained_ids:
            return self._rejected_placement(run_id, task_id, capability)

        normalized_tenant_id = tenant_id.strip() if isinstance(tenant_id, str) else ""
        tenant_limit = self.max_active_leases_per_tenant
        if tenant_limit is not None and tenant_limit > 0 and normalized_tenant_id:
            tenant_active_leases = self._active_lease_count_for_tenant(normalized_tenant_id)
            if tenant_active_leases >= tenant_limit:
                return self._rejected_tenant_quota(
                    run_id,
                    task_id,
                    capability,
                    tenant_id=normalized_tenant_id,
                    tenant_active_leases=tenant_active_leases,
                    tenant_limit=tenant_limit,
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
            capacity_eligible = len(constrained_pool_ids) if prefer_local else len(constrained_ids)
            return self._rejected_capacity(
                run_id,
                task_id,
                capability,
                eligible_devices=capacity_eligible,
                active_leases=active_candidate_leases,
            )
        device_id = choose_device(available_ids, load_by_device=load_by_device)
        if not device_id:
            return self._rejected_placement(run_id, task_id, capability)
        queue_depth = 0
        if load_by_device and device_id in load_by_device:
            queue_depth = max(0, int(load_by_device.get(device_id, 0)))
        score = self._device_selection_score(
            device_id=device_id,
            load_by_device=load_by_device,
            had_fallback=fallback_reason_code is not None,
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
            resource_snapshot={"queue_depth": queue_depth},
            route_reason_code=fallback_reason_code,
            route_reason=fallback_reason,
        )

    def release_lease(self, lease_id: str) -> dict[str, Any]:
        self._expire_due_leases()
        lease = self.leases.get(lease_id)
        if lease is None:
            raise KeyError(f"lease not found: {lease_id}")
        if lease.status == "released":
            return {
                "run_id": lease.run_id,
                "task_id": lease.task_id,
                "outcome": "lease_released",
                "device_id": lease.device_id,
                "lease_id": lease.lease_id,
            }
        if lease.status == "expired":
            raise ValueError("lease already expired")

        lease.status = "released"
        lease.released_at = datetime.now(timezone.utc).isoformat()
        if lease.device_id in self.registry.devices:
            self.registry.heartbeat(lease.device_id)
        return {
            "run_id": lease.run_id,
            "task_id": lease.task_id,
            "outcome": "lease_released",
            "device_id": lease.device_id,
            "lease_id": lease.lease_id,
        }

    def expire_lease(self, lease_id: str, *, reason_code: str = "ttl_expired") -> dict[str, Any]:
        self._expire_due_leases()
        lease = self.leases.get(lease_id)
        if lease is None:
            raise KeyError(f"lease not found: {lease_id}")
        if lease.status == "released":
            raise ValueError("lease already released")
        if lease.status == "expired":
            return {
                "run_id": lease.run_id,
                "task_id": lease.task_id,
                "outcome": "lease_expired",
                "device_id": lease.device_id,
                "lease_id": lease.lease_id,
                "reason_code": lease.expire_reason_code or reason_code,
            }

        lease.status = "expired"
        lease.expired_at = datetime.now(timezone.utc).isoformat()
        lease.expire_reason_code = reason_code
        if lease.device_id in self.registry.devices:
            self.registry.heartbeat(lease.device_id)
        return {
            "run_id": lease.run_id,
            "task_id": lease.task_id,
            "outcome": "lease_expired",
            "device_id": lease.device_id,
            "lease_id": lease.lease_id,
            "reason_code": reason_code,
        }

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
        return {
            "run_id": lease.run_id,
            "task_id": lease.task_id,
            "outcome": "lease_renewed",
            "device_id": lease.device_id,
            "lease_id": lease.lease_id,
            "lease_expires_at": lease.lease_expires_at,
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
