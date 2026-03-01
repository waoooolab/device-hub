"""Heartbeat freshness checks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .registry import DeviceRegistry


def is_stale(last_seen_at: str, timeout_seconds: int, now: datetime | None = None) -> bool:
    ts = now or datetime.now(timezone.utc)
    last = datetime.fromisoformat(last_seen_at)
    return ts - last > timedelta(seconds=timeout_seconds)


def refresh_presence(registry: DeviceRegistry, timeout_seconds: int, now: datetime | None = None) -> None:
    ts = now or datetime.now(timezone.utc)
    for rec in registry.devices.values():
        if rec.status in {"online", "busy", "degraded", "paired"} and is_stale(
            rec.last_seen_at, timeout_seconds=timeout_seconds, now=ts
        ):
            rec.status = "offline"
