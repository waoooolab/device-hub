"""Simple device routing policy."""

from __future__ import annotations


def choose_device(
    candidate_device_ids: list[str], load_by_device: dict[str, int] | None = None
) -> str | None:
    if not candidate_device_ids:
        return None
    if not load_by_device:
        # P0 deterministic policy: lexical minimum.
        return sorted(candidate_device_ids)[0]
    # P1 baseline policy: min load first, lexical tiebreak.
    ranked = sorted(candidate_device_ids, key=lambda d: (load_by_device.get(d, 0), d))
    return ranked[0]
