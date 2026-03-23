from __future__ import annotations

from pathlib import Path

from device_hub.persistence_paths import resolve_device_state_db_path


def test_resolve_device_state_db_path_prefers_explicit_override(monkeypatch) -> None:
    monkeypatch.setenv("DEVICE_HUB_STATE_DB_PATH", "~/owa/device-hub.sqlite")
    monkeypatch.setenv("OWA_PERSIST_ROOT", "/tmp/owa-persist")
    path = resolve_device_state_db_path()
    assert path == Path.home() / "owa" / "device-hub.sqlite"


def test_resolve_device_state_db_path_falls_back_to_persist_root(monkeypatch) -> None:
    monkeypatch.delenv("DEVICE_HUB_STATE_DB_PATH", raising=False)
    monkeypatch.setenv("OWA_PERSIST_ROOT", "/tmp/owa-persist")
    path = resolve_device_state_db_path()
    assert path == Path("/tmp/owa-persist/device-hub/device-state.sqlite")


def test_resolve_device_state_db_path_returns_none_when_unconfigured(monkeypatch) -> None:
    monkeypatch.delenv("DEVICE_HUB_STATE_DB_PATH", raising=False)
    monkeypatch.delenv("OWA_PERSIST_ROOT", raising=False)
    assert resolve_device_state_db_path() is None
