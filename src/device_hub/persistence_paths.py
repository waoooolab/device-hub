"""Persistence path resolution helpers for device-hub."""

from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path

from .contracts_catalog_runtime import normalized_env, resolve_catalog_data_path

_PERSISTENCE_PATHS_DATA_PATH = "catalog/runtime/persistence-paths.data.v1.json"
_SERVICE_KEY = "device-hub"

_DEFAULT_PERSIST_ROOT_ENV = "OWA_PERSIST_ROOT"
_DEFAULT_PATHS: dict[str, tuple[str, str]] = {
    "device_state_db": (
        "DEVICE_HUB_STATE_DB_PATH",
        "device-hub/device-state.sqlite",
    ),
}


def _catalog_data_path() -> Path:
    return resolve_catalog_data_path(
        anchor_file=__file__,
        relative_path=_PERSISTENCE_PATHS_DATA_PATH,
    )


@lru_cache(maxsize=1)
def _load_service_spec() -> dict[str, object]:
    path = _catalog_data_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    services = payload.get("services")
    if not isinstance(services, dict):
        return {}
    spec = services.get(_SERVICE_KEY)
    if not isinstance(spec, dict):
        return {}
    return spec


def _persist_root_env_name() -> str:
    spec = _load_service_spec()
    raw = spec.get("persist_root_env")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return _DEFAULT_PERSIST_ROOT_ENV


def _path_config(path_key: str) -> tuple[str, str]:
    default = _DEFAULT_PATHS[path_key]
    spec = _load_service_spec()
    raw_paths = spec.get("paths")
    if not isinstance(raw_paths, dict):
        return default
    raw_path = raw_paths.get(path_key)
    if not isinstance(raw_path, dict):
        return default
    explicit_env = raw_path.get("explicit_env")
    relative_path = raw_path.get("relative_path")
    explicit = (
        explicit_env.strip()
        if isinstance(explicit_env, str) and explicit_env.strip()
        else default[0]
    )
    relative = (
        relative_path.strip()
        if isinstance(relative_path, str) and relative_path.strip()
        else default[1]
    )
    return explicit, relative


def _persist_root() -> Path | None:
    raw = normalized_env(_persist_root_env_name())
    if raw is None:
        return None
    return Path(raw).expanduser()


def resolve_device_state_db_path() -> Path | None:
    explicit_env, relative_path = _path_config("device_state_db")
    explicit = normalized_env(explicit_env)
    if explicit is not None:
        return Path(explicit).expanduser()
    root = _persist_root()
    if root is None:
        return None
    return root / Path(relative_path)
