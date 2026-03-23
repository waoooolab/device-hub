"""SQLite-backed snapshot store for device-hub runtime state."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any


class DeviceHubStateStore:
    """Persist and restore service-local device/lease state snapshots."""

    def __init__(self, db_path: str | Path):
        self._db_path = Path(db_path).expanduser()
        self._lock = Lock()

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _connect(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._db_path.as_posix())
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS device_hub_state_snapshot (
                snapshot_key TEXT PRIMARY KEY,
                snapshot_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

    def load_snapshot(self) -> dict[str, Any] | None:
        with self._lock:
            with self._connect() as conn:
                self._ensure_schema(conn)
                row = conn.execute(
                    """
                    SELECT snapshot_json
                    FROM device_hub_state_snapshot
                    WHERE snapshot_key = 'default'
                    """,
                ).fetchone()
        if row is None:
            return None
        raw_payload = row["snapshot_json"]
        if not isinstance(raw_payload, str) or not raw_payload.strip():
            return None
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def save_snapshot(self, snapshot: dict[str, Any]) -> None:
        payload = json.dumps(
            dict(snapshot),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            with self._connect() as conn:
                self._ensure_schema(conn)
                conn.execute(
                    """
                    INSERT INTO device_hub_state_snapshot (snapshot_key, snapshot_json, updated_at)
                    VALUES ('default', ?, ?)
                    ON CONFLICT(snapshot_key) DO UPDATE SET
                        snapshot_json = excluded.snapshot_json,
                        updated_at = excluded.updated_at
                    """,
                    (payload, now_iso),
                )
                conn.commit()
