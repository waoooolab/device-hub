"""Device pairing management."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from secrets import randbelow


@dataclass
class PairingRequest:
    code: str
    device_id: str
    expires_at: str


@dataclass
class PairingManager:
    by_code: dict[str, PairingRequest] = field(default_factory=dict)

    def create_request(self, device_id: str, ttl_seconds: int = 300, code_length: int = 6) -> PairingRequest:
        if code_length < 4:
            raise ValueError("code_length must be >= 4")
        max_value = 10 ** code_length
        code = str(randbelow(max_value)).zfill(code_length)
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        req = PairingRequest(code=code, device_id=device_id, expires_at=expires_at)
        self.by_code[code] = req
        return req

    def approve(self, code: str, now: datetime | None = None) -> str:
        req = self.by_code.get(code)
        if not req:
            raise ValueError("pairing code not found")
        ts = now or datetime.now(timezone.utc)
        expires_at = datetime.fromisoformat(req.expires_at)
        if ts > expires_at:
            del self.by_code[code]
            raise ValueError("pairing code expired")
        del self.by_code[code]
        return req.device_id
