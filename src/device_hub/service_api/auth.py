"""Bearer claims verification utilities for device-hub boundary."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any

from fastapi import Header, HTTPException


class TokenError(ValueError):
    """Token verification or parsing error."""


def _secret() -> bytes:
    value = os.environ.get("RUNTIME_GATEWAY_TOKEN_SECRET", "dev-insecure-secret")
    return value.encode("utf-8")


def _b64url_decode(raw: str) -> bytes:
    pad = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(raw + pad)


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _sign(payload_part: str) -> str:
    digest = hmac.new(_secret(), payload_part.encode("utf-8"), hashlib.sha256).digest()
    return _b64url_encode(digest)


def verify_token(token: str, audience: str | None = None) -> dict[str, Any]:
    """Verify token signature, expiry and optional audience."""
    try:
        payload_part, signature_part = token.split(".", 1)
    except ValueError as exc:
        raise TokenError("invalid token format") from exc

    expected = _sign(payload_part)
    if not hmac.compare_digest(signature_part, expected):
        raise TokenError("invalid token signature")

    try:
        payload = json.loads(_b64url_decode(payload_part).decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
        raise TokenError("invalid token payload") from exc

    exp = payload.get("exp")
    if not isinstance(exp, int) or exp <= int(time.time()):
        raise TokenError("token expired")

    if audience:
        aud = payload.get("aud")
        if isinstance(aud, str):
            ok = aud == audience
        elif isinstance(aud, list):
            ok = audience in aud
        else:
            ok = False
        if not ok:
            raise TokenError("token audience mismatch")

    return payload


def require_claims(*, audience: str, required_scope: str):
    """Build FastAPI dependency for device-hub claim validation."""

    def _dependency(authorization: str | None = Header(default=None)) -> dict[str, Any]:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="missing bearer token")

        token = authorization.split(" ", 1)[1].strip()
        try:
            claims = verify_token(token, audience=audience)
        except TokenError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc

        scope = claims.get("scope", [])
        if not isinstance(scope, list) or required_scope not in scope:
            raise HTTPException(status_code=403, detail=f"missing required scope: {required_scope}")

        trace_id = claims.get("trace_id")
        if not isinstance(trace_id, str) or not trace_id:
            raise HTTPException(status_code=401, detail="invalid token claims: missing trace_id")

        return claims

    return _dependency
