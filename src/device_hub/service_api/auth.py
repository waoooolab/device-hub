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


_DEFAULT_TOKEN_SECRET = "dev-insecure-secret"
_ALLOWED_TOKEN_USES = {"access", "service", "device"}
_DEFAULT_ALLOWED_TOKEN_ISSUERS = {"runtime-gateway", "control-gateway"}
STRICT_TOKEN_SECRET_ENV = "DEVICE_HUB_STRICT_TOKEN_SECRET"
STRICT_TOKEN_SECRET_ENV_LEGACY = "WAOOOOLAB_STRICT_TOKEN_SECRET"
ALLOWED_TOKEN_ISSUERS_ENV = "DEVICE_HUB_ALLOWED_TOKEN_ISSUERS"
ALLOWED_TOKEN_ISSUERS_ENV_LEGACY = "WAOOOOLAB_DEVICE_HUB_ALLOWED_TOKEN_ISSUERS"


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _secret() -> bytes:
    value = os.environ.get("RUNTIME_GATEWAY_TOKEN_SECRET", _DEFAULT_TOKEN_SECRET)
    if _env_truthy_from_candidates(
        STRICT_TOKEN_SECRET_ENV,
        STRICT_TOKEN_SECRET_ENV_LEGACY,
        default=False,
    ) and value == _DEFAULT_TOKEN_SECRET:
        raise TokenError(
            f"insecure default token secret is forbidden when {STRICT_TOKEN_SECRET_ENV}=true"
        )
    return value.encode("utf-8")


def _allowed_token_issuers() -> set[str]:
    raw = _env_value_from_candidates(
        ALLOWED_TOKEN_ISSUERS_ENV,
        ALLOWED_TOKEN_ISSUERS_ENV_LEGACY,
    )
    if raw is None:
        return set(_DEFAULT_ALLOWED_TOKEN_ISSUERS)
    parsed = {item.strip() for item in raw.split(",") if item.strip()}
    if not parsed:
        return set(_DEFAULT_ALLOWED_TOKEN_ISSUERS)
    return parsed


def _env_value_from_candidates(*names: str) -> str | None:
    for name in names:
        raw = os.environ.get(name)
        if raw is not None:
            return raw
    return None


def _env_truthy_from_candidates(*names: str, default: bool = False) -> bool:
    raw = _env_value_from_candidates(*names)
    if raw is not None:
        return raw.strip().lower() in {"1", "true", "yes", "on"}
    return default


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

        for field in ("tenant_id", "app_id", "trace_id"):
            value = claims.get(field)
            if not isinstance(value, str) or not value.strip():
                raise HTTPException(status_code=401, detail=f"invalid token claims: missing {field}")
        token_use = claims.get("token_use")
        if not isinstance(token_use, str) or not token_use.strip():
            raise HTTPException(status_code=401, detail="invalid token claims: missing token_use")
        if token_use.strip().lower() not in _ALLOWED_TOKEN_USES:
            raise HTTPException(
                status_code=401,
                detail=f"invalid token claims: unsupported token_use '{token_use}'",
            )
        issuer = claims.get("iss")
        if not isinstance(issuer, str) or not issuer.strip():
            raise HTTPException(status_code=401, detail="invalid token claims: missing iss")
        if issuer.strip() not in _allowed_token_issuers():
            raise HTTPException(
                status_code=401,
                detail=f"invalid token claims: unsupported issuer '{issuer.strip()}'",
            )

        return claims

    return _dependency
