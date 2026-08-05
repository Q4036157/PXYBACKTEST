from __future__ import annotations

import hmac
from dataclasses import dataclass

from fastapi import Header, HTTPException, status

from .config import Settings


@dataclass(frozen=True)
class TrustedIdentity:
    user_id: str
    source_node: str


def build_identity_dependency(settings: Settings):
    async def require_identity(
        x_pxy_service_token: str = Header(default=""),
        x_pxy_user_id: str = Header(default=""),
        x_pxy_source_node: str = Header(default="unknown"),
    ) -> TrustedIdentity:
        if not settings.service_token:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="PXYBACKTEST service token is not configured",
            )
        if not hmac.compare_digest(x_pxy_service_token, settings.service_token):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid backtest service credential",
            )
        user_id = x_pxy_user_id.strip()
        if not user_id or len(user_id) > 128:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="trusted PXYLH user identity is required",
            )
        return TrustedIdentity(
            user_id=user_id,
            source_node=x_pxy_source_node.strip()[:64] or "unknown",
        )

    return require_identity

