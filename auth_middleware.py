"""Auth guard middleware for Brain UI endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import Callable

from aiohttp import web
from aiohttp.web_middlewares import middleware

from auth_service import AuthError


_AUTH_ALLOWLIST = {
    "/api/v1/ui/auth/login",
    "/api/v1/ui/auth/refresh",
    "/api/v1/ui/auth/logout",
}


@middleware
async def ui_auth_guard_middleware(request: web.Request, handler: Callable) -> web.Response:
    path = request.path
    if request.method.upper() == "OPTIONS":
        return await handler(request)
    if not path.startswith("/api/v1/ui/"):
        return await handler(request)
    if path in _AUTH_ALLOWLIST:
        return await handler(request)

    auth_service = request.app.get("auth_service")
    if auth_service is None:
        return web.json_response(
            {
                "success": False,
                "error": "Auth service not initialized",
                "error_code": "AUTH_SERVICE_MISSING",
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
            status=500,
        )

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return web.json_response(
            {
                "success": False,
                "error": "Missing or invalid Authorization header",
                "error_code": "UNAUTHORIZED",
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
            status=401,
        )

    token = auth_header[7:].strip()
    if not token:
        return web.json_response(
            {
                "success": False,
                "error": "Missing bearer token",
                "error_code": "UNAUTHORIZED",
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
            status=401,
        )

    try:
        auth_context = await auth_service.resolve_access_token(token)
        request["auth_claims"] = auth_context.get("claims")
        request["current_user"] = auth_context.get("user")
        return await handler(request)
    except AuthError as exc:
        return web.json_response(
            {
                "success": False,
                "error": exc.message,
                "error_code": exc.code,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
            status=exc.status,
        )
    except Exception as exc:
        return web.json_response(
            {
                "success": False,
                "error": f"Authorization failed: {exc}",
                "error_code": "UNAUTHORIZED",
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
            status=401,
        )
