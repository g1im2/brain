"""
Unified UI BFF proxy handler.
"""

from aiohttp import web

from handlers.base import BaseHandler


HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
}

CORS_HEADERS = {
    "access-control-allow-origin",
    "access-control-allow-credentials",
    "access-control-allow-methods",
    "access-control-allow-headers",
    "access-control-expose-headers",
    "access-control-max-age",
}


class UIBffHandler(BaseHandler):
    """Brain-side frontend BFF for /api/v1/ui/*."""

    def _resolve_service(self, path: str) -> str | None:
        if path.startswith("/api/v1/ui/market-snapshot"):
            return "macro"
        if path.startswith("/api/v1/ui/macro-cycle"):
            return "macro"
        if path.startswith("/api/v1/ui/structure-rotation"):
            return "macro"
        if path.startswith("/api/v1/ui/candidates"):
            return "execution"
        if path.startswith("/api/v1/ui/research"):
            return "execution"
        if path.startswith("/api/v1/ui/strategy"):
            return "execution"
        if path.startswith("/api/v1/ui/execution/sim"):
            return "portfolio"
        return None

    async def proxy(self, request: web.Request) -> web.Response:
        service_name = self._resolve_service(request.path)
        if not service_name:
            return self.error_response("Unsupported /api/v1/ui path", 404)

        registry = self.get_app_component(request, "service_registry")
        service = getattr(registry, "_services", {}).get(service_name)
        session = getattr(registry, "_session", None)
        if not service or session is None:
            return self.error_response(f"Service not available: {service_name}", 503)

        target_url = f"{service['url'].rstrip('/')}{request.path_qs}"
        body = await request.read() if request.can_read_body else None
        headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}

        try:
            async with session.request(request.method, target_url, headers=headers, data=body) as resp:
                response_body = await resp.read()
                response_headers = {
                    k: v
                    for k, v in resp.headers.items()
                    if k.lower() not in HOP_BY_HOP_HEADERS and k.lower() not in CORS_HEADERS
                }
                return web.Response(status=resp.status, headers=response_headers, body=response_body)
        except Exception as exc:
            self.logger.error(f"UI BFF proxy failed: {exc}")
            return self.error_response("UI upstream request failed", 502)
