"""
统一代理处理器

将前端的多服务请求转发到 brain 管理的各服务。
"""

from aiohttp import web

from handlers.base import BaseHandler


HOP_BY_HOP_HEADERS = {
    'connection',
    'keep-alive',
    'proxy-authenticate',
    'proxy-authorization',
    'te',
    'trailer',
    'transfer-encoding',
    'upgrade',
    'host',
}

CORS_HEADERS = {
    'access-control-allow-origin',
    'access-control-allow-credentials',
    'access-control-allow-methods',
    'access-control-allow-headers',
    'access-control-expose-headers',
    'access-control-max-age',
}


class ProxyHandler(BaseHandler):
    """服务代理处理器"""

    def _resolve_service(self, path: str) -> tuple[str | None, str]:
        parts = [p for p in path.split('/') if p]
        if len(parts) < 3:
            return None, path
        scope = parts[2]
        if scope == 'flowhub':
            return 'flowhub', path.replace('/api/v1/flowhub', '/api/v1', 1)
        if scope in ('macro', 'market', 'theories'):
            return 'macro', path
        if scope in ('analyze', 'backtest', 'strategy', 'realtime', 'quantum', 'alpha'):
            return 'execution', path
        if scope in ('portfolio', 'portfolios'):
            return 'portfolio', path
        return None, path

    async def proxy(self, request: web.Request) -> web.Response:
        """转发请求到目标服务"""
        service_name, rewritten_path = self._resolve_service(request.path)
        if not service_name:
            return self.error_response("不支持的代理路径", 404)

        registry = self.get_app_component(request, 'service_registry')
        service = getattr(registry, '_services', {}).get(service_name)
        if not service:
            return self.error_response(f"服务未注册: {service_name}", 404)

        target_base = service['url'].rstrip('/')
        if rewritten_path != request.path:
            target_path_qs = request.path_qs.replace(request.path, rewritten_path, 1)
        else:
            target_path_qs = request.path_qs
        target_url = f"{target_base}{target_path_qs}"

        session = getattr(registry, '_session', None)
        if session is None:
            return self.error_response("代理会话未初始化", 503)

        data = None
        if request.can_read_body:
            data = await request.read()

        headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}

        try:
            async with session.request(
                request.method,
                target_url,
                headers=headers,
                data=data,
            ) as resp:
                body = await resp.read()
                response_headers = {
                    k: v
                    for k, v in resp.headers.items()
                    if k.lower() not in HOP_BY_HOP_HEADERS and k.lower() not in CORS_HEADERS
                }
                return web.Response(status=resp.status, headers=response_headers, body=body)
        except Exception as e:
            self.logger.error(f"Proxy request failed: {e}")
            return self.error_response("代理请求失败", 502)
