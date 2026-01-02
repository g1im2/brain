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


class ProxyHandler(BaseHandler):
    """服务代理处理器"""

    def _resolve_service(self, path: str) -> str | None:
        parts = [p for p in path.split('/') if p]
        if len(parts) < 3:
            return None
        scope = parts[2]
        if scope in ('macro', 'market', 'theories'):
            return 'macro'
        if scope in ('analyze', 'backtest', 'strategy', 'realtime', 'quantum'):
            return 'execution'
        if scope in ('portfolio', 'portfolios'):
            return 'portfolio'
        if scope in ('tasks', 'sources', 'jobs'):
            return 'flowhub'
        return None

    async def proxy(self, request: web.Request) -> web.Response:
        """转发请求到目标服务"""
        service_name = self._resolve_service(request.path)
        if not service_name:
            return self.error_response("不支持的代理路径", 404)

        registry = self.get_app_component(request, 'service_registry')
        service = getattr(registry, '_services', {}).get(service_name)
        if not service:
            return self.error_response(f"服务未注册: {service_name}", 404)

        target_base = service['url'].rstrip('/')
        target_url = f"{target_base}{request.path_qs}"

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
                    k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS
                }
                return web.Response(status=resp.status, headers=response_headers, body=body)
        except Exception as e:
            self.logger.error(f"Proxy request failed: {e}")
            return self.error_response("代理请求失败", 502)
