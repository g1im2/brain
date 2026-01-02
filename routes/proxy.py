"""
统一代理路由
"""

from aiohttp import web
from aiohttp_cors import CorsConfig

from handlers.proxy import ProxyHandler


def setup_proxy_routes(app: web.Application, cors: CorsConfig = None):
    """设置代理路由"""
    handler = ProxyHandler()

    patterns = [
        # Macro/Market/Theory
        '/api/v1/macro',
        '/api/v1/macro/{tail:.*}',
        '/api/v1/market',
        '/api/v1/market/{tail:.*}',
        '/api/v1/theories',
        '/api/v1/theories/{tail:.*}',
        # Execution
        '/api/v1/analyze',
        '/api/v1/analyze/{tail:.*}',
        '/api/v1/backtest',
        '/api/v1/backtest/{tail:.*}',
        '/api/v1/strategy',
        '/api/v1/strategy/{tail:.*}',
        '/api/v1/realtime',
        '/api/v1/realtime/{tail:.*}',
        '/api/v1/quantum',
        '/api/v1/quantum/{tail:.*}',
        # Portfolio
        '/api/v1/portfolio',
        '/api/v1/portfolio/{tail:.*}',
        '/api/v1/portfolios',
        '/api/v1/portfolios/{tail:.*}',
        # Flowhub
        '/api/v1/tasks',
        '/api/v1/tasks/{tail:.*}',
        '/api/v1/sources',
        '/api/v1/sources/{tail:.*}',
        '/api/v1/jobs',
        '/api/v1/jobs/{tail:.*}',
    ]

    routes = []
    methods = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD']
    for pattern in patterns:
        for method in methods:
            try:
                route = app.router.add_route(method, pattern, handler.proxy)
                routes.append(route)
            except Exception:
                # 若已存在相同路由，跳过重复注册
                continue

    if cors:
        for route in routes:
            cors.add(route)
