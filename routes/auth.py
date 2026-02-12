"""Authentication routes."""

from aiohttp import web
from aiohttp_cors import CorsConfig

from handlers.auth import AuthHandler


def setup_auth_routes(app: web.Application, cors: CorsConfig = None):
    handler = AuthHandler()

    route = app.router.add_post('/api/v1/ui/auth/login', handler.login)
    if cors:
        cors.add(route)

    route = app.router.add_post('/api/v1/ui/auth/refresh', handler.refresh)
    if cors:
        cors.add(route)

    route = app.router.add_post('/api/v1/ui/auth/logout', handler.logout)
    if cors:
        cors.add(route)

    route = app.router.add_get('/api/v1/ui/auth/me', handler.me)
    if cors:
        cors.add(route)
