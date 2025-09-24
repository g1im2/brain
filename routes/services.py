"""
服务管理路由
"""

from aiohttp import web
from aiohttp_cors import CorsConfig

from handlers.services import ServiceHandler


def setup_service_routes(app: web.Application, cors: CorsConfig = None):
    """设置服务管理路由
    
    Args:
        app: aiohttp应用实例
        cors: CORS配置对象
    """
    
    # 创建处理器实例
    service_handler = ServiceHandler()
    
    # 存储到应用上下文
    app['service_handler'] = service_handler
    
    # 获取服务列表
    route = app.router.add_get('/api/v1/services', service_handler.list_services)
    if cors:
        cors.add(route)
    
    # 获取服务状态
    route = app.router.add_get('/api/v1/services/{service}/status', service_handler.get_service_status)
    if cors:
        cors.add(route)
    
    # 服务健康检查
    route = app.router.add_post('/api/v1/services/{service}/health-check', service_handler.health_check)
    if cors:
        cors.add(route)
    
    # 重启服务连接
    route = app.router.add_post('/api/v1/services/{service}/reconnect', service_handler.reconnect_service)
    if cors:
        cors.add(route)
    
    # 获取服务配置
    route = app.router.add_get('/api/v1/services/{service}/config', service_handler.get_service_config)
    if cors:
        cors.add(route)
