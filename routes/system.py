"""
系统协调路由
"""

from aiohttp import web
from aiohttp_cors import CorsConfig

from ..handlers.system import SystemHandler


def setup_system_routes(app: web.Application, cors: CorsConfig = None):
    """设置系统协调路由
    
    Args:
        app: aiohttp应用实例
        cors: CORS配置对象
    """
    
    # 创建处理器实例
    system_handler = SystemHandler()
    
    # 存储到应用上下文
    app['system_handler'] = system_handler
    
    # 系统启动
    route = app.router.add_post('/api/v1/system/startup', system_handler.startup)
    if cors:
        cors.add(route)
    
    # 系统关闭
    route = app.router.add_post('/api/v1/system/shutdown', system_handler.shutdown)
    if cors:
        cors.add(route)
    
    # 获取系统状态
    route = app.router.add_get('/api/v1/system/status', system_handler.get_status)
    if cors:
        cors.add(route)
    
    # 触发分析周期
    route = app.router.add_post('/api/v1/system/analysis/trigger', system_handler.trigger_analysis)
    if cors:
        cors.add(route)
    
    # 获取分析周期历史
    route = app.router.add_get('/api/v1/system/analysis/history', system_handler.get_analysis_history)
    if cors:
        cors.add(route)
    
    # 获取系统资源状态
    route = app.router.add_get('/api/v1/system/resources', system_handler.get_resources)
    if cors:
        cors.add(route)
