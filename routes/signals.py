"""
信号路由管理路由
"""

from aiohttp import web
from aiohttp_cors import CorsConfig

from handlers.signals import SignalHandler


def setup_signal_routes(app: web.Application, cors: CorsConfig = None):
    """设置信号路由管理路由
    
    Args:
        app: aiohttp应用实例
        cors: CORS配置对象
    """
    
    # 创建处理器实例
    signal_handler = SignalHandler()
    
    # 存储到应用上下文
    app['signal_handler'] = signal_handler
    
    # 路由信号
    route = app.router.add_post('/api/v1/signals/route', signal_handler.route_signal)
    if cors:
        cors.add(route)
    
    # 获取信号冲突
    route = app.router.add_get('/api/v1/signals/conflicts', signal_handler.get_conflicts)
    if cors:
        cors.add(route)
    
    # 解决信号冲突
    route = app.router.add_post('/api/v1/signals/resolve', signal_handler.resolve_conflicts)
    if cors:
        cors.add(route)
    
    # 获取信号历史
    route = app.router.add_get('/api/v1/signals/history', signal_handler.get_signal_history)
    if cors:
        cors.add(route)
    
    # 获取信号统计
    route = app.router.add_get('/api/v1/signals/stats', signal_handler.get_signal_stats)
    if cors:
        cors.add(route)
    
    # 清理过期信号
    route = app.router.add_post('/api/v1/signals/cleanup', signal_handler.cleanup_expired_signals)
    if cors:
        cors.add(route)
