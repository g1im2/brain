"""
数据流管理路由
"""

from aiohttp import web
from aiohttp_cors import CorsConfig

from handlers.dataflow import DataFlowHandler


def setup_dataflow_routes(app: web.Application, cors: CorsConfig = None):
    """设置数据流管理路由
    
    Args:
        app: aiohttp应用实例
        cors: CORS配置对象
    """
    
    # 创建处理器实例
    dataflow_handler = DataFlowHandler()
    
    # 存储到应用上下文
    app['dataflow_handler'] = dataflow_handler
    
    # 获取数据流状态
    route = app.router.add_get('/api/v1/dataflow/status', dataflow_handler.get_status)
    if cors:
        cors.add(route)
    
    # 优化数据流
    route = app.router.add_post('/api/v1/dataflow/optimize', dataflow_handler.optimize)
    if cors:
        cors.add(route)
    
    # 获取数据流指标
    route = app.router.add_get('/api/v1/dataflow/metrics', dataflow_handler.get_metrics)
    if cors:
        cors.add(route)
    
    # 清理缓存
    route = app.router.add_post('/api/v1/dataflow/cache/clear', dataflow_handler.clear_cache)
    if cors:
        cors.add(route)
    
    # 获取缓存统计
    route = app.router.add_get('/api/v1/dataflow/cache/stats', dataflow_handler.get_cache_stats)
    if cors:
        cors.add(route)
    
    # 触发数据同步
    route = app.router.add_post('/api/v1/dataflow/sync', dataflow_handler.trigger_sync)
    if cors:
        cors.add(route)
