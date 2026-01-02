"""
监控告警路由
"""

from aiohttp import web
from aiohttp_cors import CorsConfig

from handlers.monitoring import MonitoringHandler


def setup_monitoring_routes(app: web.Application, cors: CorsConfig = None):
    """设置监控告警路由
    
    Args:
        app: aiohttp应用实例
        cors: CORS配置对象
    """
    
    # 创建处理器实例
    monitoring_handler = MonitoringHandler()
    
    # 存储到应用上下文
    app['monitoring_handler'] = monitoring_handler
    
    # 获取监控指标
    route = app.router.add_get('/api/v1/monitoring/metrics', monitoring_handler.get_metrics)
    if cors:
        cors.add(route)
    
    # 获取告警信息
    route = app.router.add_get('/api/v1/monitoring/alerts', monitoring_handler.get_alerts)
    if cors:
        cors.add(route)

    # 确认告警
    route = app.router.add_post('/api/v1/monitoring/alerts/ack', monitoring_handler.ack_alert)
    if cors:
        cors.add(route)
    route = app.router.add_post('/api/v1/monitoring/alerts/{alert_id}/ack', monitoring_handler.ack_alert_by_id)
    if cors:
        cors.add(route)
    route = app.router.add_post('/api/v1/monitoring/alerts/{alert_id}/silence', monitoring_handler.silence_alert)
    if cors:
        cors.add(route)
    
    # 获取系统性能指标
    route = app.router.add_get('/api/v1/monitoring/performance', monitoring_handler.get_performance)
    if cors:
        cors.add(route)
    
    # 获取服务健康状态
    route = app.router.add_get('/api/v1/monitoring/health', monitoring_handler.get_health_status)
    if cors:
        cors.add(route)
    
    # 设置告警规则
    route = app.router.add_post('/api/v1/monitoring/rules', monitoring_handler.set_alert_rule)
    if cors:
        cors.add(route)
    
    # 获取告警规则
    route = app.router.add_get('/api/v1/monitoring/rules', monitoring_handler.get_alert_rules)
    if cors:
        cors.add(route)
