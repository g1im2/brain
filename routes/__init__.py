"""
Integration Service 路由模块

设置所有API路由和WebSocket连接。
"""

from aiohttp import web
from aiohttp_cors import CorsConfig

from .health import setup_health_routes
from .system import setup_system_routes
from .services import setup_service_routes
from .signals import setup_signal_routes
from .dataflow import setup_dataflow_routes
from .tasks import setup_task_routes
from .monitoring import setup_monitoring_routes


def setup_routes(app: web.Application, cors: CorsConfig = None):
    """设置所有路由
    
    Args:
        app: aiohttp应用实例
        cors: CORS配置对象
    """
    
    # 健康检查路由
    setup_health_routes(app, cors)
    
    # 系统协调路由
    setup_system_routes(app, cors)
    
    # 服务管理路由
    setup_service_routes(app, cors)
    
    # 信号路由管理
    setup_signal_routes(app, cors)
    
    # 数据流管理路由
    setup_dataflow_routes(app, cors)
    
    # 定时任务管理路由
    setup_task_routes(app, cors)
    
    # 监控告警路由
    setup_monitoring_routes(app, cors)
    
    # 静态文件路由（如果需要）
    # app.router.add_static('/', path='static', name='static')


def get_api_info():
    """获取API信息"""
    return {
        'name': 'Integration Service API',
        'version': '1.0.0',
        'description': '三层金融交易系统集成服务API',
        'endpoints': {
            'health': {
                'GET /health': '基础健康检查',
                'GET /api/v1/status': '详细状态信息'
            },
            'system': {
                'POST /api/v1/system/startup': '启动系统',
                'POST /api/v1/system/shutdown': '关闭系统',
                'GET /api/v1/system/status': '获取系统状态',
                'POST /api/v1/system/analysis/trigger': '触发分析周期'
            },
            'services': {
                'GET /api/v1/services': '获取服务列表',
                'GET /api/v1/services/{service}/status': '获取服务状态',
                'POST /api/v1/services/{service}/health-check': '服务健康检查'
            },
            'signals': {
                'POST /api/v1/signals/route': '路由信号',
                'GET /api/v1/signals/conflicts': '获取信号冲突',
                'POST /api/v1/signals/resolve': '解决信号冲突'
            },
            'dataflow': {
                'GET /api/v1/dataflow/status': '数据流状态',
                'POST /api/v1/dataflow/optimize': '优化数据流',
                'GET /api/v1/dataflow/metrics': '数据流指标'
            },
            'tasks': {
                'GET /api/v1/tasks': '获取任务列表',
                'POST /api/v1/tasks': '创建定时任务',
                'PUT /api/v1/tasks/{task_id}': '更新任务',
                'DELETE /api/v1/tasks/{task_id}': '删除任务',
                'POST /api/v1/tasks/{task_id}/trigger': '手动触发任务'
            },
            'monitoring': {
                'GET /api/v1/monitoring/metrics': '获取监控指标',
                'GET /api/v1/monitoring/alerts': '获取告警信息',
                'POST /api/v1/monitoring/alerts/ack': '确认告警'
            }
        }
    }
