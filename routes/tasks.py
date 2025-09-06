"""
定时任务管理路由
"""

from aiohttp import web
from aiohttp_cors import CorsConfig

from ..handlers.tasks import TaskHandler


def setup_task_routes(app: web.Application, cors: CorsConfig = None):
    """设置定时任务管理路由
    
    Args:
        app: aiohttp应用实例
        cors: CORS配置对象
    """
    
    # 创建处理器实例
    task_handler = TaskHandler()
    
    # 存储到应用上下文
    app['task_handler'] = task_handler
    
    # 获取任务列表
    route = app.router.add_get('/api/v1/tasks', task_handler.list_tasks)
    if cors:
        cors.add(route)
    
    # 创建定时任务
    route = app.router.add_post('/api/v1/tasks', task_handler.create_task)
    if cors:
        cors.add(route)
    
    # 获取任务详情
    route = app.router.add_get('/api/v1/tasks/{task_id}', task_handler.get_task)
    if cors:
        cors.add(route)
    
    # 更新任务
    route = app.router.add_put('/api/v1/tasks/{task_id}', task_handler.update_task)
    if cors:
        cors.add(route)
    
    # 删除任务
    route = app.router.add_delete('/api/v1/tasks/{task_id}', task_handler.delete_task)
    if cors:
        cors.add(route)
    
    # 手动触发任务
    route = app.router.add_post('/api/v1/tasks/{task_id}/trigger', task_handler.trigger_task)
    if cors:
        cors.add(route)
    
    # 启用/禁用任务
    route = app.router.add_post('/api/v1/tasks/{task_id}/toggle', task_handler.toggle_task)
    if cors:
        cors.add(route)
    
    # 获取任务执行历史
    route = app.router.add_get('/api/v1/tasks/{task_id}/history', task_handler.get_task_history)
    if cors:
        cors.add(route)
