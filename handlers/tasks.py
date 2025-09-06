"""
定时任务处理器
"""

from aiohttp import web

from .base import BaseHandler


class TaskHandler(BaseHandler):
    """定时任务处理器"""
    
    async def list_tasks(self, request: web.Request) -> web.Response:
        """获取任务列表"""
        try:
            scheduler = self.get_app_component(request, 'scheduler')
            tasks = await scheduler.get_all_tasks()
            return self.success_response(tasks)
        except Exception as e:
            self.logger.error(f"List tasks failed: {e}")
            return self.error_response("获取任务列表失败", 500)
    
    async def create_task(self, request: web.Request) -> web.Response:
        """创建定时任务"""
        try:
            data = await self.get_request_json(request)
            scheduler = self.get_app_component(request, 'scheduler')
            
            # 验证必需字段
            error = self.validate_required_fields(data, ['name', 'cron', 'function'])
            if error:
                return self.error_response(error, 400)
            
            task = await scheduler.create_task(data)
            return self.success_response(task, "任务创建成功")
        except Exception as e:
            self.logger.error(f"Create task failed: {e}")
            return self.error_response("创建任务失败", 500)
    
    async def get_task(self, request: web.Request) -> web.Response:
        """获取任务详情"""
        try:
            task_id = self.get_path_params(request)['task_id']
            scheduler = self.get_app_component(request, 'scheduler')
            task = await scheduler.get_task(task_id)
            return self.success_response(task)
        except Exception as e:
            self.logger.error(f"Get task failed: {e}")
            return self.error_response("获取任务详情失败", 500)
    
    async def update_task(self, request: web.Request) -> web.Response:
        """更新任务"""
        try:
            task_id = self.get_path_params(request)['task_id']
            data = await self.get_request_json(request)
            scheduler = self.get_app_component(request, 'scheduler')
            task = await scheduler.update_task(task_id, data)
            return self.success_response(task, "任务更新成功")
        except Exception as e:
            self.logger.error(f"Update task failed: {e}")
            return self.error_response("更新任务失败", 500)
    
    async def delete_task(self, request: web.Request) -> web.Response:
        """删除任务"""
        try:
            task_id = self.get_path_params(request)['task_id']
            scheduler = self.get_app_component(request, 'scheduler')
            await scheduler.delete_task(task_id)
            return self.success_response(None, "任务删除成功")
        except Exception as e:
            self.logger.error(f"Delete task failed: {e}")
            return self.error_response("删除任务失败", 500)
    
    async def trigger_task(self, request: web.Request) -> web.Response:
        """手动触发任务"""
        try:
            task_id = self.get_path_params(request)['task_id']
            scheduler = self.get_app_component(request, 'scheduler')
            result = await scheduler.trigger_task(task_id)
            return self.success_response(result, "任务触发成功")
        except Exception as e:
            self.logger.error(f"Trigger task failed: {e}")
            return self.error_response("触发任务失败", 500)
    
    async def toggle_task(self, request: web.Request) -> web.Response:
        """启用/禁用任务"""
        try:
            task_id = self.get_path_params(request)['task_id']
            scheduler = self.get_app_component(request, 'scheduler')
            result = await scheduler.toggle_task(task_id)
            return self.success_response(result, "任务状态切换成功")
        except Exception as e:
            self.logger.error(f"Toggle task failed: {e}")
            return self.error_response("切换任务状态失败", 500)
    
    async def get_task_history(self, request: web.Request) -> web.Response:
        """获取任务执行历史"""
        try:
            task_id = self.get_path_params(request)['task_id']
            query_params = self.get_query_params(request)
            scheduler = self.get_app_component(request, 'scheduler')
            history = await scheduler.get_task_history(task_id, query_params)
            return self.success_response(history)
        except Exception as e:
            self.logger.error(f"Get task history failed: {e}")
            return self.error_response("获取任务历史失败", 500)
