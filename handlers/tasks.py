"""
定时任务处理器
"""

from aiohttp import web
from typing import Any, Dict, List

from handlers.base import BaseHandler


class TaskHandler(BaseHandler):
    """定时任务处理器"""

    async def list_tasks_overview(self, request: web.Request) -> web.Response:
        """获取跨服务任务概览（含 Flowhub/Execution/Macro/Portfolio/Brain）"""
        query_params = self.get_query_params(request)
        limit = int(query_params.get('limit', 200))
        offset = int(query_params.get('offset', 0))

        tasks: List[Dict[str, Any]] = []
        errors: List[Dict[str, str]] = []

        # Brain 定时任务
        try:
            scheduler = self.get_app_component(request, 'scheduler')
            brain_tasks = await scheduler.get_all_tasks()
            tasks.extend([self._normalize_brain_task(t) for t in brain_tasks])
        except Exception as e:
            self.logger.warning(f"Load brain tasks failed: {e}")
            errors.append({'service': 'brain', 'error': str(e)})

        # Flowhub 任务调度列表
        try:
            flowhub_limit = min(limit, 100)
            flowhub_payload = await self._fetch_service_json(request, 'flowhub', '/api/v1/tasks', {
                'limit': flowhub_limit,
                'offset': offset
            })
            flowhub_data = flowhub_payload.get('data') if isinstance(flowhub_payload, dict) else None
            flowhub_tasks = []
            if isinstance(flowhub_data, dict):
                flowhub_tasks = flowhub_data.get('tasks', [])
            elif isinstance(flowhub_payload, dict):
                flowhub_tasks = flowhub_payload.get('tasks', [])
            elif isinstance(flowhub_payload, list):
                flowhub_tasks = flowhub_payload
            tasks.extend([self._normalize_flowhub_task(t) for t in flowhub_tasks])
        except Exception as e:
            self.logger.warning(f"Load flowhub tasks failed: {e}")
            errors.append({'service': 'flowhub', 'error': str(e)})

        # Execution/Macro/Portfolio Job 列表
        for service in ('execution', 'macro', 'portfolio'):
            try:
                job_payload = await self._fetch_service_json(request, service, '/api/v1/jobs', {
                    'limit': limit,
                    'offset': offset
                })
                job_data = job_payload.get('data') if isinstance(job_payload, dict) else None
                jobs = []
                if isinstance(job_data, dict):
                    jobs = job_data.get('jobs', [])
                elif isinstance(job_payload, dict):
                    jobs = job_payload.get('jobs', [])
                elif isinstance(job_payload, list):
                    jobs = job_payload
                tasks.extend([self._normalize_job_task(service, j) for j in jobs])
            except Exception as e:
                self.logger.warning(f"Load {service} jobs failed: {e}")
                errors.append({'service': service, 'error': str(e)})

        return self.success_response({'tasks': tasks, 'errors': errors})
    
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
            # 强制要求 JSON 请求
            ct = request.headers.get('Content-Type', '')
            if 'application/json' not in ct:
                return self.error_response("Content-Type must be application/json", 415, error_code="UNSUPPORTED_MEDIA_TYPE")

            try:
                data = await self.get_request_json(request)
            except web.HTTPBadRequest:
                return self.error_response("Invalid JSON format", 400, error_code="INVALID_JSON")

            if not isinstance(data, dict) or not data:
                return self.error_response("Invalid or empty JSON body", 400, error_code="INVALID_JSON")

            # 基本字段校验
            error = self.validate_required_fields(data, ['name', 'cron', 'function'])
            if error:
                return self.error_response(error, 400, error_code="MISSING_FIELDS")

            # 类型与取值校验
            if not isinstance(data.get('name'), str) or not data['name'].strip():
                return self.error_response("'name' must be a non-empty string", 400, error_code="INVALID_NAME")
            if not isinstance(data.get('cron'), str) or not data['cron'].strip():
                return self.error_response("'cron' must be a non-empty string (e.g. 'every:1m', 'at:02:00')", 400, error_code="INVALID_CRON")
            if not isinstance(data.get('function'), str) or not data['function'].strip():
                return self.error_response("'function' must be a non-empty string", 400, error_code="INVALID_FUNCTION")

            scheduler = self.get_app_component(request, 'scheduler')
            task = await scheduler.create_task(data)
            return self.success_response(task, "任务创建成功")
        except web.HTTPException:
            raise
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

    async def _fetch_service_json(
        self,
        request: web.Request,
        service_name: str,
        path: str,
        params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        registry = self.get_app_component(request, 'service_registry')
        service = getattr(registry, '_services', {}).get(service_name)
        session = getattr(registry, '_session', None)
        if not service or session is None:
            raise RuntimeError(f"Service {service_name} not available")
        url = f"{service['url']}{path}"
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"{service_name} HTTP {resp.status}: {text}")
            return await resp.json()

    def _normalize_flowhub_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(task or {})
        normalized.setdefault('data_source', 'flowhub')
        normalized.setdefault('source', 'flowhub')
        return normalized

    def _normalize_job_task(self, service: str, job: Dict[str, Any]) -> Dict[str, Any]:
        job_id = job.get('job_id') or job.get('id') or job.get('task_id') or ''
        job_type = job.get('job_type') or job.get('type') or job.get('task_type') or 'job'
        status = job.get('status') or job.get('state') or job.get('job_status') or 'unknown'
        name = job.get('name') or job.get('job_name') or job.get('task_name') or f"{service}:{job_type}"
        created_at = job.get('created_at') or job.get('started_at') or job.get('start_time')
        updated_at = job.get('updated_at') or job.get('completed_at') or job.get('end_time')
        success_count = job.get('success_count')
        failed_count = job.get('failed_count')
        if success_count is None:
            success_count = 1 if status in ('succeeded', 'completed', 'success') else 0
        if failed_count is None:
            failed_count = 1 if status in ('failed', 'error') else 0
        return {
            'task_id': job_id,
            'name': name,
            'data_type': job_type,
            'schedule_type': 'manual',
            'schedule_value': None,
            'status': status,
            'enabled': True,
            'run_count': job.get('run_count', 0),
            'success_count': success_count,
            'failed_count': failed_count,
            'created_at': created_at,
            'updated_at': updated_at,
            'last_run_at': updated_at,
            'next_run_at': None,
            'data_source': service,
            'source': service,
            'raw': job
        }

    def _normalize_brain_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        task_id = task.get('task_id') or task.get('id') or ''
        name = task.get('name') or task_id
        enabled = task.get('enabled', True)
        status = task.get('status') or ('disabled' if not enabled else 'idle')
        return {
            'task_id': task_id,
            'name': name,
            'data_type': task.get('data_type') or 'brain_task',
            'schedule_type': 'cron',
            'schedule_value': task.get('cron'),
            'status': status,
            'enabled': enabled,
            'run_count': task.get('run_count', 0),
            'success_count': task.get('success_count', 0),
            'failed_count': task.get('failed_count', 0),
            'created_at': task.get('created_at'),
            'updated_at': task.get('updated_at'),
            'last_run_at': task.get('last_run_at'),
            'next_run_at': task.get('next_run_at'),
            'data_source': 'brain',
            'source': 'brain',
            'raw': task
        }
