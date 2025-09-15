"""
Integration Service 定时任务调度器

基于asyncron实现的定时任务管理系统。
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
import uuid

# 导入asyncron模块
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../libs/asyncron'))

from asyncron import AsyncScheduler, Task

from ..config import IntegrationConfig
from ..exceptions import AdapterException

logger = logging.getLogger(__name__)


class IntegrationScheduler:
    """Integration Service 定时任务调度器"""
    
    def __init__(self, config: IntegrationConfig, coordinator=None):
        """初始化调度器
        
        Args:
            config: 集成配置对象
            coordinator: 系统协调器实例
        """
        self.config = config
        self.coordinator = coordinator
        self._scheduler = AsyncScheduler()
        self._is_running = False
        self._tasks: Dict[str, Task] = {}
        self._task_history: List[Dict[str, Any]] = []
        
        logger.info("IntegrationScheduler initialized")
    
    async def start(self):
        """启动调度器"""
        if self._is_running:
            return
        
        try:
            # 启动asyncron调度器
            await self._scheduler.start()
            
            # 设置默认任务
            await self._setup_default_tasks()
            
            self._is_running = True
            logger.info("IntegrationScheduler started")
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            raise
    
    async def stop(self):
        """停止调度器"""
        if not self._is_running:
            return
        
        try:
            # 停止所有任务
            for task in self._tasks.values():
                await self._scheduler.remove_task(task)
            
            # 停止调度器
            await self._scheduler.stop()
            
            self._is_running = False
            logger.info("IntegrationScheduler stopped")
            
        except Exception as e:
            logger.error(f"Failed to stop scheduler: {e}")
    
    async def _setup_default_tasks(self):
        """设置默认任务"""
        try:
            # 每日数据抓取任务 (18:00)
            daily_fetch_task = Task(
                name="daily_data_fetch",
                func=self._trigger_daily_data_fetch,
                cron="0 18 * * *",  # 每天18:00
                timezone=self.config.service.scheduler_timezone
            )
            
            # 完整分析周期任务 (19:00)
            analysis_cycle_task = Task(
                name="full_analysis_cycle", 
                func=self._trigger_analysis_cycle,
                cron="0 19 * * *",  # 每天19:00
                timezone=self.config.service.scheduler_timezone
            )
            
            # 系统健康检查任务 (每30分钟)
            health_check_task = Task(
                name="system_health_check",
                func=self._system_health_check,
                cron="*/30 * * * *",  # 每30分钟
                timezone=self.config.service.scheduler_timezone
            )
            
            # 注册任务
            await self._add_task(daily_fetch_task)
            await self._add_task(analysis_cycle_task)
            await self._add_task(health_check_task)
            
            logger.info("Default tasks setup completed")
            
        except Exception as e:
            logger.error(f"Failed to setup default tasks: {e}")
            raise
    
    async def _add_task(self, task: Task):
        """添加任务"""
        try:
            await self._scheduler.add_task(task)
            self._tasks[task.name] = task
            logger.info(f"Task added: {task.name}")
        except Exception as e:
            logger.error(f"Failed to add task {task.name}: {e}")
            raise
    
    async def create_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """创建新任务
        
        Args:
            task_data: 任务数据
            
        Returns:
            Dict[str, Any]: 创建的任务信息
        """
        try:
            task_id = str(uuid.uuid4())
            task_name = task_data.get('name', f"task_{task_id}")
            
            # 创建任务函数
            async def task_function():
                await self._execute_custom_task(task_data)
            
            task = Task(
                name=task_name,
                func=task_function,
                cron=task_data['cron'],
                timezone=task_data.get('timezone', self.config.service.scheduler_timezone)
            )
            
            await self._add_task(task)
            
            return {
                'task_id': task_id,
                'name': task_name,
                'cron': task_data['cron'],
                'status': 'created',
                'created_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to create task: {e}")
            raise AdapterException(f"Task creation failed: {e}")
    
    async def get_all_tasks(self) -> List[Dict[str, Any]]:
        """获取所有任务"""
        tasks = []
        for name, task in self._tasks.items():
            tasks.append({
                'name': name,
                'cron': task.cron if hasattr(task, 'cron') else 'unknown',
                'timezone': task.timezone if hasattr(task, 'timezone') else 'unknown',
                'status': 'active' if self._is_running else 'stopped'
            })
        return tasks
    
    async def get_task(self, task_id: str) -> Dict[str, Any]:
        """获取任务详情"""
        # 简化实现，实际应该根据task_id查找
        if task_id in self._tasks:
            task = self._tasks[task_id]
            return {
                'task_id': task_id,
                'name': task.name,
                'status': 'active' if self._is_running else 'stopped'
            }
        else:
            raise AdapterException(f"Task not found: {task_id}")
    
    async def trigger_task(self, task_id: str) -> Dict[str, Any]:
        """手动触发任务"""
        if task_id in self._tasks:
            task = self._tasks[task_id]
            try:
                await task.func()
                return {
                    'task_id': task_id,
                    'status': 'triggered',
                    'timestamp': datetime.utcnow().isoformat()
                }
            except Exception as e:
                logger.error(f"Failed to trigger task {task_id}: {e}")
                raise AdapterException(f"Task trigger failed: {e}")
        else:
            raise AdapterException(f"Task not found: {task_id}")
    
    # 默认任务实现
    async def _trigger_daily_data_fetch(self):
        """触发每日数据抓取"""
        try:
            logger.info("Triggering daily data fetch...")

            # 获取FlowhubAdapter实例
            flowhub_adapter = await self._get_flowhub_adapter()

            if flowhub_adapter:
                # 创建每日数据抓取任务
                job_result = await flowhub_adapter.create_daily_data_fetch_job(
                    symbols=None,  # None表示抓取所有股票
                    incremental=True  # 使用增量更新
                )

                job_id = job_result.get('job_id')
                logger.info(f"Daily data fetch job created: {job_id}")

                # 记录任务执行成功
                self._record_task_execution(
                    "daily_data_fetch",
                    "completed",
                    f"Daily data fetch job created: {job_id}"
                )

                # 可选：等待任务完成（对于定时任务，通常不等待）
                # result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=1800)

            else:
                logger.warning("FlowhubAdapter not available, skipping data fetch")
                self._record_task_execution("daily_data_fetch", "skipped", "FlowhubAdapter not available")

        except Exception as e:
            logger.error(f"Daily data fetch failed: {e}")
            self._record_task_execution("daily_data_fetch", "failed", str(e))
    
    async def _trigger_analysis_cycle(self):
        """触发完整分析周期"""
        try:
            logger.info("Triggering full analysis cycle...")
            
            if self.coordinator:
                result = await self.coordinator.coordinate_full_analysis_cycle()
                self._record_task_execution("full_analysis_cycle", "completed", f"Analysis cycle: {result.cycle_id}")
            else:
                logger.warning("No coordinator available for analysis cycle")
                self._record_task_execution("full_analysis_cycle", "skipped", "No coordinator available")
                
        except Exception as e:
            logger.error(f"Analysis cycle failed: {e}")
            self._record_task_execution("full_analysis_cycle", "failed", str(e))

    async def _get_flowhub_adapter(self):
        """获取FlowhubAdapter实例

        Returns:
            FlowhubAdapter: FlowhubAdapter实例，如果不可用则返回None
        """
        try:
            from ..adapters import FlowhubAdapter

            # 创建FlowhubAdapter实例
            flowhub_adapter = FlowhubAdapter(self.config)

            # 连接到Flowhub服务
            connected = await flowhub_adapter.connect_to_system()

            if connected:
                logger.debug("FlowhubAdapter connected successfully")
                return flowhub_adapter
            else:
                logger.warning("Failed to connect FlowhubAdapter")
                return None

        except Exception as e:
            logger.error(f"Failed to get FlowhubAdapter: {e}")
            return None
    
    async def _system_health_check(self):
        """系统健康检查"""
        try:
            logger.debug("Performing system health check...")
            
            # 简化的健康检查
            health_status = "healthy" if self._is_running else "unhealthy"
            self._record_task_execution("system_health_check", "completed", f"System status: {health_status}")
            
        except Exception as e:
            logger.error(f"System health check failed: {e}")
            self._record_task_execution("system_health_check", "failed", str(e))
    
    async def _execute_custom_task(self, task_data: Dict[str, Any]):
        """执行自定义任务"""
        try:
            logger.info(f"Executing custom task: {task_data.get('name', 'unknown')}")
            
            # 这里应该根据task_data中的function字段执行相应的功能
            # 暂时记录日志
            self._record_task_execution(task_data.get('name', 'custom'), "completed", "Custom task executed")
            
        except Exception as e:
            logger.error(f"Custom task execution failed: {e}")
            self._record_task_execution(task_data.get('name', 'custom'), "failed", str(e))
    
    def _record_task_execution(self, task_name: str, status: str, message: str):
        """记录任务执行历史"""
        execution_record = {
            'task_name': task_name,
            'status': status,
            'message': message,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        self._task_history.append(execution_record)
        
        # 保持历史记录数量限制
        if len(self._task_history) > 1000:
            self._task_history = self._task_history[-500:]  # 保留最近500条
        
        logger.info(f"Task execution recorded: {task_name} - {status}")
    
    async def get_task_history(self, task_id: str, query_params: Dict[str, Any]) -> Dict[str, Any]:
        """获取任务执行历史"""
        limit = int(query_params.get('limit', 10))
        offset = int(query_params.get('offset', 0))
        
        # 过滤指定任务的历史记录
        task_history = [record for record in self._task_history if record['task_name'] == task_id]
        
        return {
            'task_id': task_id,
            'history': task_history[offset:offset+limit],
            'total': len(task_history),
            'limit': limit,
            'offset': offset
        }
