"""
Integration Service 定时任务调度器

基于asyncron实现的定时任务管理系统。
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
import uuid


from asyncron import (
    Scheduler,
    BluePrint,
    PlansAt,
    PlansEvery,
    TimeUnit,
    TimeUnits,
    start_scheduler
)

from config import IntegrationConfig
from exceptions import AdapterException

logger = logging.getLogger(__name__)

try:
    from adapters import FlowhubAdapter
except Exception:
    FlowhubAdapter = None


class IntegrationScheduler:
    """Integration Service 定时任务调度器"""

    def __init__(self, config: IntegrationConfig, coordinator=None, app=None):
        """初始化调度器

        Args:
            config: 集成配置对象
            coordinator: 系统协调器实例
            app: aiohttp应用实例
        """
        self.config = config
        self.coordinator = coordinator
        self.app = app
        self._is_running = False
        self._task_history: List[Dict[str, Any]] = []
        self._blueprint = None
        # 自定义任务注册表: task_id -> {name, cron, function, enabled, payload}
        self._managed_tasks: Dict[str, Dict[str, Any]] = {}

        logger.info("IntegrationScheduler initialized")

    async def start(self):
        """启动调度器"""
        if self._is_running:
            logger.info("Scheduler is already running")
            return

        try:

            # 创建任务蓝图
            self._blueprint = self._create_task_blueprint()

            # 启动asyncron调度器
            start_scheduler([self._blueprint])

            self._is_running = True
            logger.info("IntegrationScheduler started successfully")

        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            raise

    def get_planer(self) -> BluePrint:
        """获取asyncron planer（蓝图）"""
        if not hasattr(self, '_blueprint') or self._blueprint is None:
            self._blueprint = self._create_task_blueprint()
        return self._blueprint

    def _create_task_blueprint(self) -> BluePrint:
        """创建任务蓝图: 包含默认任务 + 启用的自定义任务"""
        blueprint = BluePrint()

        # 默认任务：每日数据抓取（可配置cron，缺省每天一次）
        daily_cron = getattr(self.config.service, 'daily_data_fetch_cron', None)
        daily_plans = self._parse_cron_to_plans(daily_cron) if daily_cron else PlansEvery([TimeUnits.DAYS], [1])
        blueprint.task('/daily_data_fetch', plans=daily_plans)(
            self._trigger_daily_data_fetch
        )

        # 新增任务：每日指数数据抓取
        blueprint.task('/daily_index_fetch', plans=daily_plans)(
            self._trigger_daily_index_fetch
        )

        # 新增任务：每日板块数据抓取
        blueprint.task('/daily_board_fetch', plans=daily_plans)(
            self._trigger_daily_board_fetch
        )

        # 默认任务：系统健康检查（每30分钟执行一次）
        blueprint.task('/system_health_check', plans=PlansEvery([TimeUnits.MINUTES], [30]))(
            self._system_health_check
        )

        # 合并自定义任务（启用状态）
        for task_id, t in self._managed_tasks.items():
            if not t.get('enabled', True):
                continue
            plans = self._parse_cron_to_plans(t.get('cron'))
            url = f"/custom/{task_id}"

            async def _runner(context, _tid=task_id):
                await self._execute_custom_task(self._managed_tasks.get(_tid, {}))

            blueprint.task(url, plans=plans)(_runner)

        logger.info("Task blueprint created with default and custom tasks")
        return blueprint

    async def stop(self):
        """停止调度器"""
        if not self._is_running:
            logger.info("Scheduler is not running")
            return

        try:
            # 停止asyncron调度器（兼容本地实现）
            success = False
            try:
                sched = Scheduler.get_instance()
                loop_thread = getattr(sched, "_Scheduler__loop_thread", None)
                if loop_thread and hasattr(loop_thread, "cancel"):
                    success = loop_thread.cancel()
            except Exception:
                success = False
            if success:
                self._is_running = False
                logger.info("IntegrationScheduler stopped successfully")
            else:
                logger.warning("Failed to stop scheduler gracefully")

        except Exception as e:
            logger.error(f"Failed to stop scheduler: {e}")
            raise

    def _parse_cron_to_plans(self, cron: Optional[str]) -> Any:
        """将简化的cron表达式转换为asyncron的Plans对象
        支持格式：
        - every:<n>s|m|h|d  例如 every:30m, every:1h
        - at:HH:MM[:SS]    例如 at:02:00 或 at:02:00:00 （按天）
        """
        if not cron or not isinstance(cron, str):
            # 默认每天一次
            return PlansEvery([TimeUnits.DAYS], [1])
        try:
            cron = cron.strip().lower()
            if cron.startswith('every:'):
                val = cron.split(':', 1)[1]
                num_str = ''.join(ch for ch in val if ch.isdigit()) or '1'
                unit = ''.join(ch for ch in val if ch.isalpha()) or 'm'
                n = int(num_str)
                if unit in ('s', 'sec', 'secs', 'second', 'seconds'):
                    return PlansEvery([TimeUnits.SECONDS], [n])
                if unit in ('m', 'min', 'mins', 'minute', 'minutes'):
                    return PlansEvery([TimeUnits.MINUTES], [n])
                if unit in ('h', 'hour', 'hours'):
                    return PlansEvery([TimeUnits.HOURS], [n])
                if unit in ('d', 'day', 'days'):
                    return PlansEvery([TimeUnits.DAYS], [n])
                # 默认按分钟
                return PlansEvery([TimeUnits.MINUTES], [n])
            if cron.startswith('at:'):
                # asyncron的"at"按天触发，使用PlansEvery + Timer.at 需要通过PlansAt来表达具体时间
                time_str = cron.split(':', 1)[1]
                time_str = time_str if time_str.count(':') == 2 else (time_str + ":00")
                return PlansAt([TimeUnit.DAY], [time_str])
        except Exception:
            pass
        # 兜底：每小时一次
        return PlansEvery([TimeUnits.HOURS], [1])

    async def _restart_scheduler(self):
        """应用任务变更：运行中则热更新新增蓝图，未运行则启动"""
        try:
            self._blueprint = self._create_task_blueprint()
            if self._is_running:
                Scheduler.get_instance().add_plan(self._blueprint)
                logger.info("Scheduler updated with new blueprint (hot-add)")
            else:
                start_scheduler([self._blueprint])
                self._is_running = True
                logger.info("Scheduler started with blueprint")
        except Exception as e:
            logger.error(f"Failed to apply scheduler update: {e}")
            raise AdapterException("IntegrationScheduler", f"Scheduler update failed: {e}")

    async def create_task(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """创建自定义定时任务
        需要字段: name, cron, function(字符串，标识要执行的功能)，可选payload
        """
        name = (data or {}).get('name')
        cron = (data or {}).get('cron')
        func = (data or {}).get('function')
        if not name or not cron or not func:
            raise AdapterException("IntegrationScheduler", "Missing required fields: name/cron/function")
        task_id = str(uuid.uuid4())
        self._managed_tasks[task_id] = {
            'task_id': task_id,
            'name': name,
            'cron': cron,
            'function': func,
            'payload': data.get('payload'),
            'enabled': True,
            'created_at': datetime.utcnow().isoformat()
        }
        await self._restart_scheduler()
        return self._managed_tasks[task_id]

    async def update_task(self, task_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """更新自定义任务配置并应用"""
        if task_id not in self._managed_tasks:
            raise AdapterException("IntegrationScheduler", f"Task not found: {task_id}")
        t = self._managed_tasks[task_id]
        for k in ('name', 'cron', 'function', 'payload', 'enabled'):
            if k in data:
                t[k] = data[k]
        t['updated_at'] = datetime.utcnow().isoformat()
        await self._restart_scheduler()
        return t

    async def delete_task(self, task_id: str) -> bool:
        """删除自定义任务：从注册表移除并重启调度器"""
        if task_id not in self._managed_tasks:
            raise AdapterException("IntegrationScheduler", f"Task not found: {task_id}")
        self._managed_tasks.pop(task_id, None)
        await self._restart_scheduler()
        return True

    async def toggle_task(self, task_id: str) -> Dict[str, Any]:
        """切换任务启用状态并应用"""
        if task_id not in self._managed_tasks:
            raise AdapterException("IntegrationScheduler", f"Task not found: {task_id}")
        t = self._managed_tasks[task_id]
        t['enabled'] = not t.get('enabled', True)
        t['updated_at'] = datetime.utcnow().isoformat()
        await self._restart_scheduler()
        return t



    async def get_all_tasks(self) -> List[Dict[str, Any]]:
        """获取所有任务"""
        if not self._is_running:
            return []

        try:
            scheduler = Scheduler.get_instance()
            tasks = scheduler.list_tasks()
            return [
                {
                    'name': task.get_task_name(),
                    'task_id': task.get_task_id(),
                    'status': 'active' if self._is_running else 'stopped'
                }
                for task in tasks
            ]
        except Exception as e:
            logger.error(f"Failed to get tasks: {e}")
            return []

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        """获取任务详情"""
        try:
            scheduler = Scheduler.get_instance()
            task = scheduler.get_task(task_id)
            return {
                'task_id': task_id,
                'name': task.get_task_name(),
                'status': 'active' if self._is_running else 'stopped'
            }
        except Exception as e:
            raise AdapterException("IntegrationScheduler", f"Task not found: {task_id}")

    async def trigger_task(self, task_id: str) -> Dict[str, Any]:
        """手动触发任务"""
        try:
            scheduler = Scheduler.get_instance()
            scheduler.start_task(task_id)
            return {
                'task_id': task_id,
                'status': 'triggered',
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to trigger task {task_id}: {e}")
            raise AdapterException("IntegrationScheduler", f"Task trigger failed: {e}")

    # 默认任务实现
    async def _trigger_daily_data_fetch(self, context):
        """触发每日数据抓取

        注意：
        1. 创建 Flowhub 数据抓取任务
        2. 等待任务完成（最多30分钟）
        3. 通知 AnalysisTriggerScheduler 各个数据抓取任务已完成
        """
        flowhub_adapter = None
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

                # 等待任务完成（最多30分钟）
                logger.info(f"Waiting for job {job_id} to complete (timeout: 30 minutes)...")
                result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=1800)

                job_status = result.get('status')
                if job_status == 'completed':
                    logger.info(f"Daily data fetch job {job_id} completed successfully")

                    # 记录任务执行成功
                    self._record_task_execution(
                        "daily_data_fetch",
                        "completed",
                        f"Daily data fetch job completed: {job_id}"
                    )

                    # 通知 AnalysisTriggerScheduler 数据抓取任务已完成
                    await self._notify_data_fetch_completed()

                else:
                    logger.warning(f"Daily data fetch job {job_id} finished with status: {job_status}")
                    self._record_task_execution(
                        "daily_data_fetch",
                        "failed",
                        f"Job finished with status: {job_status}"
                    )

            else:
                logger.warning("FlowhubAdapter not available, skipping data fetch")
                self._record_task_execution("daily_data_fetch", "skipped", "FlowhubAdapter not available")

        except Exception as e:
            logger.error(f"Daily data fetch failed: {e}", exc_info=True)
            self._record_task_execution("daily_data_fetch", "failed", str(e))
        finally:
            if flowhub_adapter:
                try:
                    await flowhub_adapter.disconnect_from_system()
                except Exception as ce:
                    logger.warning(f"Failed to close FlowhubAdapter session: {ce}")

    async def _trigger_daily_index_fetch(self, context):
        """触发每日指数数据抓取"""
        flowhub_adapter = None
        try:
            logger.info("Triggering daily index data fetch...")

            # 获取FlowhubAdapter实例
            flowhub_adapter = await self._get_flowhub_adapter()

            if flowhub_adapter:
                # 创建指数日线数据抓取任务
                job_result = await flowhub_adapter.create_index_daily_data_job(
                    index_codes=None,  # None表示主要指数
                    update_mode='incremental'
                )

                job_id = job_result.get('job_id')
                logger.info(f"Index daily data fetch job created: {job_id}")

                # 等待任务完成（最多30分钟）
                logger.info(f"Waiting for job {job_id} to complete (timeout: 30 minutes)...")
                result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=1800)

                job_status = result.get('status')
                if job_status == 'completed':
                    logger.info(f"Index daily data fetch job {job_id} completed successfully")
                    self._record_task_execution(
                        "daily_index_fetch",
                        "completed",
                        f"Index data fetch job completed: {job_id}"
                    )
                else:
                    logger.warning(f"Index daily data fetch job {job_id} finished with status: {job_status}")
                    self._record_task_execution(
                        "daily_index_fetch",
                        "failed",
                        f"Job finished with status: {job_status}"
                    )
            else:
                logger.warning("FlowhubAdapter not available, skipping index data fetch")
                self._record_task_execution("daily_index_fetch", "skipped", "FlowhubAdapter not available")

        except Exception as e:
            logger.error(f"Daily index data fetch failed: {e}", exc_info=True)
            self._record_task_execution("daily_index_fetch", "failed", str(e))
        finally:
            if flowhub_adapter:
                try:
                    await flowhub_adapter.disconnect_from_system()
                except Exception as ce:
                    logger.warning(f"Failed to close FlowhubAdapter session: {ce}")

    async def _trigger_daily_board_fetch(self, context):
        """触发每日板块数据抓取（行业板块 + 概念板块）"""
        flowhub_adapter = None
        try:
            logger.info("Triggering daily board data fetch...")

            # 获取FlowhubAdapter实例
            flowhub_adapter = await self._get_flowhub_adapter()

            if flowhub_adapter:
                # 创建行业板块数据抓取任务
                industry_job_result = await flowhub_adapter.create_industry_board_job(
                    source='ths',
                    update_mode='incremental'
                )
                industry_job_id = industry_job_result.get('job_id')
                logger.info(f"Industry board data fetch job created: {industry_job_id}")

                # 创建概念板块数据抓取任务
                concept_job_result = await flowhub_adapter.create_concept_board_job(
                    source='ths',
                    update_mode='incremental'
                )
                concept_job_id = concept_job_result.get('job_id')
                logger.info(f"Concept board data fetch job created: {concept_job_id}")

                # 等待行业板块任务完成
                logger.info(f"Waiting for industry board job {industry_job_id} to complete...")
                industry_result = await flowhub_adapter.wait_for_job_completion(industry_job_id, timeout=1800)

                # 等待概念板块任务完成
                logger.info(f"Waiting for concept board job {concept_job_id} to complete...")
                concept_result = await flowhub_adapter.wait_for_job_completion(concept_job_id, timeout=1800)

                # 检查任务状态
                industry_status = industry_result.get('status')
                concept_status = concept_result.get('status')

                if industry_status == 'completed' and concept_status == 'completed':
                    logger.info(f"Board data fetch jobs completed successfully")
                    self._record_task_execution(
                        "daily_board_fetch",
                        "completed",
                        f"Industry: {industry_job_id}, Concept: {concept_job_id}"
                    )
                else:
                    logger.warning(f"Board data fetch jobs finished with status: Industry={industry_status}, Concept={concept_status}")
                    self._record_task_execution(
                        "daily_board_fetch",
                        "partial",
                        f"Industry: {industry_status}, Concept: {concept_status}"
                    )
            else:
                logger.warning("FlowhubAdapter not available, skipping board data fetch")
                self._record_task_execution("daily_board_fetch", "skipped", "FlowhubAdapter not available")

        except Exception as e:
            logger.error(f"Daily board data fetch failed: {e}", exc_info=True)
            self._record_task_execution("daily_board_fetch", "failed", str(e))
        finally:
            if flowhub_adapter:
                try:
                    await flowhub_adapter.disconnect_from_system()
                except Exception as ce:
                    logger.warning(f"Failed to close FlowhubAdapter session: {ce}")

    async def _notify_data_fetch_completed(self):
        """通知 AnalysisTriggerScheduler 数据抓取任务已完成

        根据 AnalysisTriggerScheduler 的依赖配置，通知以下任务已完成：
        - stock_basic_data_fetch（股票基本信息）
        - stock_daily_data_fetch（股票日K线数据）
        - stock_index_data_fetch（股票指数）
        - industry_board_data_fetch（行业板块）
        - concept_board_data_fetch（概念板块）
        """
        try:
            if self.app and 'analysis_trigger' in self.app:
                analysis_trigger = self.app['analysis_trigger']

                # 通知所有股票相关数据抓取任务已完成
                tasks_to_notify = [
                    'stock_basic_data_fetch',
                    'stock_daily_data_fetch',
                    'stock_index_data_fetch',
                    'industry_board_data_fetch',
                    'concept_board_data_fetch'
                ]

                for task_name in tasks_to_notify:
                    await analysis_trigger.mark_task_completed(task_name)
                    logger.info(f"Notified analysis trigger: {task_name} completed")

            else:
                logger.warning("AnalysisTriggerScheduler not available, skipping notification")

        except Exception as e:
            logger.error(f"Failed to notify data fetch completion: {e}", exc_info=True)

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
            # 创建FlowhubAdapter实例
            if FlowhubAdapter is None:
                logger.warning("FlowhubAdapter import failed")
                return None
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

    async def _system_health_check(self, context):
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
            self._record_task_execution(task_data.get('name', 'custom'), "completed", "Custom task executed", task_data.get('task_id'))

        except Exception as e:
            logger.error(f"Custom task execution failed: {e}")
            self._record_task_execution(task_data.get('name', 'custom'), "failed", str(e), task_data.get('task_id'))

    def _record_task_execution(self, task_name: str, status: str, message: str, task_id: Optional[str] = None):
        """记录任务执行历史"""
        execution_record = {
            'task_id': task_id,
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

        # 如果任务成功完成，通知分析触发调度器
        if status == 'completed':
            asyncio.create_task(self._notify_task_completion(task_name))

    async def _notify_task_completion(self, task_name: str):
        """通知分析触发调度器任务已完成

        Args:
            task_name: 任务名称
        """
        try:
            if self.app and 'analysis_trigger' in self.app:
                analysis_trigger = self.app['analysis_trigger']
                await analysis_trigger.mark_task_completed(task_name)
                logger.info(f"Notified analysis trigger: {task_name} completed")
        except Exception as e:
            logger.warning(f"Failed to notify task completion for {task_name}: {e}")

    async def get_task_history(self, task_id: str, query_params: Dict[str, Any]) -> Dict[str, Any]:
        """获取任务执行历史"""
        limit = int(query_params.get('limit', 10))
        offset = int(query_params.get('offset', 0))

        # 过滤指定任务的历史记录
        task_history = [record for record in self._task_history if record.get('task_id') == task_id or record.get('task_name') == task_id]

        return {
            'task_id': task_id,
            'history': task_history[offset:offset+limit],
            'total': len(task_history),
            'limit': limit,
            'offset': offset
        }
