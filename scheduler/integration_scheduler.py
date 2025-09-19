"""
Integration Service 定时任务调度器

基于asyncron实现的定时任务管理系统。
"""

import logging
from datetime import datetime, date, timezone
from typing import Dict, List, Any

# 导入asyncron模块
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../external/asyncron'))

import asyncron
from asyncron import (
    create_planer,
    add_planer,
    start_scheduler,
    stop_scheduler,
    is_scheduler_running,
    PlansAt,
    PlansEvery,
    TimeUnit,
    TimeUnits,
    TaskContext
)

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
        self._is_running = False
        self._task_history: List[Dict[str, Any]] = []

        # 创建asyncron planer
        self._planer = create_planer()

        # 创建宏观数据任务调度器
        from .macro_data_tasks import MacroDataTaskScheduler
        self._macro_scheduler = MacroDataTaskScheduler(config, coordinator)

        # 创建Portfolio数据任务调度器
        from .portfolio_data_tasks import PortfolioDataTaskScheduler
        self._portfolio_scheduler = PortfolioDataTaskScheduler(config, coordinator)

        self._setup_default_tasks()

        logger.info("IntegrationScheduler initialized")
    


    async def start(self):
        """启动调度器"""
        if self._is_running:
            logger.warning("IntegrationScheduler is already running")
            return

        try:
            logger.info("Starting IntegrationScheduler...")

            # 检查asyncron调度器状态
            if is_scheduler_running():
                logger.warning("Asyncron scheduler is already running, adding planers only")
                # 只添加planer，不重新启动
                add_planer(self._planer)
                add_planer(self._macro_scheduler.get_planer())
                add_planer(self._portfolio_scheduler.get_planer())
                self._is_running = True
                logger.info("IntegrationScheduler started (asyncron already running)")
                return

            # 添加主planer到asyncron
            add_planer(self._planer)

            # 添加宏观数据任务planer到asyncron
            add_planer(self._macro_scheduler.get_planer())

            # 添加Portfolio数据任务planer到asyncron
            add_planer(self._portfolio_scheduler.get_planer())

            # 启动asyncron调度器
            start_scheduler()

            self._is_running = True
            logger.info("IntegrationScheduler started with macro and portfolio data tasks")

        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            self._is_running = False
            raise

    async def stop(self):
        """停止调度器"""
        if not self._is_running:
            return

        try:
            logger.info("Stopping IntegrationScheduler...")

            # 使用新的停止函数
            success = stop_scheduler()
            if not success:
                logger.warning("Failed to stop asyncron scheduler gracefully")

            self._is_running = False
            logger.info("IntegrationScheduler stopped successfully")

        except Exception as e:
            logger.error(f"Failed to stop scheduler: {e}")
            # 即使出错也要设置停止标志
            self._is_running = False

    def _setup_default_tasks(self):
        """设置默认任务（使用Asyncron装饰器语法）"""

        # ==================== 股票数据抓取任务 ====================

        # 每日股票数据抓取任务 (18:00)
        daily_stock_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["18:00:00"]
        )

        @self._planer.task(url='/daily_stock_data_fetch', plans=daily_stock_plan)
        async def daily_stock_data_fetch(context: TaskContext):
            """每日股票数据抓取任务"""
            await self._trigger_daily_data_fetch()

        # ==================== 宏观数据抓取任务 ====================

        # 每日宏观数据抓取任务 (18:30)
        daily_macro_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["18:30:00"]
        )

        @self._planer.task(url='/daily_macro_data_fetch', plans=daily_macro_plan)
        async def daily_macro_data_fetch(context: TaskContext):
            """每日宏观数据抓取任务"""
            await self._trigger_daily_macro_data_fetch()

        # 每月宏观数据抓取任务 (每月1日 19:00)
        monthly_macro_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["19:00:00"]
        )

        @self._planer.task(url='/monthly_macro_data_fetch', plans=monthly_macro_plan)
        async def monthly_macro_data_fetch(context: TaskContext):
            """月度宏观数据抓取任务"""
            # 只在每月1日执行
            today = date.today()
            if today.day == 1:
                await self._trigger_monthly_macro_data_fetch()

        # 季度宏观数据抓取任务 (每季度第一个月15日 19:30)
        quarterly_macro_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["19:30:00"]
        )

        @self._planer.task(url='/quarterly_macro_data_fetch', plans=quarterly_macro_plan)
        async def quarterly_macro_data_fetch(context: TaskContext):
            """季度宏观数据抓取任务"""
            # 只在1月、4月、7月、10月的15日执行
            today = date.today()
            if today.day == 15 and today.month in [1, 4, 7, 10]:
                await self._trigger_quarterly_macro_data_fetch()

        # 年度宏观数据抓取任务 (每年1月15日 20:00)
        yearly_macro_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["20:00:00"]
        )

        @self._planer.task(url='/yearly_macro_data_fetch', plans=yearly_macro_plan)
        async def yearly_macro_data_fetch(context: TaskContext):
            """年度宏观数据抓取任务"""
            # 只在每年1月15日执行
            today = date.today()
            if today.month == 1 and today.day == 15:
                await self._trigger_yearly_macro_data_fetch()

        # 完整分析周期任务 (19:00)
        analysis_cycle_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["19:00:00"]
        )

        @self._planer.task(url='/full_analysis_cycle', plans=analysis_cycle_plan)
        async def full_analysis_cycle(context: TaskContext):
            """完整分析周期任务"""
            await self._trigger_analysis_cycle()

        # 系统健康检查任务 (每30分钟)
        health_check_plan = PlansEvery(
            time_units=[TimeUnits.MINUTES],
            every=[30]
        )

        @self._planer.task(url='/system_health_check', plans=health_check_plan)
        async def system_health_check(context: TaskContext):
            """系统健康检查任务"""
            await self._system_health_check()

        logger.info("Default tasks setup completed")

        # 注册模块到asyncron
        asyncron.register_mod('config', self.config)
        asyncron.register_mod('coordinator', self.coordinator)
        asyncron.register_mod('scheduler', self)

    async def get_all_tasks(self) -> List[Dict[str, Any]]:
        """获取所有任务"""
        from .macro_data_config import MacroDataConfig

        # 基础任务
        tasks = [
            {'name': 'daily_stock_data_fetch', 'schedule': 'Daily at 18:00', 'status': 'active' if self._is_running else 'stopped'},
            {'name': 'full_analysis_cycle', 'schedule': 'Daily at 19:00', 'status': 'active' if self._is_running else 'stopped'},
            {'name': 'system_health_check', 'schedule': 'Every 30 minutes', 'status': 'active' if self._is_running else 'stopped'},
        ]

        # 添加14个独立的宏观数据任务
        macro_tasks = [
            # 日度任务
            {'name': 'interest_rate_data_fetch', 'schedule': 'Daily at 18:30', 'type': 'daily'},
            {'name': 'stock_index_data_fetch', 'schedule': 'Daily at 18:35', 'type': 'daily'},
            {'name': 'market_flow_data_fetch', 'schedule': 'Daily at 18:40', 'type': 'daily'},
            {'name': 'commodity_price_data_fetch', 'schedule': 'Daily at 18:45', 'type': 'daily'},

            # 月度任务
            {'name': 'price_index_data_fetch', 'schedule': 'Monthly on 1st at 19:00', 'type': 'monthly'},
            {'name': 'money_supply_data_fetch', 'schedule': 'Monthly on 1st at 19:05', 'type': 'monthly'},
            {'name': 'social_financing_data_fetch', 'schedule': 'Monthly on 1st at 19:10', 'type': 'monthly'},
            {'name': 'investment_data_fetch', 'schedule': 'Monthly on 1st at 19:15', 'type': 'monthly'},
            {'name': 'industrial_data_fetch', 'schedule': 'Monthly on 1st at 19:20', 'type': 'monthly'},
            {'name': 'sentiment_index_data_fetch', 'schedule': 'Monthly on 1st at 19:25', 'type': 'monthly'},
            {'name': 'inventory_cycle_data_fetch', 'schedule': 'Monthly on 1st at 19:30', 'type': 'monthly'},

            # 季度任务
            {'name': 'gdp_data_fetch', 'schedule': 'Quarterly on 15th at 19:35', 'type': 'quarterly'},

            # 年度任务
            {'name': 'innovation_data_fetch', 'schedule': 'Yearly on Jan 15th at 20:00', 'type': 'yearly'},
            {'name': 'demographic_data_fetch', 'schedule': 'Yearly on Jan 15th at 20:05', 'type': 'yearly'},
        ]

        # 为宏观数据任务添加状态
        for task in macro_tasks:
            task['status'] = 'active' if self._is_running else 'stopped'

        tasks.extend(macro_tasks)

        # 添加Portfolio数据任务
        portfolio_tasks = self._portfolio_scheduler.get_portfolio_tasks_info()
        for task in portfolio_tasks:
            task['status'] = 'active' if self._is_running else 'stopped'
            task['category'] = 'portfolio'

        tasks.extend(portfolio_tasks)
        return tasks

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        """获取任务详情"""
        tasks = await self.get_all_tasks()
        for task in tasks:
            if task['name'] == task_id:
                return {
                    'task_id': task_id,
                    'name': task['name'],
                    'schedule': task['schedule'],
                    'status': task['status']
                }
        raise AdapterException(f"Task not found: {task_id}")

    async def trigger_task(self, task_id: str) -> Dict[str, Any]:
        """手动触发任务"""
        task_methods = {
            'daily_stock_data_fetch': self._trigger_daily_data_fetch,
            'daily_macro_data_fetch': self._trigger_daily_macro_data_fetch,
            'monthly_macro_data_fetch': self._trigger_monthly_macro_data_fetch,
            'quarterly_macro_data_fetch': self._trigger_quarterly_macro_data_fetch,
            'yearly_macro_data_fetch': self._trigger_yearly_macro_data_fetch,
            'full_analysis_cycle': self._trigger_analysis_cycle,
            'system_health_check': self._system_health_check,
        }

        # 检查是否为Portfolio任务
        portfolio_tasks = [
            'adj_factors_fetch',
            'stock_basic_fetch',
            'industry_classification_fetch',
            'index_components_fetch',
            'data_quality_check',
            'full_data_rebuild'
        ]

        if task_id in task_methods:
            try:
                await task_methods[task_id]()
                return {
                    'task_id': task_id,
                    'status': 'triggered',
                    'timestamp': datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Failed to trigger task {task_id}: {e}")
                raise AdapterException(f"Task trigger failed: {e}")
        elif task_id in portfolio_tasks:
            try:
                result = await self._portfolio_scheduler.trigger_task_manually(task_id)
                return result
            except Exception as e:
                logger.error(f"Failed to trigger portfolio task {task_id}: {e}")
                raise AdapterException(f"Portfolio task trigger failed: {e}")
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

    # ==================== 宏观数据抓取任务 ====================

    async def _trigger_daily_macro_data_fetch(self):
        """触发日度宏观数据抓取"""
        try:
            logger.info("Triggering daily macro data fetch...")

            # 获取FlowhubAdapter实例
            flowhub_adapter = await self._get_flowhub_adapter()

            if flowhub_adapter:
                # 日度宏观数据类型
                daily_data_types = [
                    'interest-rate-data',
                    'stock-index-data',
                    'market-flow-data',
                    'commodity-price-data'
                ]

                job_ids = []

                # 为每种数据类型创建抓取任务
                for data_type in daily_data_types:
                    try:
                        job_result = await flowhub_adapter.create_macro_data_job(
                            data_type=data_type,
                            incremental=True
                        )

                        job_id = job_result.get('job_id')
                        if job_id:
                            job_ids.append((data_type, job_id))
                            logger.info(f"Daily macro data job created for {data_type}: {job_id}")

                    except Exception as e:
                        logger.error(f"Failed to create job for {data_type}: {e}")

                # 记录任务执行结果
                if job_ids:
                    self._record_task_execution(
                        "daily_macro_data_fetch",
                        "completed",
                        f"Created {len(job_ids)} daily macro data jobs: {[jid for _, jid in job_ids]}"
                    )
                else:
                    self._record_task_execution(
                        "daily_macro_data_fetch",
                        "failed",
                        "No jobs created"
                    )

            else:
                logger.warning("FlowhubAdapter not available, skipping daily macro data fetch")
                self._record_task_execution("daily_macro_data_fetch", "skipped", "FlowhubAdapter not available")

        except Exception as e:
            logger.error(f"Daily macro data fetch failed: {e}")
            self._record_task_execution("daily_macro_data_fetch", "failed", str(e))

    async def _trigger_monthly_macro_data_fetch(self):
        """触发月度宏观数据抓取"""
        try:
            logger.info("Triggering monthly macro data fetch...")

            flowhub_adapter = await self._get_flowhub_adapter()

            if flowhub_adapter:
                # 月度宏观数据类型
                monthly_data_types = [
                    'price-index-data',
                    'money-supply-data',
                    'social-financing-data',
                    'investment-data',
                    'industrial-data',
                    'sentiment-index-data',
                    'inventory-cycle-data'
                ]

                job_ids = []

                for data_type in monthly_data_types:
                    try:
                        job_result = await flowhub_adapter.create_macro_data_job(
                            data_type=data_type,
                            incremental=True
                        )

                        job_id = job_result.get('job_id')
                        if job_id:
                            job_ids.append((data_type, job_id))
                            logger.info(f"Monthly macro data job created for {data_type}: {job_id}")

                    except Exception as e:
                        logger.error(f"Failed to create job for {data_type}: {e}")

                if job_ids:
                    self._record_task_execution(
                        "monthly_macro_data_fetch",
                        "completed",
                        f"Created {len(job_ids)} monthly macro data jobs"
                    )
                else:
                    self._record_task_execution("monthly_macro_data_fetch", "failed", "No jobs created")

            else:
                logger.warning("FlowhubAdapter not available, skipping monthly macro data fetch")
                self._record_task_execution("monthly_macro_data_fetch", "skipped", "FlowhubAdapter not available")

        except Exception as e:
            logger.error(f"Monthly macro data fetch failed: {e}")
            self._record_task_execution("monthly_macro_data_fetch", "failed", str(e))

    async def _trigger_quarterly_macro_data_fetch(self):
        """触发季度宏观数据抓取"""
        try:
            logger.info("Triggering quarterly macro data fetch...")

            flowhub_adapter = await self._get_flowhub_adapter()

            if flowhub_adapter:
                # 季度宏观数据类型
                quarterly_data_types = [
                    'gdp-data'
                ]

                job_ids = []

                for data_type in quarterly_data_types:
                    try:
                        job_result = await flowhub_adapter.create_macro_data_job(
                            data_type=data_type,
                            incremental=True
                        )

                        job_id = job_result.get('job_id')
                        if job_id:
                            job_ids.append((data_type, job_id))
                            logger.info(f"Quarterly macro data job created for {data_type}: {job_id}")

                    except Exception as e:
                        logger.error(f"Failed to create job for {data_type}: {e}")

                if job_ids:
                    self._record_task_execution(
                        "quarterly_macro_data_fetch",
                        "completed",
                        f"Created {len(job_ids)} quarterly macro data jobs"
                    )
                else:
                    self._record_task_execution("quarterly_macro_data_fetch", "failed", "No jobs created")

            else:
                logger.warning("FlowhubAdapter not available, skipping quarterly macro data fetch")
                self._record_task_execution("quarterly_macro_data_fetch", "skipped", "FlowhubAdapter not available")

        except Exception as e:
            logger.error(f"Quarterly macro data fetch failed: {e}")
            self._record_task_execution("quarterly_macro_data_fetch", "failed", str(e))

    async def _trigger_yearly_macro_data_fetch(self):
        """触发年度宏观数据抓取"""
        try:
            logger.info("Triggering yearly macro data fetch...")

            flowhub_adapter = await self._get_flowhub_adapter()

            if flowhub_adapter:
                # 年度宏观数据类型
                yearly_data_types = [
                    'innovation-data',
                    'demographic-data'
                ]

                job_ids = []

                for data_type in yearly_data_types:
                    try:
                        job_result = await flowhub_adapter.create_macro_data_job(
                            data_type=data_type,
                            incremental=True
                        )

                        job_id = job_result.get('job_id')
                        if job_id:
                            job_ids.append((data_type, job_id))
                            logger.info(f"Yearly macro data job created for {data_type}: {job_id}")

                    except Exception as e:
                        logger.error(f"Failed to create job for {data_type}: {e}")

                if job_ids:
                    self._record_task_execution(
                        "yearly_macro_data_fetch",
                        "completed",
                        f"Created {len(job_ids)} yearly macro data jobs"
                    )
                else:
                    self._record_task_execution("yearly_macro_data_fetch", "failed", "No jobs created")

            else:
                logger.warning("FlowhubAdapter not available, skipping yearly macro data fetch")
                self._record_task_execution("yearly_macro_data_fetch", "skipped", "FlowhubAdapter not available")

        except Exception as e:
            logger.error(f"Yearly macro data fetch failed: {e}")
            self._record_task_execution("yearly_macro_data_fetch", "failed", str(e))
    
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
            'timestamp': datetime.now(timezone.utc).isoformat()
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
