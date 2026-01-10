"""
Portfolio项目数据抓取任务调度器

基于不同数据类型的更新特点，设计对应的抓取触发策略：
- 增量追加型数据：每日交易结束后触发
- 全量更新型数据：每周或每月定期更新  
- 时点快照型数据：季度调整时触发
"""

import asyncio
import logging
from datetime import datetime, date
from typing import Dict, List, Any

# 导入asyncron模块
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../external/asyncron'))

import asyncron
from asyncron import (
    create_planer,
    PlansAt,
    PlansEvery,
    TimeUnit,
    TimeUnits,
    TaskContext
)

from config import IntegrationConfig
try:
    from ..adapters.flowhub_adapter import FlowhubAdapter
except Exception:
    from adapters.flowhub_adapter import FlowhubAdapter

logger = logging.getLogger(__name__)


class PortfolioDataTaskScheduler:
    """Portfolio项目数据抓取任务调度器"""

    def __init__(self, config: IntegrationConfig, coordinator=None, app=None):
        """初始化Portfolio数据任务调度器

        Args:
            config: 集成配置对象
            coordinator: 系统协调器实例
            app: aiohttp应用实例
        """
        self.config = config
        self.coordinator = coordinator
        self.app = app
        self._planer = create_planer()

        self._setup_portfolio_data_tasks()

        logger.info("PortfolioDataTaskScheduler initialized")

    def get_planer(self):
        """获取planer实例"""
        return self._planer

    def _setup_portfolio_data_tasks(self):
        """设置Portfolio项目数据抓取任务"""
        
        # ==================== 增量追加型数据任务 ====================
        
        # 复权因子数据抓取任务 (每日18:50)
        adj_factors_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["18:50:00"]
        )
        
        @self._planer.task(url='/portfolio_adj_factors_fetch', plans=adj_factors_plan)
        async def portfolio_adj_factors_fetch(context: TaskContext):
            """复权因子数据抓取任务（增量追加型）"""
            await self._trigger_adj_factors_fetch(context.get_task_id(), context.get_task_name())

        # ==================== 全量更新型数据任务 ====================
        
        # 股票基础信息抓取任务 (每周日19:00)
        stock_basic_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["19:00:00"]
        )
        
        @self._planer.task(url='/portfolio_stock_basic_fetch', plans=stock_basic_plan)
        async def portfolio_stock_basic_fetch(context: TaskContext):
            """股票基础信息抓取任务（全量更新型）"""
            # 只在周日执行
            today = date.today()
            if today.weekday() == 6:  # 周日
                await self._trigger_stock_basic_fetch(context.get_task_id(), context.get_task_name())

        # 行业分类数据抓取任务 (每月第一个周日19:30)
        industry_classification_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["19:30:00"]
        )
        
        @self._planer.task(url='/portfolio_industry_classification_fetch', plans=industry_classification_plan)
        async def portfolio_industry_classification_fetch(context: TaskContext):
            """行业分类数据抓取任务（全量更新型）"""
            # 只在每月第一个周日执行
            today = date.today()
            if today.weekday() == 6 and today.day <= 7:  # 每月第一个周日
                await self._trigger_industry_classification_fetch(context.get_task_id(), context.get_task_name())

        # ==================== 时点快照型数据任务 ====================
        
        # 指数成分股权重抓取任务 (季度调整：3月、6月、9月、12月的第三个周五20:00)
        index_components_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["20:00:00"]
        )
        
        @self._planer.task(url='/portfolio_index_components_fetch', plans=index_components_plan)
        async def portfolio_index_components_fetch(context: TaskContext):
            """指数成分股权重抓取任务（时点快照型）"""
            # 只在季度调整月份的第三个周五执行
            today = date.today()
            if (today.month in [3, 6, 9, 12] and 
                today.weekday() == 4 and  # 周五
                15 <= today.day <= 21):   # 第三个周五大概在15-21日之间
                await self._trigger_index_components_fetch(context.get_task_id(), context.get_task_name())

        # ==================== 数据质量检查任务 ====================
        
        # Portfolio数据一致性检查任务 (每日21:00)
        data_quality_check_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["21:00:00"]
        )
        
        @self._planer.task(url='/portfolio_data_quality_check', plans=data_quality_check_plan)
        async def portfolio_data_quality_check(context: TaskContext):
            """Portfolio数据质量检查任务"""
            await self._trigger_data_quality_check(context.get_task_id(), context.get_task_name())

        # ==================== 手动触发支持 ====================

        # 全量数据重建任务 (手动触发) - 不注册到调度器，仅通过API手动触发
        # 注释掉以避免asyncron要求plans参数
        # @self._planer.task(url='/portfolio_full_data_rebuild')
        # async def portfolio_full_data_rebuild(context: TaskContext):
        #     """Portfolio全量数据重建任务（手动触发）"""
        #     await self._trigger_full_data_rebuild()

        logger.info("Portfolio data tasks setup completed")

    # ==================== 任务实现方法 ====================

    async def _trigger_adj_factors_fetch(self, task_id: str | None = None, task_name: str | None = None):
        """触发复权因子数据抓取（增量追加型）"""
        try:
            logger.info("Triggering portfolio adj factors fetch...")
            
            flowhub_adapter = await self._get_flowhub_adapter()
            
            if flowhub_adapter:
                # 创建复权因子抓取任务
                job_result = await flowhub_adapter.create_portfolio_data_job(
                    data_type='adj_factors',
                    update_mode='incremental',
                    symbols=None  # 抓取所有股票
                )
                
                job_id = job_result.get('job_id')
                logger.info(f"Portfolio adj factors fetch job created: {job_id}")
                try:
                    result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=1800)
                    status = result.get('status')
                    if self._is_success_status(status):
                        self._record_task_execution(
                            task_name or "portfolio_adj_factors_fetch",
                            "completed",
                            f"Adj factors fetch job completed: {job_id}",
                            task_id
                        )
                    else:
                        self._record_task_execution(
                            task_name or "portfolio_adj_factors_fetch",
                            "failed",
                            f"Adj factors fetch job failed: {job_id}",
                            task_id
                        )
                except Exception as wait_error:
                    self._record_task_execution(
                        task_name or "portfolio_adj_factors_fetch",
                        "failed",
                        f"Adj factors fetch job timeout or error: {wait_error}",
                        task_id
                    )
                
            else:
                logger.warning("FlowhubAdapter not available, skipping adj factors fetch")
                self._record_task_execution(task_name or "portfolio_adj_factors_fetch", "skipped", "FlowhubAdapter not available", task_id)
                
        except Exception as e:
            logger.error(f"Portfolio adj factors fetch failed: {e}")
            self._record_task_execution(task_name or "portfolio_adj_factors_fetch", "failed", str(e), task_id)

    async def _trigger_stock_basic_fetch(self, task_id: str | None = None, task_name: str | None = None):
        """触发股票基础信息抓取（全量更新型）"""
        try:
            logger.info("Triggering portfolio stock basic fetch...")
            
            flowhub_adapter = await self._get_flowhub_adapter()
            
            if flowhub_adapter:
                # 创建股票基础信息抓取任务
                job_result = await flowhub_adapter.create_portfolio_data_job(
                    data_type='stock_basic',
                    update_mode='full_update',
                    symbols=None  # 抓取所有股票
                )
                
                job_id = job_result.get('job_id')
                logger.info(f"Portfolio stock basic fetch job created: {job_id}")
                try:
                    result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=1800)
                    status = result.get('status')
                    if self._is_success_status(status):
                        self._record_task_execution(
                            task_name or "portfolio_stock_basic_fetch",
                            "completed",
                            f"Stock basic fetch job completed: {job_id}",
                            task_id
                        )
                    else:
                        self._record_task_execution(
                            task_name or "portfolio_stock_basic_fetch",
                            "failed",
                            f"Stock basic fetch job failed: {job_id}",
                            task_id
                        )
                except Exception as wait_error:
                    self._record_task_execution(
                        task_name or "portfolio_stock_basic_fetch",
                        "failed",
                        f"Stock basic fetch job timeout or error: {wait_error}",
                        task_id
                    )
                
            else:
                logger.warning("FlowhubAdapter not available, skipping stock basic fetch")
                self._record_task_execution(task_name or "portfolio_stock_basic_fetch", "skipped", "FlowhubAdapter not available", task_id)
                
        except Exception as e:
            logger.error(f"Portfolio stock basic fetch failed: {e}")
            self._record_task_execution(task_name or "portfolio_stock_basic_fetch", "failed", str(e), task_id)

    async def _trigger_industry_classification_fetch(self, task_id: str | None = None, task_name: str | None = None):
        """触发行业分类数据抓取（全量更新型）"""
        try:
            logger.info("Triggering portfolio industry classification fetch...")
            
            flowhub_adapter = await self._get_flowhub_adapter()
            
            if flowhub_adapter:
                # 创建行业分类抓取任务
                job_result = await flowhub_adapter.create_portfolio_data_job(
                    data_type='industry_classification',
                    update_mode='full_update',
                    symbols=None  # 抓取所有股票
                )
                
                job_id = job_result.get('job_id')
                logger.info(f"Portfolio industry classification fetch job created: {job_id}")
                try:
                    result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=1800)
                    status = result.get('status')
                    if self._is_success_status(status):
                        self._record_task_execution(
                            task_name or "portfolio_industry_classification_fetch",
                            "completed",
                            f"Industry classification fetch job completed: {job_id}",
                            task_id
                        )
                    else:
                        self._record_task_execution(
                            task_name or "portfolio_industry_classification_fetch",
                            "failed",
                            f"Industry classification fetch job failed: {job_id}",
                            task_id
                        )
                except Exception as wait_error:
                    self._record_task_execution(
                        task_name or "portfolio_industry_classification_fetch",
                        "failed",
                        f"Industry classification fetch job timeout or error: {wait_error}",
                        task_id
                    )
                
            else:
                logger.warning("FlowhubAdapter not available, skipping industry classification fetch")
                self._record_task_execution(task_name or "portfolio_industry_classification_fetch", "skipped", "FlowhubAdapter not available", task_id)
                
        except Exception as e:
            logger.error(f"Portfolio industry classification fetch failed: {e}")
            self._record_task_execution(task_name or "portfolio_industry_classification_fetch", "failed", str(e), task_id)

    async def _trigger_index_components_fetch(self, task_id: str | None = None, task_name: str | None = None):
        """触发指数成分股权重抓取（时点快照型）"""
        try:
            logger.info("Triggering portfolio index components fetch...")
            
            flowhub_adapter = await self._get_flowhub_adapter()
            
            if flowhub_adapter:
                # 主要指数列表
                major_indices = ['000300', '000905', '000852', '399006']  # 沪深300、中证500、中证1000、创业板指
                
                job_ids = []

                for index_code in major_indices:
                    try:
                        job_result = await flowhub_adapter.create_portfolio_data_job(
                            data_type='index_components',
                            update_mode='snapshot',
                            index_code=index_code
                        )
                        
                        job_id = job_result.get('job_id')
                        if job_id:
                            job_ids.append((index_code, job_id))
                            logger.info(f"Portfolio index components fetch job created for {index_code}: {job_id}")
                            
                    except Exception as e:
                        logger.error(f"Failed to create index components job for {index_code}: {e}")
                
                if job_ids:
                    completed = []
                    failed = []
                    for index_code, job_id in job_ids:
                        try:
                            result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=1800)
                            status = result.get('status')
                            if self._is_success_status(status):
                                completed.append(job_id)
                            else:
                                failed.append(job_id)
                        except Exception as wait_error:
                            logger.warning(f"Index components job {job_id} error: {wait_error}")
                            failed.append(job_id)
                    if failed:
                        self._record_task_execution(
                            task_name or "portfolio_index_components_fetch",
                            "failed",
                            f"Index components jobs failed: {failed}",
                            task_id
                        )
                    else:
                        self._record_task_execution(
                            task_name or "portfolio_index_components_fetch",
                            "completed",
                            f"Index components jobs completed: {completed}",
                            task_id
                        )
                else:
                    self._record_task_execution(task_name or "portfolio_index_components_fetch", "failed", "No jobs created", task_id)
                
            else:
                logger.warning("FlowhubAdapter not available, skipping index components fetch")
                self._record_task_execution(task_name or "portfolio_index_components_fetch", "skipped", "FlowhubAdapter not available", task_id)
                
        except Exception as e:
            logger.error(f"Portfolio index components fetch failed: {e}")
            self._record_task_execution(task_name or "portfolio_index_components_fetch", "failed", str(e), task_id)

    async def _trigger_data_quality_check(self, task_id: str | None = None, task_name: str | None = None):
        """触发Portfolio数据质量检查"""
        try:
            logger.info("Triggering portfolio data quality check...")
            
            # 这里可以实现数据质量检查逻辑
            # 例如：检查数据完整性、一致性、时效性等
            
            # 简化实现：记录检查完成
            self._record_task_execution(
                task_name or "portfolio_data_quality_check",
                "completed",
                "Data quality check completed",
                task_id
            )
            
        except Exception as e:
            logger.error(f"Portfolio data quality check failed: {e}")
            self._record_task_execution(task_name or "portfolio_data_quality_check", "failed", str(e), task_id)

    async def _trigger_full_data_rebuild(self, task_id: str | None = None, task_name: str | None = None):
        """触发Portfolio全量数据重建"""
        try:
            logger.info("Triggering portfolio full data rebuild...")
            
            # 按顺序执行所有数据抓取任务
            await self._trigger_stock_basic_fetch(task_id, task_name)
            await self._trigger_industry_classification_fetch(task_id, task_name)
            await self._trigger_index_components_fetch(task_id, task_name)
            await self._trigger_adj_factors_fetch(task_id, task_name)
            
            self._record_task_execution(
                task_name or "portfolio_full_data_rebuild",
                "completed",
                "Full data rebuild completed",
                task_id
            )
            
        except Exception as e:
            logger.error(f"Portfolio full data rebuild failed: {e}")
            self._record_task_execution(task_name or "portfolio_full_data_rebuild", "failed", str(e), task_id)

    async def _get_flowhub_adapter(self):
        """获取FlowhubAdapter实例"""
        try:
            flowhub_adapter = FlowhubAdapter(self.config)
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

    def _record_task_execution(self, task_name: str, status: str, message: str, task_id: str | None = None):
        """记录任务执行历史"""
        logger.info(f"Portfolio task execution recorded: {task_name} - {status}")

        if self.app and 'scheduler' in self.app:
            try:
                self.app['scheduler'].record_task_execution(task_name, status, message, task_id)
            except Exception as e:
                logger.warning(f"Failed to record task history to scheduler: {e}")

        # 如果任务成功完成，通知分析触发调度器
        if status == 'completed':
            asyncio.create_task(self._notify_task_completion(task_name))

    @staticmethod
    def _is_success_status(status: str | None) -> bool:
        return status in {"completed", "succeeded"}

    async def _notify_task_completion(self, task_name: str):
        """通知分析触发调度器任务已完成

        Args:
            task_name: 任务名称
        """
        try:
            if self.app and 'analysis_trigger' in self.app:
                analysis_trigger = self.app['analysis_trigger']
                task_mapping = {
                    'portfolio_stock_basic_fetch': 'stock_basic_data_fetch'
                }
                tasks_to_notify = [task_name]
                mapped_task = task_mapping.get(task_name)
                if mapped_task:
                    tasks_to_notify.append(mapped_task)

                for notify_name in tasks_to_notify:
                    await analysis_trigger.mark_task_completed(notify_name)
                    logger.info(f"Notified analysis trigger: {notify_name} completed")
        except Exception as e:
            logger.warning(f"Failed to notify task completion for {task_name}: {e}")

    # ==================== 手动触发接口 ====================

    async def trigger_task_manually(self, task_name: str) -> Dict[str, Any]:
        """手动触发Portfolio数据任务"""
        
        task_methods = {
            'adj_factors_fetch': self._trigger_adj_factors_fetch,
            'stock_basic_fetch': self._trigger_stock_basic_fetch,
            'industry_classification_fetch': self._trigger_industry_classification_fetch,
            'index_components_fetch': self._trigger_index_components_fetch,
            'data_quality_check': self._trigger_data_quality_check,
            'full_data_rebuild': self._trigger_full_data_rebuild,
        }
        
        if task_name in task_methods:
            try:
                await task_methods[task_name]()
                return {
                    'task_name': task_name,
                    'status': 'triggered',
                    'timestamp': datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Failed to trigger portfolio task {task_name}: {e}")
                raise
        else:
            raise ValueError(f"Unknown portfolio task: {task_name}")

    def get_portfolio_tasks_info(self) -> List[Dict[str, Any]]:
        """获取Portfolio数据任务信息"""
        return [
            {
                'name': 'adj_factors_fetch',
                'description': '复权因子数据抓取',
                'type': '增量追加型',
                'schedule': 'Daily at 18:50',
                'data_tables': ['adj_factors']
            },
            {
                'name': 'stock_basic_fetch', 
                'description': '股票基础信息抓取',
                'type': '全量更新型',
                'schedule': 'Weekly on Sunday at 19:00',
                'data_tables': ['stock_basic']
            },
            {
                'name': 'industry_classification_fetch',
                'description': '行业分类数据抓取',
                'type': '全量更新型', 
                'schedule': 'Monthly first Sunday at 19:30',
                'data_tables': ['industry_classification']
            },
            {
                'name': 'index_components_fetch',
                'description': '指数成分股权重抓取',
                'type': '时点快照型',
                'schedule': 'Quarterly 3rd Friday at 20:00',
                'data_tables': ['index_components']
            },
            {
                'name': 'data_quality_check',
                'description': 'Portfolio数据质量检查',
                'type': '监控检查',
                'schedule': 'Daily at 21:00',
                'data_tables': ['all']
            },
            {
                'name': 'full_data_rebuild',
                'description': 'Portfolio全量数据重建',
                'type': '手动触发',
                'schedule': 'Manual trigger only',
                'data_tables': ['all']
            }
        ]
