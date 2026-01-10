"""
宏观数据抓取任务定义

基于asyncron实现的独立宏观数据抓取任务，每个数据类型都有独立的任务。
支持最大历史数据范围抓取和增量更新。
"""

import asyncio
import logging
from datetime import datetime, date
from typing import Dict, List, Any, Optional
import sys
import os

# 导入asyncron模块
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

try:
    from ..adapters.flowhub_adapter import FlowhubAdapter
except Exception:
    from adapters.flowhub_adapter import FlowhubAdapter

logger = logging.getLogger(__name__)


class MacroDataTaskScheduler:
    """宏观数据任务调度器"""

    def __init__(self, config=None, coordinator=None, app=None):
        """初始化调度器"""
        self.config = config
        self.coordinator = coordinator
        self.app = app
        self._planer = create_planer()
        self._setup_macro_data_tasks()

        logger.info("MacroDataTaskScheduler initialized")
    
    def _setup_macro_data_tasks(self):
        """设置所有14个独立的宏观数据抓取任务"""

        # 设置宏观数据任务（包含日度、月度、季度、年度）
        self._setup_daily_macro_tasks()

        # 设置辅助数据任务
        self._setup_auxiliary_data_tasks()

    def _setup_daily_macro_tasks(self):
        """设置日度宏观数据任务"""

        
        # 利率收益率数据 (每日 18:30)
        interest_rate_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["18:30:00"]
        )
        
        @self._planer.task(url='/interest_rate_data_fetch', plans=interest_rate_plan)
        async def interest_rate_data_fetch(context: TaskContext):
            """利率收益率数据抓取任务"""
            await self._fetch_single_macro_data(
                'interest-rate-data',
                'daily',
                context.get_task_id(),
                context.get_task_name()
            )
        
        # 股票指数数据 (每日 18:35)
        stock_index_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["18:35:00"]
        )
        
        @self._planer.task(url='/stock_index_data_fetch', plans=stock_index_plan)
        async def stock_index_data_fetch(context: TaskContext):
            """股票指数数据抓取任务"""
            await self._fetch_single_macro_data(
                'stock-index-data',
                'daily',
                context.get_task_id(),
                context.get_task_name()
            )
        
        # 市场资金流数据 (每日 18:40)
        market_flow_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["18:40:00"]
        )
        
        @self._planer.task(url='/market_flow_data_fetch', plans=market_flow_plan)
        async def market_flow_data_fetch(context: TaskContext):
            """市场资金流数据抓取任务"""
            await self._fetch_single_macro_data(
                'market-flow-data',
                'daily',
                context.get_task_id(),
                context.get_task_name()
            )
        
        # 商品价格数据 (每日 18:45)
        commodity_price_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["18:45:00"]
        )
        
        @self._planer.task(url='/commodity_price_data_fetch', plans=commodity_price_plan)
        async def commodity_price_data_fetch(context: TaskContext):
            """商品价格数据抓取任务"""
            await self._fetch_single_macro_data(
                'commodity-price-data',
                'daily',
                context.get_task_id(),
                context.get_task_name()
            )
        
        # ==================== 月度宏观数据任务 ====================
        
        # 价格指数数据 (每月1日 19:00)
        price_index_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["19:00:00"]
        )
        
        @self._planer.task(url='/price_index_data_fetch', plans=price_index_plan)
        async def price_index_data_fetch(context: TaskContext):
            """价格指数数据抓取任务"""
            today = date.today()
            if today.day == 1:  # 每月1日执行
                await self._fetch_single_macro_data(
                    'price-index-data',
                    'monthly',
                    context.get_task_id(),
                    context.get_task_name()
                )
        
        # 货币供应量数据 (每月1日 19:05)
        money_supply_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["19:05:00"]
        )
        
        @self._planer.task(url='/money_supply_data_fetch', plans=money_supply_plan)
        async def money_supply_data_fetch(context: TaskContext):
            """货币供应量数据抓取任务"""
            today = date.today()
            if today.day == 1:  # 每月1日执行
                await self._fetch_single_macro_data(
                    'money-supply-data',
                    'monthly',
                    context.get_task_id(),
                    context.get_task_name()
                )
        
        # 社会融资数据 (每月1日 19:10)
        social_financing_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["19:10:00"]
        )
        
        @self._planer.task(url='/social_financing_data_fetch', plans=social_financing_plan)
        async def social_financing_data_fetch(context: TaskContext):
            """社会融资数据抓取任务"""
            today = date.today()
            if today.day == 1:  # 每月1日执行
                await self._fetch_single_macro_data(
                    'social-financing-data',
                    'monthly',
                    context.get_task_id(),
                    context.get_task_name()
                )
        
        # 投资统计数据 (每月1日 19:15)
        investment_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["19:15:00"]
        )
        
        @self._planer.task(url='/investment_data_fetch', plans=investment_plan)
        async def investment_data_fetch(context: TaskContext):
            """投资统计数据抓取任务"""
            today = date.today()
            if today.day == 1:  # 每月1日执行
                await self._fetch_single_macro_data(
                    'investment-data',
                    'monthly',
                    context.get_task_id(),
                    context.get_task_name()
                )
        
        # 工业生产数据 (每月1日 19:20)
        industrial_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["19:20:00"]
        )
        
        @self._planer.task(url='/industrial_data_fetch', plans=industrial_plan)
        async def industrial_data_fetch(context: TaskContext):
            """工业生产数据抓取任务"""
            today = date.today()
            if today.day == 1:  # 每月1日执行
                await self._fetch_single_macro_data(
                    'industrial-data',
                    'monthly',
                    context.get_task_id(),
                    context.get_task_name()
                )
        
        # 景气指数数据 (每月1日 19:25)
        sentiment_index_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["19:25:00"]
        )
        
        @self._planer.task(url='/sentiment_index_data_fetch', plans=sentiment_index_plan)
        async def sentiment_index_data_fetch(context: TaskContext):
            """景气指数数据抓取任务"""
            today = date.today()
            if today.day == 1:  # 每月1日执行
                await self._fetch_single_macro_data(
                    'sentiment-index-data',
                    'monthly',
                    context.get_task_id(),
                    context.get_task_name()
                )
        
        # 库存周期数据 (每月1日 19:30)
        inventory_cycle_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["19:30:00"]
        )
        
        @self._planer.task(url='/inventory_cycle_data_fetch', plans=inventory_cycle_plan)
        async def inventory_cycle_data_fetch(context: TaskContext):
            """库存周期数据抓取任务"""
            today = date.today()
            if today.day == 1:  # 每月1日执行
                await self._fetch_single_macro_data(
                    'inventory-cycle-data',
                    'monthly',
                    context.get_task_id(),
                    context.get_task_name()
                )
        
        # ==================== 季度宏观数据任务 ====================
        
        # GDP数据 (每季度第一个月15日 19:35)
        gdp_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["19:35:00"]
        )
        
        @self._planer.task(url='/gdp_data_fetch', plans=gdp_plan)
        async def gdp_data_fetch(context: TaskContext):
            """GDP数据抓取任务"""
            today = date.today()
            if today.day == 15 and today.month in [1, 4, 7, 10]:  # 季度第一个月15日执行
                await self._fetch_single_macro_data(
                    'gdp-data',
                    'quarterly',
                    context.get_task_id(),
                    context.get_task_name()
                )
        
        # ==================== 年度宏观数据任务 ====================
        
        # 技术创新数据 (每年1月15日 20:00)
        innovation_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["20:00:00"]
        )
        
        @self._planer.task(url='/innovation_data_fetch', plans=innovation_plan)
        async def innovation_data_fetch(context: TaskContext):
            """技术创新数据抓取任务"""
            today = date.today()
            if today.month == 1 and today.day == 15:  # 每年1月15日执行
                await self._fetch_single_macro_data(
                    'innovation-data',
                    'yearly',
                    context.get_task_id(),
                    context.get_task_name()
                )
        
        # 人口统计数据 (每年1月15日 20:05)
        demographic_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["20:05:00"]
        )
        
        @self._planer.task(url='/demographic_data_fetch', plans=demographic_plan)
        async def demographic_data_fetch(context: TaskContext):
            """人口统计数据抓取任务"""
            today = date.today()
            if today.month == 1 and today.day == 15:  # 每年1月15日执行
                await self._fetch_single_macro_data(
                    'demographic-data',
                    'yearly',
                    context.get_task_id(),
                    context.get_task_name()
                )
        
        logger.info("All 14 macro data tasks setup completed")
    
    async def _fetch_single_macro_data(
        self,
        data_type: str,
        frequency: str,
        task_id: str | None = None,
        task_name: str | None = None
    ):
        """抓取单个宏观数据类型

        Args:
            data_type: 数据类型
            frequency: 数据频率 (daily, monthly, quarterly, yearly)
        """
        try:
            logger.info(f"Triggering {frequency} {data_type} fetch...")

            # 获取FlowhubAdapter实例
            flowhub_adapter = await self._get_flowhub_adapter()

            if flowhub_adapter:
                # 创建数据抓取任务，使用最大历史数据范围
                job_result = await flowhub_adapter.create_macro_data_job(
                    data_type=data_type,
                    incremental=True,  # 使用增量更新
                    max_history=True   # 首次抓取使用最大历史范围
                )

                job_id = job_result.get('job_id')
                if job_id:
                    logger.info(f"{data_type} job created: {job_id}")
                    try:
                        result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=1800)
                        status = result.get('status')
                        if self._is_success_status(status):
                            await self._notify_task_completed(f"{data_type.replace('-', '_')}_fetch")
                            self._record_task_execution(
                                task_name or f"{data_type.replace('-', '_')}_fetch",
                                "completed",
                                f"{data_type} job completed: {job_id}",
                                task_id
                            )
                            return {'status': 'success', 'job_id': job_id}
                        self._record_task_execution(
                            task_name or f"{data_type.replace('-', '_')}_fetch",
                            "failed",
                            f"{data_type} job failed: {job_id}",
                            task_id
                        )
                        return {'status': 'failed', 'job_id': job_id}
                    except Exception as wait_error:
                        self._record_task_execution(
                            task_name or f"{data_type.replace('-', '_')}_fetch",
                            "failed",
                            f"{data_type} job timeout or error: {wait_error}",
                            task_id
                        )
                        return {'status': 'failed', 'job_id': job_id, 'error': str(wait_error)}
                else:
                    logger.error(f"Failed to create job for {data_type}")
                    self._record_task_execution(
                        task_name or f"{data_type.replace('-', '_')}_fetch",
                        "failed",
                        "No job ID returned",
                        task_id
                    )
                    return {'status': 'failed', 'error': 'No job ID returned'}
            else:
                logger.warning(f"FlowhubAdapter not available for {data_type}")
                self._record_task_execution(
                    task_name or f"{data_type.replace('-', '_')}_fetch",
                    "skipped",
                    "FlowhubAdapter not available",
                    task_id
                )
                return {'status': 'skipped', 'error': 'FlowhubAdapter not available'}

        except Exception as e:
            logger.error(f"{data_type} fetch failed: {e}")
            self._record_task_execution(
                task_name or f"{data_type.replace('-', '_')}_fetch",
                "failed",
                str(e),
                task_id
            )
            return {'status': 'failed', 'error': str(e)}
    
    async def _get_flowhub_adapter(self):
        """获取FlowhubAdapter实例"""
        try:
            flowhub_adapter = FlowhubAdapter(self.config)
            connected = await flowhub_adapter.connect_to_system()

            if connected:
                return flowhub_adapter
            else:
                logger.warning("Failed to connect FlowhubAdapter")
                return None

        except Exception as e:
            logger.error(f"Failed to get FlowhubAdapter: {e}")
            return None

    async def _notify_task_completed(self, task_name: str):
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
    
    def _setup_auxiliary_data_tasks(self):
        """设置辅助数据定时任务"""

        # ==================== 复权因子数据任务 ====================

        # 复权因子数据 (每周六 20:00)
        adj_factors_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["20:00:00"]
        )

        @self._planer.task(url='/adj_factors_data_fetch', plans=adj_factors_plan)
        async def adj_factors_data_fetch(context: TaskContext):
            """复权因子数据抓取任务"""
            today = date.today()
            if today.weekday() == 5:  # 周六 (0=周一, 5=周六)
                logger.info("开始执行复权因子数据抓取任务")
                await self._fetch_auxiliary_data(
                    'adj-factors',
                    'weekly',
                    context.get_task_id(),
                    context.get_task_name()
                )

        # ==================== 指数成分股数据任务 ====================

        # 指数成分股数据 (每月1日 20:30)
        index_components_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["20:30:00"]
        )

        @self._planer.task(url='/index_components_data_fetch', plans=index_components_plan)
        async def index_components_data_fetch(context: TaskContext):
            """指数成分股数据抓取任务"""
            today = date.today()
            if today.day == 1:  # 每月1日执行
                logger.info("开始执行指数成分股数据抓取任务")
                await self._fetch_auxiliary_data(
                    'index-components',
                    'monthly',
                    context.get_task_id(),
                    context.get_task_name()
                )

        # ==================== 板块数据任务 ====================

        # 行业板块数据（月度全量校验，每月1日 21:00）
        industry_board_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["21:00:00"]
        )

        @self._planer.task(url='/industry_board_data_fetch', plans=industry_board_plan)
        async def industry_board_data_fetch(context: TaskContext):
            """行业板块数据抓取任务"""
            today = date.today()
            if today.day == 1:
                logger.info("开始执行行业板块数据抓取任务（月度全量校验）")
                await self._fetch_auxiliary_data(
                    'industry-board',
                    'monthly',
                    context.get_task_id(),
                    context.get_task_name()
                )

        # 概念板块数据（月度全量校验，每月1日 21:30）
        concept_board_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["21:30:00"]
        )

        @self._planer.task(url='/concept_board_data_fetch', plans=concept_board_plan)
        async def concept_board_data_fetch(context: TaskContext):
            """概念板块数据抓取任务"""
            today = date.today()
            if today.day == 1:
                logger.info("开始执行概念板块数据抓取任务（月度全量校验）")
                await self._fetch_auxiliary_data(
                    'concept-board',
                    'monthly',
                    context.get_task_id(),
                    context.get_task_name()
                )

        # 行业板块成分股数据 (每周日 22:00)
        industry_board_stocks_plan = PlansEvery(
            time_units=[TimeUnits.WEEKS],
            every=[1]
        )

        @self._planer.task(url='/industry_board_stocks_data_fetch', plans=industry_board_stocks_plan)
        async def industry_board_stocks_data_fetch(context: TaskContext):
            """行业板块成分股数据抓取任务"""
            logger.info("开始执行行业板块成分股数据抓取任务")
            await self._fetch_auxiliary_data(
                'industry-board-stocks',
                'weekly',
                context.get_task_id(),
                context.get_task_name()
            )

        # 概念板块成分股数据 (每周日 22:30)
        concept_board_stocks_plan = PlansEvery(
            time_units=[TimeUnits.WEEKS],
            every=[1]
        )

        @self._planer.task(url='/concept_board_stocks_data_fetch', plans=concept_board_stocks_plan)
        async def concept_board_stocks_data_fetch(context: TaskContext):
            """概念板块成分股数据抓取任务"""
            logger.info("开始执行概念板块成分股数据抓取任务")
            await self._fetch_auxiliary_data(
                'concept-board-stocks',
                'weekly',
                context.get_task_id(),
                context.get_task_name()
            )

        # ==================== 板块成分股月度全量校验任务 ====================

        industry_board_stocks_full_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["23:00:00"]
        )

        @self._planer.task(url='/industry_board_stocks_full_fetch', plans=industry_board_stocks_full_plan)
        async def industry_board_stocks_full_fetch(context: TaskContext):
            """行业板块成分股月度全量校验"""
            today = date.today()
            if today.day == 1:
                logger.info("开始执行行业板块成分股数据抓取任务（月度全量校验）")
                await self._fetch_auxiliary_data(
                    'industry-board-stocks',
                    'monthly',
                    context.get_task_id(),
                    context.get_task_name()
                )

        concept_board_stocks_full_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["23:30:00"]
        )

        @self._planer.task(url='/concept_board_stocks_full_fetch', plans=concept_board_stocks_full_plan)
        async def concept_board_stocks_full_fetch(context: TaskContext):
            """概念板块成分股月度全量校验"""
            today = date.today()
            if today.day == 1:
                logger.info("开始执行概念板块成分股数据抓取任务（月度全量校验）")
                await self._fetch_auxiliary_data(
                    'concept-board-stocks',
                    'monthly',
                    context.get_task_id(),
                    context.get_task_name()
                )

    async def _fetch_auxiliary_data(
        self,
        data_type: str,
        frequency: str,
        task_id: str | None = None,
        task_name: str | None = None
    ):
        """抓取辅助数据的通用方法"""
        try:
            logger.info(f"开始抓取{data_type}数据 (频率: {frequency})")

            flowhub_adapter = await self._get_flowhub_adapter()
            if flowhub_adapter:
                # 根据数据类型选择不同的参数
                if data_type == 'adj-factors':
                    # 复权因子数据抓取 - 增量更新模式
                    job_result = await flowhub_adapter.create_portfolio_data_job(
                        data_type='adj_factors',
                        update_mode='incremental'
                    )
                elif data_type == 'index-components':
                    # 指数成分股数据抓取 - 抓取主要指数
                    major_indices = [
                        '000016.SH',  # 上证50
                        '000300.SH',  # 沪深300
                        '000905.SH',  # 中证500
                        '399006.SZ',  # 创业板指
                        '399001.SZ',  # 深证成指
                    ]
                    job_result = await flowhub_adapter.create_portfolio_data_job(
                        data_type='index_components',
                        update_mode='incremental',
                        index_codes=major_indices
                    )
                elif data_type == 'industry-board':
                    # 行业板块数据抓取 - 周期更新（周更增量，月度全量）
                    update_mode = 'full_update' if frequency == 'monthly' else 'incremental'
                    job_result = await flowhub_adapter.send_request({
                        "method": "POST",
                        "endpoint": "/api/v1/jobs/industry-board",
                        "payload": {
                            "source": "ths",
                            "update_mode": update_mode
                        }
                    })
                elif data_type == 'concept-board':
                    # 概念板块数据抓取 - 周期更新（周更增量，月度全量）
                    update_mode = 'full_update' if frequency == 'monthly' else 'incremental'
                    job_result = await flowhub_adapter.send_request({
                        "method": "POST",
                        "endpoint": "/api/v1/jobs/concept-board",
                        "payload": {
                            "source": "ths",
                            "update_mode": update_mode
                        }
                    })
                elif data_type == 'industry-board-stocks':
                    # 行业板块成分股数据抓取 - 周期更新（周更增量，月度全量）
                    update_mode = 'full_update' if frequency == 'monthly' else 'incremental'
                    job_result = await flowhub_adapter.send_request({
                        "method": "POST",
                        "endpoint": "/api/v1/jobs/industry-board-stocks",
                        "payload": {
                            "source": "ths",
                            "update_mode": update_mode
                        }
                    })
                elif data_type == 'concept-board-stocks':
                    # 概念板块成分股数据抓取 - 周期更新（周更增量，月度全量）
                    update_mode = 'full_update' if frequency == 'monthly' else 'incremental'
                    job_result = await flowhub_adapter.send_request({
                        "method": "POST",
                        "endpoint": "/api/v1/jobs/concept-board-stocks",
                        "payload": {
                            "source": "ths",
                            "update_mode": update_mode
                        }
                    })
                else:
                    logger.warning(f"Unknown auxiliary data type: {data_type}")
                    return {'status': 'failed', 'error': f'Unknown data type: {data_type}'}

                job_id = job_result.get('job_id')
                if job_id:
                    logger.info(f"{data_type} job created: {job_id}")
                    try:
                        result = await flowhub_adapter.wait_for_job_completion(job_id, timeout=1800)
                        status = result.get('status')
                        if self._is_success_status(status):
                            await self._notify_task_completed(f"{data_type.replace('-', '_')}_fetch")
                            self._record_task_execution(
                                task_name or f"{data_type.replace('-', '_')}_fetch",
                                "completed",
                                f"{data_type} job completed: {job_id}",
                                task_id
                            )
                            return {'status': 'success', 'job_id': job_id}
                        self._record_task_execution(
                            task_name or f"{data_type.replace('-', '_')}_fetch",
                            "failed",
                            f"{data_type} job failed: {job_id}",
                            task_id
                        )
                        return {'status': 'failed', 'job_id': job_id}
                    except Exception as wait_error:
                        self._record_task_execution(
                            task_name or f"{data_type.replace('-', '_')}_fetch",
                            "failed",
                            f"{data_type} job timeout or error: {wait_error}",
                            task_id
                        )
                        return {'status': 'failed', 'job_id': job_id, 'error': str(wait_error)}
                else:
                    logger.error(f"Failed to create job for {data_type}")
                    self._record_task_execution(
                        task_name or f"{data_type.replace('-', '_')}_fetch",
                        "failed",
                        "No job ID returned",
                        task_id
                    )
                    return {'status': 'failed', 'error': 'No job ID returned'}
            else:
                logger.warning(f"FlowhubAdapter not available for {data_type}")
                self._record_task_execution(
                    task_name or f"{data_type.replace('-', '_')}_fetch",
                    "skipped",
                    "FlowhubAdapter not available",
                    task_id
                )
                return {'status': 'skipped', 'error': 'FlowhubAdapter not available'}

        except Exception as e:
            logger.error(f"{data_type} fetch failed: {e}")
            self._record_task_execution(
                task_name or f"{data_type.replace('-', '_')}_fetch",
                "failed",
                str(e),
                task_id
            )
            return {'status': 'failed', 'error': str(e)}

    def _record_task_execution(self, task_name: str, status: str, message: str, task_id: str | None = None):
        """记录任务执行历史"""
        logger.info(f"Macro task execution recorded: {task_name} - {status}")

        if self.app and 'scheduler' in self.app:
            try:
                self.app['scheduler'].record_task_execution(task_name, status, message, task_id)
            except Exception as e:
                logger.warning(f"Failed to record task history to scheduler: {e}")

    @staticmethod
    def _is_success_status(status: str | None) -> bool:
        return status in {"completed", "succeeded"}

    def get_planer(self):
        """获取asyncron planer"""
        return self._planer
