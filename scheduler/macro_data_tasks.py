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

logger = logging.getLogger(__name__)


class MacroDataTaskScheduler:
    """宏观数据任务调度器"""
    
    def __init__(self, config=None, coordinator=None):
        """初始化调度器"""
        self.config = config
        self.coordinator = coordinator
        self._planer = create_planer()
        self._setup_macro_data_tasks()
        
        logger.info("MacroDataTaskScheduler initialized")
    
    def _setup_macro_data_tasks(self):
        """设置所有14个独立的宏观数据抓取任务"""
        
        # ==================== 日度宏观数据任务 ====================
        
        # 利率收益率数据 (每日 18:30)
        interest_rate_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["18:30:00"]
        )
        
        @self._planer.task(url='/interest_rate_data_fetch', plans=interest_rate_plan)
        async def interest_rate_data_fetch(context: TaskContext):
            """利率收益率数据抓取任务"""
            await self._fetch_single_macro_data('interest-rate-data', 'daily')
        
        # 股票指数数据 (每日 18:35)
        stock_index_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["18:35:00"]
        )
        
        @self._planer.task(url='/stock_index_data_fetch', plans=stock_index_plan)
        async def stock_index_data_fetch(context: TaskContext):
            """股票指数数据抓取任务"""
            await self._fetch_single_macro_data('stock-index-data', 'daily')
        
        # 市场资金流数据 (每日 18:40)
        market_flow_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["18:40:00"]
        )
        
        @self._planer.task(url='/market_flow_data_fetch', plans=market_flow_plan)
        async def market_flow_data_fetch(context: TaskContext):
            """市场资金流数据抓取任务"""
            await self._fetch_single_macro_data('market-flow-data', 'daily')
        
        # 商品价格数据 (每日 18:45)
        commodity_price_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=["18:45:00"]
        )
        
        @self._planer.task(url='/commodity_price_data_fetch', plans=commodity_price_plan)
        async def commodity_price_data_fetch(context: TaskContext):
            """商品价格数据抓取任务"""
            await self._fetch_single_macro_data('commodity-price-data', 'daily')
        
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
                await self._fetch_single_macro_data('price-index-data', 'monthly')
        
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
                await self._fetch_single_macro_data('money-supply-data', 'monthly')
        
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
                await self._fetch_single_macro_data('social-financing-data', 'monthly')
        
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
                await self._fetch_single_macro_data('investment-data', 'monthly')
        
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
                await self._fetch_single_macro_data('industrial-data', 'monthly')
        
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
                await self._fetch_single_macro_data('sentiment-index-data', 'monthly')
        
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
                await self._fetch_single_macro_data('inventory-cycle-data', 'monthly')
        
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
                await self._fetch_single_macro_data('gdp-data', 'quarterly')
        
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
                await self._fetch_single_macro_data('innovation-data', 'yearly')
        
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
                await self._fetch_single_macro_data('demographic-data', 'yearly')
        
        logger.info("All 14 macro data tasks setup completed")
    
    async def _fetch_single_macro_data(self, data_type: str, frequency: str):
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
                    return {'status': 'success', 'job_id': job_id}
                else:
                    logger.error(f"Failed to create job for {data_type}")
                    return {'status': 'failed', 'error': 'No job ID returned'}
            else:
                logger.warning(f"FlowhubAdapter not available for {data_type}")
                return {'status': 'skipped', 'error': 'FlowhubAdapter not available'}
                
        except Exception as e:
            logger.error(f"{data_type} fetch failed: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    async def _get_flowhub_adapter(self):
        """获取FlowhubAdapter实例"""
        try:
            from ..adapters import FlowhubAdapter
            
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
    
    def get_planer(self):
        """获取asyncron planer"""
        return self._planer
