"""
分析任务自动触发调度器

当数据抓取任务完成后，自动触发依赖该数据的分析任务
"""

import asyncio
import logging
from typing import Dict, Any, List, Set
from datetime import datetime

from asyncron import (
    create_planer,
    PlansAt,
    PlansEvery,
    TimeUnit,
    TimeUnits,
    TaskContext
)

logger = logging.getLogger(__name__)


class AnalysisTriggerScheduler:
    """分析任务自动触发调度器"""
    
    def __init__(self, app):
        """初始化调度器
        
        Args:
            app: aiohttp应用实例
        """
        self.app = app
        self._planer = create_planer()
        
        # 数据依赖映射：分析任务 -> 所需数据抓取任务
        # 任务名称必须与各调度器中的实际任务名称一致
        self.analysis_dependencies = {
            'macro_analysis': {
                # 季度数据
                'gdp_data_fetch',
                # 月度数据
                'price_index_data_fetch',  # CPI/PPI
                'money_supply_data_fetch',  # M0/M1/M2
                'social_financing_data_fetch',
                'investment_data_fetch',  # 固定资产投资
                'industrial_data_fetch',  # 工业增加值
                'sentiment_index_data_fetch',  # PMI
                # 日度数据
                'interest_rate_data_fetch',
                'stock_index_data_fetch',
                'market_flow_data_fetch',
                'commodity_price_data_fetch'
            },
            'stock_batch_analysis': {
                # 股票核心数据（必需）
                'stock_basic_data_fetch',  # 股票基本信息
                'stock_daily_data_fetch',  # 股票日K线数据
                # 股票辅助数据（可选，但建议包含）
                'stock_index_data_fetch',  # 股票指数
                'industry_board_data_fetch',  # 行业板块
                'concept_board_data_fetch'  # 概念板块
            }
        }
        
        # 已完成的数据抓取任务（使用Redis存储）
        self.completed_tasks: Set[str] = set()
        
        # 上次检查时间
        self.last_check_time: Dict[str, datetime] = {}
        
        self._setup_tasks()
        
    def _setup_tasks(self):
        """设置定时任务"""
        
        # ==================== 定时检查任务 ====================
        
        # 每小时检查一次是否可以触发宏观分析
        macro_check_plan = PlansEvery(
            time_units=[TimeUnits.HOURS],
            every=[1]
        )
        
        @self._planer.task(url='/check_macro_analysis_trigger', plans=macro_check_plan)
        async def check_macro_analysis_trigger(context: TaskContext):
            """检查是否可以触发宏观分析"""
            logger.info("检查宏观分析触发条件")
            await self._check_and_trigger_analysis('macro_analysis')
        
        # 每天检查一次是否可以触发股票批量分析
        stock_check_plan = PlansAt(
            time_unit=[TimeUnit.DAY],
            at=['22:00']
        )
        
        @self._planer.task(url='/check_stock_analysis_trigger', plans=stock_check_plan)
        async def check_stock_analysis_trigger(context: TaskContext):
            """检查是否可以触发股票批量分析"""
            logger.info("检查股票批量分析触发条件")
            await self._check_and_trigger_analysis('stock_batch_analysis')
        
        logger.info("Analysis trigger tasks setup completed")
    
    def get_planer(self):
        """获取asyncron planer（蓝图）"""
        return self._planer
    
    async def mark_task_completed(self, task_name: str):
        """标记数据抓取任务已完成
        
        Args:
            task_name: 任务名称
        """
        try:
            # 添加到已完成任务集合
            self.completed_tasks.add(task_name)
            
            # 同时存储到Redis（持久化）
            redis = self.app.get('redis')
            if redis:
                await redis.sadd('completed_data_tasks', task_name)
                # 设置过期时间为7天
                await redis.expire('completed_data_tasks', 7 * 24 * 3600)
            
            logger.info(f"数据抓取任务已标记为完成: {task_name}")
            
            # 立即检查是否可以触发分析
            await self._check_all_analysis_triggers()
            
        except Exception as e:
            logger.error(f"标记任务完成失败 {task_name}: {e}")
    
    async def _check_all_analysis_triggers(self):
        """检查所有分析任务的触发条件"""
        for analysis_name in self.analysis_dependencies.keys():
            await self._check_and_trigger_analysis(analysis_name)
    
    async def _check_and_trigger_analysis(self, analysis_name: str):
        """检查并触发分析任务
        
        Args:
            analysis_name: 分析任务名称 ('macro_analysis' 或 'stock_batch_analysis')
        """
        try:
            # 获取所需的数据抓取任务
            required_tasks = self.analysis_dependencies.get(analysis_name, set())
            
            if not required_tasks:
                logger.warning(f"未找到分析任务的依赖配置: {analysis_name}")
                return
            
            # 从Redis加载已完成任务（如果本地集合为空）
            if not self.completed_tasks:
                await self._load_completed_tasks_from_redis()
            
            # 检查所有依赖任务是否都已完成
            missing_tasks = required_tasks - self.completed_tasks
            
            if missing_tasks:
                logger.info(
                    f"分析任务 {analysis_name} 的依赖未满足，"
                    f"缺少: {', '.join(missing_tasks)}"
                )
                return
            
            # 检查是否在冷却期内（避免频繁触发）
            if not self._should_trigger(analysis_name):
                logger.info(f"分析任务 {analysis_name} 在冷却期内，跳过触发")
                return
            
            # 所有依赖都已满足，触发分析
            logger.info(f"所有依赖已满足，触发分析任务: {analysis_name}")
            
            if analysis_name == 'macro_analysis':
                await self._trigger_macro_analysis()
            elif analysis_name == 'stock_batch_analysis':
                await self._trigger_stock_batch_analysis()
            
            # 更新最后触发时间
            self.last_check_time[analysis_name] = datetime.now()
            
            # 清空已完成任务集合（为下一轮准备）
            self.completed_tasks.clear()
            redis = self.app.get('redis')
            if redis:
                await redis.delete('completed_data_tasks')
            
        except Exception as e:
            logger.error(f"检查并触发分析任务失败 {analysis_name}: {e}")
    
    def _should_trigger(self, analysis_name: str) -> bool:
        """检查是否应该触发分析（冷却期检查）
        
        Args:
            analysis_name: 分析任务名称
            
        Returns:
            是否应该触发
        """
        last_time = self.last_check_time.get(analysis_name)
        if not last_time:
            return True
        
        # 宏观分析：至少间隔6小时
        # 股票批量分析：至少间隔12小时
        cooldown_hours = 6 if analysis_name == 'macro_analysis' else 12
        
        elapsed = (datetime.now() - last_time).total_seconds() / 3600
        return elapsed >= cooldown_hours
    
    async def _load_completed_tasks_from_redis(self):
        """从Redis加载已完成任务"""
        try:
            redis = self.app.get('redis')
            if redis:
                tasks = await redis.smembers('completed_data_tasks')
                if tasks:
                    self.completed_tasks = set(tasks)
                    logger.info(f"从Redis加载了 {len(tasks)} 个已完成任务")
        except Exception as e:
            logger.error(f"从Redis加载已完成任务失败: {e}")
    
    async def _trigger_macro_analysis(self):
        """触发宏观分析"""
        try:
            # 获取MacroAdapter
            macro_adapter = self.app.get('macro_adapter')
            if not macro_adapter:
                logger.error("MacroAdapter未初始化")
                return
            
            # 调用宏观分析API
            analysis_params = {
                'force_refresh': True,
                'include_details': True
            }
            
            result = await macro_adapter.trigger_macro_analysis(analysis_params)
            logger.info(f"宏观分析任务已触发: {result}")
            
        except Exception as e:
            logger.error(f"触发宏观分析失败: {e}")
    
    async def _trigger_stock_batch_analysis(self):
        """触发股票批量分析

        注意：
        1. 不传入symbols参数，让Execution服务自动从数据库获取所有股票
        2. 使用所有可用的分析器（livermore + multi_indicator）
        3. save_all_dates=True：保存所有历史日期的分析结果
        4. cache_enabled=False：禁用缓存，确保使用最新数据
        """
        try:
            # 获取ExecutionAdapter
            execution_adapter = self.app.get('execution_adapter')
            if not execution_adapter:
                logger.error("ExecutionAdapter未初始化")
                return

            # 调用批量分析API
            # 注意：不传入symbols参数，让Execution服务自动获取所有股票
            analysis_params = {
                'analyzers': ['all'],  # 触发所有可用的分析器（livermore + multi_indicator）
                'config': {
                    'parallel_limit': 10,  # 并发限制
                    'save_all_dates': True,  # 保存所有历史日期的分析结果
                    'cache_enabled': False  # 禁用缓存，确保使用最新数据
                }
            }

            logger.info("触发股票批量分析（全量分析模式，所有分析器）")
            result = await execution_adapter.trigger_batch_analysis(analysis_params)
            logger.info(f"股票批量分析任务已触发: {result}")

        except Exception as e:
            logger.error(f"触发股票批量分析失败: {e}", exc_info=True)
    


