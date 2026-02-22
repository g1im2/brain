"""
分析任务自动触发调度器

当数据抓取任务完成后，自动触发依赖该数据的分析任务
"""

import asyncio
import logging
import os
from typing import Dict, Any, List, Set, Optional
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
        # required: 必需项；optional: 可选项
        self.analysis_dependencies = {
            'macro_analysis': {
                'required': {
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
                'optional': set()
            },
            'stock_batch_analysis': {
                # 仅依赖核心日线数据即可触发
                'required': {
                    'stock_daily_data_fetch'
                },
                # 其他数据完成后不会阻塞触发
                'optional': {
                    'stock_basic_data_fetch',
                    'stock_index_data_fetch',
                    'industry_board_data_fetch',
                    'concept_board_data_fetch'
                }
            }
        }
        
        # 已完成的数据抓取任务（使用Redis存储）
        self.completed_tasks: Set[str] = set()

        # 仅处理分析依赖相关的任务，避免被其他任务（如 system_health_check）误触发
        self._relevant_tasks: Set[str] = set()
        for deps in self.analysis_dependencies.values():
            self._relevant_tasks.update(deps.get('required', set()))
            self._relevant_tasks.update(deps.get('optional', set()))
        
        # 上次检查时间
        self.last_check_time: Dict[str, datetime] = {}

        # 分析任务等待配置
        self.analysis_poll_interval = 15
        self.stock_analysis_timeout = 12 * 3600
        self.macro_analysis_timeout = 6 * 3600
        
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
        if task_name not in self._relevant_tasks:
            logger.debug(f"忽略非分析依赖任务: {task_name}")
            return

        # 添加到已完成任务集合
        self.completed_tasks.add(task_name)

        # 同时存储到Redis（持久化）
        redis = self.app.get('redis')
        if redis:
            try:
                await redis.sadd('completed_data_tasks', task_name)
                # 设置过期时间为7天
                await redis.expire('completed_data_tasks', 7 * 24 * 3600)
            except Exception as e:
                logger.warning(f"持久化完成任务到Redis失败，将继续触发分析: {e}")

        logger.info(f"数据抓取任务已标记为完成: {task_name}")

        # 立即检查是否可以触发分析
        try:
            await self._check_all_analysis_triggers()
        except Exception as e:
            logger.error(f"标记任务完成后触发分析失败 {task_name}: {e}")
    
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
            dependencies = self.analysis_dependencies.get(analysis_name, {})
            required_tasks = set(dependencies.get('required', set()))
            optional_tasks = set(dependencies.get('optional', set()))
            
            if not required_tasks:
                logger.warning(f"未找到分析任务的依赖配置: {analysis_name}")
                return
            
            # 从Redis加载已完成任务（如果本地集合为空）
            if not self.completed_tasks:
                await self._load_completed_tasks_from_redis()
            
            # 检查必需依赖任务是否都已完成
            missing_required = required_tasks - self.completed_tasks
            missing_optional = optional_tasks - self.completed_tasks

            if missing_required:
                logger.info(
                    f"分析任务 {analysis_name} 的依赖未满足，"
                    f"缺少: {', '.join(missing_required)}"
                )
                return
            if missing_optional:
                logger.info(
                    f"分析任务 {analysis_name} 可选依赖未满足，"
                    f"将继续触发。缺少: {', '.join(missing_optional)}"
                )
            
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
            job_id = None
            if isinstance(result, dict):
                job_id = result.get('job_id') or result.get('task_id')
            if job_id:
                job = await macro_adapter.wait_for_job_completion(
                    job_id,
                    timeout=self.macro_analysis_timeout,
                    poll_interval=self.analysis_poll_interval
                )
                self._record_analysis_execution(
                    "macro_analysis",
                    "completed",
                    f"Macro analysis job completed: {job_id}",
                    job_id
                )
                logger.info(f"宏观分析任务完成: {job}")
            else:
                logger.warning(f"宏观分析触发未返回job_id: {result}")

        except Exception as e:
            self._record_analysis_execution("macro_analysis", "failed", str(e))
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

            parallel_limit = int(os.getenv("EXECUTION_ANALYSIS_PARALLEL_LIMIT", "2"))
            parallel_limit = max(1, min(4, parallel_limit))

            # 调用批量分析API
            # 注意：不传入symbols参数，让Execution服务自动获取所有股票
            analysis_params = {
                'analyzers': ['all'],  # 触发所有可用的分析器（livermore + multi_indicator）
                'config': {
                    'parallel_limit': parallel_limit,  # 并发限制
                    'save_all_dates': False,  # 日常增量只保存最新结果
                    'cache_enabled': False  # 禁用缓存，确保使用最新数据
                }
            }

            logger.info("触发股票批量分析（全量分析模式，所有分析器）")
            result = await execution_adapter.trigger_batch_analysis(analysis_params)
            job_id = None
            if isinstance(result, dict):
                job_id = result.get('job_id') or result.get('task_id')
            if job_id:
                job = await execution_adapter.wait_for_job_completion(
                    job_id,
                    timeout=self.stock_analysis_timeout,
                    poll_interval=self.analysis_poll_interval
                )
                # 分析完成后，将 livermore/chanlun/多指标结果按统一入池标准同步到候选池
                sync_enabled = str(os.getenv("EXECUTION_CANDIDATE_SYNC_ENABLED", "true")).strip().lower() not in {"0", "false", "off", "no"}
                sync_job_id = None
                promote_job_id = None
                if sync_enabled:
                    try:
                        sync_params = {
                            "lookback_days": max(1, int(os.getenv("EXECUTION_CANDIDATE_SYNC_LOOKBACK_DAYS", "7"))),
                            "max_candidates": max(1, int(os.getenv("EXECUTION_CANDIDATE_SYNC_MAX_CANDIDATES", "120"))),
                            "analyzers": ["livermore", "chanlun", "multi_indicator"],
                            "entry_rules": {
                                "min_analyzer_hits": max(1, int(os.getenv("EXECUTION_CANDIDATE_SYNC_MIN_HITS", "2"))),
                                "min_weighted_score": float(os.getenv("EXECUTION_CANDIDATE_SYNC_MIN_SCORE", "0.58")),
                                "min_confidence": float(os.getenv("EXECUTION_CANDIDATE_SYNC_MIN_CONFIDENCE", "0.55")),
                                "min_positive_ratio": float(os.getenv("EXECUTION_CANDIDATE_SYNC_MIN_POSITIVE_RATIO", "0.5")),
                                "signal_whitelist": ["STRONG_BUY", "BUY", "ADD", "UNCERTAIN"],
                                "weights": {
                                    "livermore": 0.40,
                                    "chanlun": 0.35,
                                    "multi_indicator": 0.25,
                                },
                            },
                        }
                        sync_resp = await execution_adapter.sync_candidates_from_analysis(sync_params)
                        sync_job_id = sync_resp.get("job_id") or sync_resp.get("task_id")
                        if sync_job_id:
                            await execution_adapter.wait_for_job_completion(
                                str(sync_job_id),
                                timeout=3600,
                                poll_interval=self.analysis_poll_interval,
                            )
                            logger.info(f"候选池同步任务完成: {sync_job_id}")
                        else:
                            logger.warning(f"候选池同步触发未返回job_id: {sync_resp}")

                        # 候选池同步完成后，自动推进到标的研究队列，避免 Page5 空队列。
                        promote_enabled = str(
                            os.getenv("EXECUTION_RESEARCH_AUTO_PROMOTE_ENABLED", "true")
                        ).strip().lower() not in {"0", "false", "off", "no"}
                        if promote_enabled:
                            promote_params = {
                                "limit": max(1, int(os.getenv("EXECUTION_RESEARCH_AUTO_PROMOTE_LIMIT", "30"))),
                                "lookback_days": max(1, int(os.getenv("EXECUTION_RESEARCH_AUTO_PROMOTE_LOOKBACK_DAYS", "30"))),
                                "title_prefix": os.getenv("EXECUTION_RESEARCH_AUTO_PROMOTE_TITLE_PREFIX", "候选池推送"),
                            }
                            promote_resp = await execution_adapter.submit_ui_job(
                                "ui_candidates_auto_promote",
                                params=promote_params,
                            )
                            promote_job_id = promote_resp.get("job_id") or promote_resp.get("task_id")
                            if promote_job_id:
                                await execution_adapter.wait_for_job_completion(
                                    str(promote_job_id),
                                    timeout=1800,
                                    poll_interval=self.analysis_poll_interval,
                                )
                                logger.info(f"标的研究自动晋级完成: {promote_job_id}")
                            else:
                                logger.warning(f"标的研究自动晋级触发未返回job_id: {promote_resp}")
                    except Exception as sync_exc:
                        logger.error(f"候选池同步失败（不影响当次批量分析完成状态）: {sync_exc}", exc_info=True)

                self._record_analysis_execution(
                    "stock_batch_analysis",
                    "completed",
                    f"Stock batch analysis completed: {job_id}; "
                    f"candidate_sync_job={sync_job_id or 'n/a'}; "
                    f"research_promote_job={promote_job_id or 'n/a'}",
                    job_id
                )
                logger.info(f"股票批量分析任务完成: {job}")
            else:
                logger.warning(f"股票批量分析触发未返回job_id: {result}")

        except Exception as e:
            self._record_analysis_execution("stock_batch_analysis", "failed", str(e))
            logger.error(f"触发股票批量分析失败: {e}", exc_info=True)

    def _record_analysis_execution(self, task_name: str, status: str, message: str, job_id: Optional[str] = None) -> None:
        """记录分析任务执行结果到调度器历史"""
        scheduler = self.app.get('scheduler')
        if scheduler and hasattr(scheduler, 'record_task_execution'):
            scheduler.record_task_execution(task_name, status, message, job_id)
    
