"""
系统协调器实现

负责统一协调三层金融交易系统的分析周期、资源管理和异常处理，
是整个集成层的核心控制组件。
"""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set
from concurrent.futures import ThreadPoolExecutor
import threading

from ..interfaces import ISystemCoordinator
from ..models import (
    AnalysisCycleResult, SystemStatus, MarketEvent, ResourceAllocation,
    ConflictResolution, SystemHealthStatus, EventType
)
from ..config import IntegrationConfig
from ..exceptions import (
    SystemCoordinatorException, CycleExecutionException, 
    ResourceAllocationException, handle_exception
)

logger = logging.getLogger(__name__)


class SystemCoordinator(ISystemCoordinator):
    """系统协调器实现类
    
    统一协调三层系统的分析周期、资源分配和异常处理，
    确保系统各组件协调运行和高效执行。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化系统协调器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False
        self._active_cycles: Dict[str, AnalysisCycleResult] = {}
        self._resource_allocation: Optional[ResourceAllocation] = None
        self._system_status = SystemStatus(
            overall_health=SystemHealthStatus.HEALTHY,
            macro_system_status=SystemHealthStatus.HEALTHY,
            portfolio_system_status=SystemHealthStatus.HEALTHY,
            tactical_system_status=SystemHealthStatus.HEALTHY,
            data_pipeline_status=SystemHealthStatus.HEALTHY,
            last_update_time=datetime.now()
        )
        
        # 线程池用于并发执行
        self._executor = ThreadPoolExecutor(
            max_workers=self.config.system_coordinator.max_concurrent_cycles
        )
        
        # 事件队列
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._event_handlers: Dict[EventType, List[callable]] = {}
        
        # 锁和同步
        self._cycle_lock = threading.RLock()
        self._resource_lock = threading.RLock()
        
        # 监控任务
        self._monitoring_task: Optional[asyncio.Task] = None
        
        logger.info("SystemCoordinator initialized")
    
    async def start(self) -> bool:
        """启动系统协调器
        
        Returns:
            bool: 启动是否成功
        """
        try:
            if self._is_running:
                logger.warning("SystemCoordinator is already running")
                return True
            
            # 启动系统
            startup_success = await self.coordinate_system_startup()
            if not startup_success:
                raise SystemCoordinatorException("Failed to start system")
            
            # 分配初始资源
            await self.allocate_system_resources()
            
            # 启动监控任务
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            
            # 启动事件处理任务
            asyncio.create_task(self._event_processing_loop())
            
            self._is_running = True
            logger.info("SystemCoordinator started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start SystemCoordinator: {e}")
            await self.stop()
            return False
    
    async def stop(self) -> bool:
        """停止系统协调器
        
        Returns:
            bool: 停止是否成功
        """
        try:
            if not self._is_running:
                return True
            
            logger.info("Stopping SystemCoordinator...")
            
            # 停止接受新的分析周期
            self._is_running = False
            
            # 等待活跃周期完成
            await self._wait_for_active_cycles()
            
            # 停止监控任务
            if self._monitoring_task:
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass
            
            # 关闭线程池
            self._executor.shutdown(wait=True)
            
            # 协调系统关闭
            await self.coordinate_system_shutdown()
            
            logger.info("SystemCoordinator stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping SystemCoordinator: {e}")
            return False
    
    async def coordinate_full_analysis_cycle(self) -> AnalysisCycleResult:
        """协调完整分析周期
        
        Returns:
            AnalysisCycleResult: 分析周期执行结果
        """
        if not self._is_running:
            raise SystemCoordinatorException("SystemCoordinator is not running")
        
        # 检查并发限制
        with self._cycle_lock:
            if len(self._active_cycles) >= self.config.system_coordinator.max_concurrent_cycles:
                raise SystemCoordinatorException("Maximum concurrent cycles limit reached")
        
        cycle_id = str(uuid.uuid4())
        cycle_result = AnalysisCycleResult(
            cycle_id=cycle_id,
            start_time=datetime.now(),
            status="running"
        )
        
        try:
            # 注册活跃周期
            with self._cycle_lock:
                self._active_cycles[cycle_id] = cycle_result
            
            logger.info(f"Starting analysis cycle: {cycle_id}")
            
            # 阶段1: 宏观分析
            macro_state = await self._execute_macro_analysis(cycle_id)
            cycle_result.macro_state = macro_state
            
            # 阶段2: 组合管理
            portfolio_instruction = await self._execute_portfolio_management(cycle_id, macro_state)
            cycle_result.portfolio_instruction = portfolio_instruction
            
            # 阶段3: 战术分析
            trading_signals = await self._execute_tactical_analysis(cycle_id, portfolio_instruction)
            cycle_result.trading_signals = trading_signals
            
            # 阶段4: 验证和执行
            execution_result = await self._execute_validation_and_execution(cycle_id, trading_signals)
            cycle_result.execution_result = execution_result
            
            # 完成周期
            cycle_result.end_time = datetime.now()
            cycle_result.status = "completed"
            
            logger.info(f"Analysis cycle completed: {cycle_id}, duration: {cycle_result.duration:.2f}s")
            return cycle_result
            
        except Exception as e:
            cycle_result.end_time = datetime.now()
            cycle_result.status = "failed"
            cycle_result.error_messages.append(str(e))
            
            logger.error(f"Analysis cycle failed: {cycle_id}, error: {e}")
            raise CycleExecutionException(cycle_id, "unknown", str(e))
            
        finally:
            # 清理活跃周期
            with self._cycle_lock:
                self._active_cycles.pop(cycle_id, None)
    
    async def coordinate_real_time_updates(self, events: List[MarketEvent]) -> None:
        """协调实时更新处理
        
        Args:
            events: 市场事件列表
        """
        try:
            # 按优先级排序事件
            sorted_events = sorted(events, key=lambda e: e.priority, reverse=True)
            
            for event in sorted_events:
                await self._event_queue.put(event)
                logger.debug(f"Queued event: {event.event_id}, type: {event.event_type}")
            
        except Exception as e:
            logger.error(f"Failed to coordinate real-time updates: {e}")
            raise SystemCoordinatorException(f"Real-time update coordination failed: {e}")
    
    async def coordinate_system_startup(self) -> bool:
        """协调系统启动流程
        
        Returns:
            bool: 启动是否成功
        """
        try:
            logger.info("Coordinating system startup...")
            
            # 检查各层系统健康状态
            health_checks = await asyncio.gather(
                self._check_macro_system_health(),
                self._check_portfolio_system_health(),
                self._check_tactical_system_health(),
                self._check_data_pipeline_health(),
                return_exceptions=True
            )
            
            # 更新系统状态
            self._system_status.macro_system_status = health_checks[0] if not isinstance(health_checks[0], Exception) else SystemHealthStatus.CRITICAL
            self._system_status.portfolio_system_status = health_checks[1] if not isinstance(health_checks[1], Exception) else SystemHealthStatus.CRITICAL
            self._system_status.tactical_system_status = health_checks[2] if not isinstance(health_checks[2], Exception) else SystemHealthStatus.CRITICAL
            self._system_status.data_pipeline_status = health_checks[3] if not isinstance(health_checks[3], Exception) else SystemHealthStatus.CRITICAL
            
            # 计算整体健康状态
            self._update_overall_health()
            
            # 检查是否可以启动
            if self._system_status.overall_health == SystemHealthStatus.CRITICAL:
                logger.error("System startup failed: critical health issues detected")
                return False
            
            logger.info("System startup coordination completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"System startup coordination failed: {e}")
            return False
    
    async def coordinate_system_shutdown(self) -> bool:
        """协调系统关闭流程
        
        Returns:
            bool: 关闭是否成功
        """
        try:
            logger.info("Coordinating system shutdown...")
            
            # 等待活跃周期完成
            await self._wait_for_active_cycles()
            
            # 通知各层系统准备关闭
            await asyncio.gather(
                self._notify_macro_system_shutdown(),
                self._notify_portfolio_system_shutdown(),
                self._notify_tactical_system_shutdown(),
                return_exceptions=True
            )
            
            logger.info("System shutdown coordination completed")
            return True
            
        except Exception as e:
            logger.error(f"System shutdown coordination failed: {e}")
            return False
    
    async def allocate_system_resources(self) -> ResourceAllocation:
        """分配系统资源
        
        Returns:
            ResourceAllocation: 资源分配结果
        """
        try:
            with self._resource_lock:
                allocation_id = str(uuid.uuid4())
                
                # 基于配置策略分配资源
                strategy = self.config.system_coordinator.resource_allocation_strategy
                
                if strategy == "balanced":
                    allocation = self._create_balanced_allocation(allocation_id)
                elif strategy == "performance":
                    allocation = self._create_performance_allocation(allocation_id)
                elif strategy == "conservative":
                    allocation = self._create_conservative_allocation(allocation_id)
                else:
                    allocation = self._create_balanced_allocation(allocation_id)
                
                self._resource_allocation = allocation
                logger.info(f"Resource allocation completed: {allocation_id}, strategy: {strategy}")
                return allocation
                
        except Exception as e:
            logger.error(f"Resource allocation failed: {e}")
            raise ResourceAllocationException("system", 0, 0, {"error": str(e)})
    
    async def handle_system_exception(self, exception: Exception) -> ConflictResolution:
        """处理系统异常
        
        Args:
            exception: 系统异常
            
        Returns:
            ConflictResolution: 异常处理结果
        """
        try:
            integration_exception = handle_exception(exception, "SystemCoordinator")
            
            resolution_id = str(uuid.uuid4())
            resolution = ConflictResolution(
                resolution_id=resolution_id,
                conflict_type="system_exception",
                conflict_description=str(exception),
                resolution_strategy="auto_recovery",
                resolution_result="pending",
                affected_components=[integration_exception.component]
            )
            
            # 尝试自动恢复
            if self.config.system_coordinator.enable_auto_recovery:
                recovery_success = await self._attempt_auto_recovery(integration_exception)
                resolution.resolution_result = "success" if recovery_success else "failed"
            else:
                resolution.resolution_result = "manual_intervention_required"
            
            logger.info(f"Exception handled: {resolution_id}, result: {resolution.resolution_result}")
            return resolution
            
        except Exception as e:
            logger.error(f"Exception handling failed: {e}")
            raise SystemCoordinatorException(f"Failed to handle exception: {e}")
    
    def get_system_status(self) -> SystemStatus:
        """获取系统状态
        
        Returns:
            SystemStatus: 当前系统状态
        """
        return self._system_status
    
    def get_active_cycles(self) -> Dict[str, AnalysisCycleResult]:
        """获取活跃分析周期
        
        Returns:
            Dict[str, AnalysisCycleResult]: 活跃周期字典
        """
        with self._cycle_lock:
            return self._active_cycles.copy()
    
    # 私有方法实现
    async def _execute_macro_analysis(self, cycle_id: str) -> Any:
        """执行宏观分析阶段"""
        try:
            # 这里应该调用宏观适配器
            # 暂时返回模拟结果
            await asyncio.sleep(0.1)  # 模拟处理时间
            return {"cycle_phase": "expansion", "risk_level": 0.3}
        except Exception as e:
            raise CycleExecutionException(cycle_id, "macro_analysis", str(e))
    
    async def _execute_portfolio_management(self, cycle_id: str, macro_state: Any) -> Any:
        """执行组合管理阶段"""
        try:
            # 这里应该调用组合适配器
            await asyncio.sleep(0.1)  # 模拟处理时间
            return {"target_position": 0.8, "sector_weights": {"tech": 0.4, "finance": 0.3}}
        except Exception as e:
            raise CycleExecutionException(cycle_id, "portfolio_management", str(e))
    
    async def _execute_tactical_analysis(self, cycle_id: str, portfolio_instruction: Any) -> List[Any]:
        """执行战术分析阶段"""
        try:
            # 这里应该调用战术适配器
            await asyncio.sleep(0.1)  # 模拟处理时间
            return [{"symbol": "000001.SZ", "signal": "buy", "strength": 0.8}]
        except Exception as e:
            raise CycleExecutionException(cycle_id, "tactical_analysis", str(e))
    
    async def _execute_validation_and_execution(self, cycle_id: str, trading_signals: List[Any]) -> Any:
        """执行验证和执行阶段"""
        try:
            # 这里应该调用验证协调器和执行引擎
            await asyncio.sleep(0.1)  # 模拟处理时间
            return {"executed_orders": len(trading_signals), "success_rate": 0.95}
        except Exception as e:
            raise CycleExecutionException(cycle_id, "validation_execution", str(e))

    async def _wait_for_active_cycles(self) -> None:
        """等待活跃周期完成"""
        timeout = self.config.system_coordinator.cycle_timeout
        start_time = datetime.now()

        while self._active_cycles:
            if (datetime.now() - start_time).total_seconds() > timeout:
                logger.warning(f"Timeout waiting for active cycles: {list(self._active_cycles.keys())}")
                break
            await asyncio.sleep(1)

    async def _monitoring_loop(self) -> None:
        """监控循环"""
        interval = self.config.system_coordinator.health_check_interval

        while self._is_running:
            try:
                await self._update_system_health()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(interval)

    async def _event_processing_loop(self) -> None:
        """事件处理循环"""
        while self._is_running:
            try:
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)
                await self._process_event(event)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Event processing error: {e}")

    async def _process_event(self, event: MarketEvent) -> None:
        """处理单个事件"""
        try:
            handlers = self._event_handlers.get(event.event_type, [])
            if handlers:
                await asyncio.gather(*[handler(event) for handler in handlers])
            else:
                logger.debug(f"No handlers for event type: {event.event_type}")
        except Exception as e:
            logger.error(f"Error processing event {event.event_id}: {e}")

    async def _update_system_health(self) -> None:
        """更新系统健康状态"""
        try:
            # 检查各层系统健康状态
            health_checks = await asyncio.gather(
                self._check_macro_system_health(),
                self._check_portfolio_system_health(),
                self._check_tactical_system_health(),
                self._check_data_pipeline_health(),
                return_exceptions=True
            )

            # 更新状态
            self._system_status.macro_system_status = health_checks[0] if not isinstance(health_checks[0], Exception) else SystemHealthStatus.CRITICAL
            self._system_status.portfolio_system_status = health_checks[1] if not isinstance(health_checks[1], Exception) else SystemHealthStatus.CRITICAL
            self._system_status.tactical_system_status = health_checks[2] if not isinstance(health_checks[2], Exception) else SystemHealthStatus.CRITICAL
            self._system_status.data_pipeline_status = health_checks[3] if not isinstance(health_checks[3], Exception) else SystemHealthStatus.CRITICAL

            self._update_overall_health()
            self._system_status.last_update_time = datetime.now()
            self._system_status.active_sessions = len(self._active_cycles)

        except Exception as e:
            logger.error(f"Failed to update system health: {e}")

    def _update_overall_health(self) -> None:
        """更新整体健康状态"""
        statuses = [
            self._system_status.macro_system_status,
            self._system_status.portfolio_system_status,
            self._system_status.tactical_system_status,
            self._system_status.data_pipeline_status
        ]

        if any(status == SystemHealthStatus.CRITICAL for status in statuses):
            self._system_status.overall_health = SystemHealthStatus.CRITICAL
        elif any(status == SystemHealthStatus.WARNING for status in statuses):
            self._system_status.overall_health = SystemHealthStatus.WARNING
        else:
            self._system_status.overall_health = SystemHealthStatus.HEALTHY

    async def _check_macro_system_health(self) -> SystemHealthStatus:
        """检查宏观系统健康状态"""
        try:
            # 这里应该调用宏观适配器的健康检查
            await asyncio.sleep(0.01)  # 模拟检查时间
            return SystemHealthStatus.HEALTHY
        except Exception:
            return SystemHealthStatus.CRITICAL

    async def _check_portfolio_system_health(self) -> SystemHealthStatus:
        """检查组合系统健康状态"""
        try:
            # 这里应该调用组合适配器的健康检查
            await asyncio.sleep(0.01)  # 模拟检查时间
            return SystemHealthStatus.HEALTHY
        except Exception:
            return SystemHealthStatus.CRITICAL

    async def _check_tactical_system_health(self) -> SystemHealthStatus:
        """检查战术系统健康状态"""
        try:
            # 这里应该调用战术适配器的健康检查
            await asyncio.sleep(0.01)  # 模拟检查时间
            return SystemHealthStatus.HEALTHY
        except Exception:
            return SystemHealthStatus.CRITICAL

    async def _check_data_pipeline_health(self) -> SystemHealthStatus:
        """检查数据管道健康状态"""
        try:
            # 这里应该调用数据流管理器的健康检查
            await asyncio.sleep(0.01)  # 模拟检查时间
            return SystemHealthStatus.HEALTHY
        except Exception:
            return SystemHealthStatus.CRITICAL

    async def _notify_macro_system_shutdown(self) -> None:
        """通知宏观系统准备关闭"""
        try:
            # 这里应该通知宏观适配器
            await asyncio.sleep(0.01)
        except Exception as e:
            logger.error(f"Failed to notify macro system shutdown: {e}")

    async def _notify_portfolio_system_shutdown(self) -> None:
        """通知组合系统准备关闭"""
        try:
            # 这里应该通知组合适配器
            await asyncio.sleep(0.01)
        except Exception as e:
            logger.error(f"Failed to notify portfolio system shutdown: {e}")

    async def _notify_tactical_system_shutdown(self) -> None:
        """通知战术系统准备关闭"""
        try:
            # 这里应该通知战术适配器
            await asyncio.sleep(0.01)
        except Exception as e:
            logger.error(f"Failed to notify tactical system shutdown: {e}")

    def _create_balanced_allocation(self, allocation_id: str) -> ResourceAllocation:
        """创建平衡资源分配"""
        return ResourceAllocation(
            allocation_id=allocation_id,
            cpu_allocation={"macro": 0.3, "portfolio": 0.4, "tactical": 0.3},
            memory_allocation={"macro": 0.2, "portfolio": 0.3, "tactical": 0.5},
            network_allocation={"macro": 0.3, "portfolio": 0.3, "tactical": 0.4},
            storage_allocation={"macro": 0.2, "portfolio": 0.3, "tactical": 0.5},
            priority_weights={"macro": 0.3, "portfolio": 0.4, "tactical": 0.3}
        )

    def _create_performance_allocation(self, allocation_id: str) -> ResourceAllocation:
        """创建性能优先资源分配"""
        return ResourceAllocation(
            allocation_id=allocation_id,
            cpu_allocation={"macro": 0.2, "portfolio": 0.3, "tactical": 0.5},
            memory_allocation={"macro": 0.15, "portfolio": 0.25, "tactical": 0.6},
            network_allocation={"macro": 0.2, "portfolio": 0.3, "tactical": 0.5},
            storage_allocation={"macro": 0.15, "portfolio": 0.25, "tactical": 0.6},
            priority_weights={"macro": 0.2, "portfolio": 0.3, "tactical": 0.5}
        )

    def _create_conservative_allocation(self, allocation_id: str) -> ResourceAllocation:
        """创建保守资源分配"""
        return ResourceAllocation(
            allocation_id=allocation_id,
            cpu_allocation={"macro": 0.4, "portfolio": 0.4, "tactical": 0.2},
            memory_allocation={"macro": 0.3, "portfolio": 0.4, "tactical": 0.3},
            network_allocation={"macro": 0.4, "portfolio": 0.4, "tactical": 0.2},
            storage_allocation={"macro": 0.3, "portfolio": 0.4, "tactical": 0.3},
            priority_weights={"macro": 0.4, "portfolio": 0.4, "tactical": 0.2}
        )

    async def _attempt_auto_recovery(self, exception) -> bool:
        """尝试自动恢复"""
        try:
            # 根据异常类型选择恢复策略
            # 这里实现基本的重试逻辑
            await asyncio.sleep(self.config.system_coordinator.retry_delay)
            return True
        except Exception as e:
            logger.error(f"Auto recovery failed: {e}")
            return False
