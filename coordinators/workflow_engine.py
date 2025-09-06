"""
工作流引擎实现

负责工作流定义、执行、状态管理和监控，
支持复杂业务流程的编排和控制。
"""

import asyncio
import logging
import uuid
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Set
from enum import Enum
from dataclasses import dataclass, field
import threading

from ..config import IntegrationConfig
from ..exceptions import IntegrationException

logger = logging.getLogger(__name__)


class WorkflowState(Enum):
    """工作流状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskState(Enum):
    """任务状态枚举"""
    WAITING = "waiting"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class WorkflowTask:
    """工作流任务定义"""
    task_id: str
    name: str
    task_type: str
    handler: Optional[Callable] = None
    dependencies: Set[str] = field(default_factory=set)
    parameters: Dict[str, Any] = field(default_factory=dict)
    timeout: int = 300  # 秒
    retry_count: int = 3
    retry_delay: float = 1.0
    state: TaskState = TaskState.WAITING
    result: Any = None
    error: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def duration(self) -> Optional[float]:
        """计算任务执行时长"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


@dataclass
class WorkflowDefinition:
    """工作流定义"""
    workflow_id: str
    name: str
    description: str
    tasks: Dict[str, WorkflowTask] = field(default_factory=dict)
    global_timeout: int = 1800  # 秒
    max_parallel_tasks: int = 5
    failure_strategy: str = "stop"  # stop, continue, retry
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_task(self, task: WorkflowTask) -> None:
        """添加任务到工作流"""
        self.tasks[task.task_id] = task
    
    def get_ready_tasks(self, completed_tasks: Set[str]) -> List[WorkflowTask]:
        """获取准备执行的任务"""
        ready_tasks = []
        for task in self.tasks.values():
            if (task.state == TaskState.WAITING and 
                task.dependencies.issubset(completed_tasks)):
                ready_tasks.append(task)
        return ready_tasks


@dataclass
class WorkflowExecution:
    """工作流执行实例"""
    execution_id: str
    workflow_id: str
    state: WorkflowState = WorkflowState.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    completed_tasks: Set[str] = field(default_factory=set)
    failed_tasks: Set[str] = field(default_factory=set)
    running_tasks: Set[str] = field(default_factory=set)
    context: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    
    @property
    def duration(self) -> Optional[float]:
        """计算工作流执行时长"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


class WorkflowEngine:
    """工作流引擎实现类
    
    负责工作流的定义、执行、状态管理和监控，
    支持复杂业务流程的编排和控制。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化工作流引擎
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False
        
        # 工作流定义存储
        self._workflow_definitions: Dict[str, WorkflowDefinition] = {}
        
        # 工作流执行实例
        self._active_executions: Dict[str, WorkflowExecution] = {}
        self._execution_history: List[WorkflowExecution] = []
        
        # 任务处理器注册表
        self._task_handlers: Dict[str, Callable] = {}
        
        # 执行统计
        self._execution_statistics = {
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'average_execution_time': 0.0,
            'total_tasks_executed': 0
        }
        
        # 锁和同步
        self._execution_lock = threading.RLock()
        
        # 监控任务
        self._monitoring_task: Optional[asyncio.Task] = None
        
        # 注册内置任务处理器
        self._register_builtin_handlers()
        
        logger.info("WorkflowEngine initialized")
    
    async def start(self) -> bool:
        """启动工作流引擎
        
        Returns:
            bool: 启动是否成功
        """
        try:
            if self._is_running:
                logger.warning("WorkflowEngine is already running")
                return True
            
            # 启动监控任务
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            
            self._is_running = True
            logger.info("WorkflowEngine started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start WorkflowEngine: {e}")
            return False
    
    async def stop(self) -> bool:
        """停止工作流引擎
        
        Returns:
            bool: 停止是否成功
        """
        try:
            if not self._is_running:
                return True
            
            logger.info("Stopping WorkflowEngine...")
            self._is_running = False
            
            # 等待活跃执行完成
            await self._wait_for_active_executions()
            
            # 停止监控任务
            if self._monitoring_task:
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass
            
            logger.info("WorkflowEngine stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping WorkflowEngine: {e}")
            return False
    
    def register_workflow(self, workflow: WorkflowDefinition) -> bool:
        """注册工作流定义
        
        Args:
            workflow: 工作流定义
            
        Returns:
            bool: 注册是否成功
        """
        try:
            # 验证工作流定义
            if not self._validate_workflow_definition(workflow):
                return False
            
            self._workflow_definitions[workflow.workflow_id] = workflow
            logger.info(f"Workflow registered: {workflow.workflow_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register workflow {workflow.workflow_id}: {e}")
            return False
    
    def register_task_handler(self, task_type: str, handler: Callable) -> bool:
        """注册任务处理器
        
        Args:
            task_type: 任务类型
            handler: 处理器函数
            
        Returns:
            bool: 注册是否成功
        """
        try:
            self._task_handlers[task_type] = handler
            logger.info(f"Task handler registered: {task_type}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register task handler {task_type}: {e}")
            return False
    
    async def execute_workflow(self, workflow_id: str, 
                             context: Optional[Dict[str, Any]] = None) -> str:
        """执行工作流
        
        Args:
            workflow_id: 工作流ID
            context: 执行上下文
            
        Returns:
            str: 执行ID
        """
        if not self._is_running:
            raise IntegrationException("WorkflowEngine is not running", "WORKFLOW_ENGINE_NOT_RUNNING")
        
        if workflow_id not in self._workflow_definitions:
            raise IntegrationException(f"Workflow not found: {workflow_id}", "WORKFLOW_NOT_FOUND")
        
        execution_id = str(uuid.uuid4())
        execution = WorkflowExecution(
            execution_id=execution_id,
            workflow_id=workflow_id,
            context=context or {},
            start_time=datetime.now()
        )
        
        try:
            with self._execution_lock:
                self._active_executions[execution_id] = execution
                self._execution_statistics['total_executions'] += 1
            
            logger.info(f"Starting workflow execution: {execution_id}")
            
            # 异步执行工作流
            asyncio.create_task(self._execute_workflow_async(execution))
            
            return execution_id
            
        except Exception as e:
            logger.error(f"Failed to start workflow execution: {e}")
            raise IntegrationException(f"Workflow execution failed: {e}", "WORKFLOW_EXECUTION_FAILED")
    
    async def pause_workflow(self, execution_id: str) -> bool:
        """暂停工作流执行
        
        Args:
            execution_id: 执行ID
            
        Returns:
            bool: 暂停是否成功
        """
        try:
            with self._execution_lock:
                if execution_id in self._active_executions:
                    execution = self._active_executions[execution_id]
                    if execution.state == WorkflowState.RUNNING:
                        execution.state = WorkflowState.PAUSED
                        logger.info(f"Workflow paused: {execution_id}")
                        return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to pause workflow {execution_id}: {e}")
            return False
    
    async def resume_workflow(self, execution_id: str) -> bool:
        """恢复工作流执行
        
        Args:
            execution_id: 执行ID
            
        Returns:
            bool: 恢复是否成功
        """
        try:
            with self._execution_lock:
                if execution_id in self._active_executions:
                    execution = self._active_executions[execution_id]
                    if execution.state == WorkflowState.PAUSED:
                        execution.state = WorkflowState.RUNNING
                        logger.info(f"Workflow resumed: {execution_id}")
                        return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to resume workflow {execution_id}: {e}")
            return False
    
    async def cancel_workflow(self, execution_id: str) -> bool:
        """取消工作流执行
        
        Args:
            execution_id: 执行ID
            
        Returns:
            bool: 取消是否成功
        """
        try:
            with self._execution_lock:
                if execution_id in self._active_executions:
                    execution = self._active_executions[execution_id]
                    execution.state = WorkflowState.CANCELLED
                    execution.end_time = datetime.now()
                    logger.info(f"Workflow cancelled: {execution_id}")
                    return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to cancel workflow {execution_id}: {e}")
            return False
    
    def get_execution_status(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """获取工作流执行状态
        
        Args:
            execution_id: 执行ID
            
        Returns:
            Optional[Dict[str, Any]]: 执行状态信息
        """
        with self._execution_lock:
            if execution_id in self._active_executions:
                execution = self._active_executions[execution_id]
                workflow = self._workflow_definitions[execution.workflow_id]
                
                return {
                    'execution_id': execution_id,
                    'workflow_id': execution.workflow_id,
                    'workflow_name': workflow.name,
                    'state': execution.state.value,
                    'start_time': execution.start_time.isoformat() if execution.start_time else None,
                    'end_time': execution.end_time.isoformat() if execution.end_time else None,
                    'duration': execution.duration,
                    'completed_tasks': len(execution.completed_tasks),
                    'failed_tasks': len(execution.failed_tasks),
                    'running_tasks': len(execution.running_tasks),
                    'total_tasks': len(workflow.tasks),
                    'progress': len(execution.completed_tasks) / len(workflow.tasks) if workflow.tasks else 0,
                    'error_message': execution.error_message
                }
        return None
    
    def get_workflow_statistics(self) -> Dict[str, Any]:
        """获取工作流统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        with self._execution_lock:
            stats = self._execution_statistics.copy()
            stats.update({
                'registered_workflows': len(self._workflow_definitions),
                'registered_handlers': len(self._task_handlers),
                'active_executions': len(self._active_executions),
                'execution_history_size': len(self._execution_history)
            })
            
            # 计算成功率
            total = stats['total_executions']
            if total > 0:
                stats['success_rate'] = stats['successful_executions'] / total
            else:
                stats['success_rate'] = 0.0
            
            return stats

    # 私有方法实现
    def _validate_workflow_definition(self, workflow: WorkflowDefinition) -> bool:
        """验证工作流定义"""
        try:
            # 检查基本字段
            if not workflow.workflow_id or not workflow.name:
                logger.error("Workflow ID and name are required")
                return False

            # 检查任务依赖关系
            task_ids = set(workflow.tasks.keys())
            for task in workflow.tasks.values():
                if not task.dependencies.issubset(task_ids):
                    logger.error(f"Invalid dependencies for task {task.task_id}")
                    return False

            # 检查循环依赖
            if self._has_circular_dependencies(workflow):
                logger.error("Circular dependencies detected")
                return False

            return True

        except Exception as e:
            logger.error(f"Workflow validation failed: {e}")
            return False

    def _has_circular_dependencies(self, workflow: WorkflowDefinition) -> bool:
        """检查循环依赖"""
        def visit(task_id: str, visited: Set[str], path: Set[str]) -> bool:
            if task_id in path:
                return True
            if task_id in visited:
                return False

            visited.add(task_id)
            path.add(task_id)

            task = workflow.tasks.get(task_id)
            if task:
                for dep in task.dependencies:
                    if visit(dep, visited, path):
                        return True

            path.remove(task_id)
            return False

        visited = set()
        for task_id in workflow.tasks:
            if task_id not in visited:
                if visit(task_id, visited, set()):
                    return True
        return False

    async def _execute_workflow_async(self, execution: WorkflowExecution) -> None:
        """异步执行工作流"""
        try:
            execution.state = WorkflowState.RUNNING
            workflow = self._workflow_definitions[execution.workflow_id]

            logger.info(f"Executing workflow: {execution.workflow_id}")

            # 执行任务
            while True:
                # 检查是否暂停或取消
                if execution.state in [WorkflowState.PAUSED, WorkflowState.CANCELLED]:
                    break

                # 获取准备执行的任务
                ready_tasks = workflow.get_ready_tasks(execution.completed_tasks)

                if not ready_tasks:
                    # 检查是否还有运行中的任务
                    if not execution.running_tasks:
                        # 所有任务完成
                        break
                    else:
                        # 等待运行中的任务完成
                        await asyncio.sleep(0.1)
                        continue

                # 限制并行任务数量
                available_slots = workflow.max_parallel_tasks - len(execution.running_tasks)
                tasks_to_run = ready_tasks[:available_slots]

                # 启动任务
                for task in tasks_to_run:
                    task.state = TaskState.RUNNING
                    execution.running_tasks.add(task.task_id)
                    asyncio.create_task(self._execute_task_async(execution, task))

                await asyncio.sleep(0.1)

            # 完成工作流
            await self._complete_workflow_execution(execution)

        except Exception as e:
            logger.error(f"Workflow execution failed: {execution.execution_id}, error: {e}")
            execution.state = WorkflowState.FAILED
            execution.error_message = str(e)
            execution.end_time = datetime.now()
            self._update_execution_statistics(False, execution.duration or 0)
        finally:
            # 移动到历史记录
            with self._execution_lock:
                if execution.execution_id in self._active_executions:
                    del self._active_executions[execution.execution_id]
                self._execution_history.append(execution)

    async def _execute_task_async(self, execution: WorkflowExecution, task: WorkflowTask) -> None:
        """异步执行任务"""
        try:
            task.start_time = datetime.now()
            logger.debug(f"Executing task: {task.task_id}")

            # 获取任务处理器
            handler = self._task_handlers.get(task.task_type)
            if not handler:
                raise IntegrationException(f"No handler for task type: {task.task_type}")

            # 准备任务参数
            task_params = task.parameters.copy()
            task_params['context'] = execution.context
            task_params['execution_id'] = execution.execution_id

            # 执行任务（带重试）
            retry_count = 0
            while retry_count <= task.retry_count:
                try:
                    # 执行任务处理器
                    if asyncio.iscoroutinefunction(handler):
                        result = await asyncio.wait_for(
                            handler(**task_params),
                            timeout=task.timeout
                        )
                    else:
                        result = await asyncio.wait_for(
                            asyncio.get_event_loop().run_in_executor(None, lambda: handler(**task_params)),
                            timeout=task.timeout
                        )

                    # 任务成功完成
                    task.result = result
                    task.state = TaskState.COMPLETED
                    task.end_time = datetime.now()

                    # 更新执行状态
                    with self._execution_lock:
                        execution.running_tasks.discard(task.task_id)
                        execution.completed_tasks.add(task.task_id)
                        self._execution_statistics['total_tasks_executed'] += 1

                    logger.debug(f"Task completed: {task.task_id}")
                    break

                except asyncio.TimeoutError:
                    retry_count += 1
                    if retry_count > task.retry_count:
                        raise IntegrationException(f"Task timeout: {task.task_id}")
                    logger.warning(f"Task timeout, retrying: {task.task_id} ({retry_count}/{task.retry_count})")
                    await asyncio.sleep(task.retry_delay * retry_count)

                except Exception as e:
                    retry_count += 1
                    if retry_count > task.retry_count:
                        raise e
                    logger.warning(f"Task failed, retrying: {task.task_id} ({retry_count}/{task.retry_count}): {e}")
                    await asyncio.sleep(task.retry_delay * retry_count)

        except Exception as e:
            # 任务失败
            task.state = TaskState.FAILED
            task.error = str(e)
            task.end_time = datetime.now()

            with self._execution_lock:
                execution.running_tasks.discard(task.task_id)
                execution.failed_tasks.add(task.task_id)

            logger.error(f"Task failed: {task.task_id}, error: {e}")

            # 根据失败策略处理
            workflow = self._workflow_definitions[execution.workflow_id]
            if workflow.failure_strategy == "stop":
                execution.state = WorkflowState.FAILED
                execution.error_message = f"Task {task.task_id} failed: {e}"

    async def _complete_workflow_execution(self, execution: WorkflowExecution) -> None:
        """完成工作流执行"""
        execution.end_time = datetime.now()

        if execution.failed_tasks:
            execution.state = WorkflowState.FAILED
            self._update_execution_statistics(False, execution.duration or 0)
        else:
            execution.state = WorkflowState.COMPLETED
            self._update_execution_statistics(True, execution.duration or 0)

        logger.info(f"Workflow execution completed: {execution.execution_id}, state: {execution.state.value}")

    def _update_execution_statistics(self, success: bool, duration: float) -> None:
        """更新执行统计"""
        with self._execution_lock:
            if success:
                self._execution_statistics['successful_executions'] += 1

                # 更新平均执行时间
                total_successful = self._execution_statistics['successful_executions']
                current_avg = self._execution_statistics['average_execution_time']
                self._execution_statistics['average_execution_time'] = (
                    (current_avg * (total_successful - 1) + duration) / total_successful
                )
            else:
                self._execution_statistics['failed_executions'] += 1

    def _register_builtin_handlers(self) -> None:
        """注册内置任务处理器"""
        self.register_task_handler("macro_analysis", self._handle_macro_analysis)
        self.register_task_handler("portfolio_optimization", self._handle_portfolio_optimization)
        self.register_task_handler("tactical_analysis", self._handle_tactical_analysis)
        self.register_task_handler("validation", self._handle_validation)
        self.register_task_handler("notification", self._handle_notification)

    async def _handle_macro_analysis(self, **kwargs) -> Dict[str, Any]:
        """处理宏观分析任务"""
        await asyncio.sleep(0.1)  # 模拟处理时间
        return {"cycle_phase": "expansion", "risk_level": 0.3}

    async def _handle_portfolio_optimization(self, **kwargs) -> Dict[str, Any]:
        """处理组合优化任务"""
        await asyncio.sleep(0.1)  # 模拟处理时间
        return {"target_position": 0.8, "sector_weights": {"tech": 0.4}}

    async def _handle_tactical_analysis(self, **kwargs) -> List[Dict[str, Any]]:
        """处理战术分析任务"""
        await asyncio.sleep(0.1)  # 模拟处理时间
        return [{"symbol": "000001.SZ", "signal": "buy", "strength": 0.8}]

    async def _handle_validation(self, **kwargs) -> Dict[str, Any]:
        """处理验证任务"""
        await asyncio.sleep(0.1)  # 模拟处理时间
        return {"validation_score": 0.85, "confidence": 0.9}

    async def _handle_notification(self, **kwargs) -> Dict[str, Any]:
        """处理通知任务"""
        await asyncio.sleep(0.01)  # 模拟处理时间
        return {"notification_sent": True}

    async def _wait_for_active_executions(self) -> None:
        """等待活跃执行完成"""
        timeout = 300  # 5分钟超时
        start_time = datetime.now()

        while self._active_executions:
            if (datetime.now() - start_time).total_seconds() > timeout:
                logger.warning(f"Timeout waiting for active executions: {list(self._active_executions.keys())}")
                break
            await asyncio.sleep(1)

    async def _monitoring_loop(self) -> None:
        """监控循环"""
        while self._is_running:
            try:
                await self._check_execution_timeouts()
                await self._cleanup_execution_history()
                await asyncio.sleep(30)  # 每30秒检查一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Workflow monitoring loop error: {e}")
                await asyncio.sleep(30)

    async def _check_execution_timeouts(self) -> None:
        """检查执行超时"""
        current_time = datetime.now()

        with self._execution_lock:
            for execution in list(self._active_executions.values()):
                workflow = self._workflow_definitions[execution.workflow_id]
                if (execution.start_time and
                    (current_time - execution.start_time).total_seconds() > workflow.global_timeout):
                    logger.warning(f"Workflow execution timeout: {execution.execution_id}")
                    execution.state = WorkflowState.FAILED
                    execution.error_message = "Workflow execution timeout"
                    execution.end_time = current_time

    async def _cleanup_execution_history(self) -> None:
        """清理执行历史"""
        max_history_size = 1000
        if len(self._execution_history) > max_history_size:
            # 保留最近的记录
            self._execution_history = self._execution_history[-max_history_size:]
