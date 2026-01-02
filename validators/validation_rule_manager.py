"""
验证规则管理器实现

负责动态验证规则配置、规则验证和管理、规则学习和优化，
提供灵活的验证规则生命周期管理。
"""

import asyncio
import logging
import uuid
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Set
from collections import defaultdict, deque
from enum import Enum
from dataclasses import dataclass, field
import threading

try:
    from ..config import IntegrationConfig
    from ..exceptions import ValidationException
except ImportError:
    from config import IntegrationConfig
    from exceptions import ValidationException

logger = logging.getLogger(__name__)


class RuleType(Enum):
    """规则类型枚举"""
    FORMAT_RULE = "format"
    RANGE_RULE = "range"
    BUSINESS_RULE = "business"
    STATISTICAL_RULE = "statistical"
    CORRELATION_RULE = "correlation"
    THRESHOLD_RULE = "threshold"
    PATTERN_RULE = "pattern"


class RuleStatus(Enum):
    """规则状态枚举"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    DEPRECATED = "deprecated"
    TESTING = "testing"


@dataclass
class ValidationRule:
    """验证规则定义"""
    rule_id: str
    name: str
    description: str
    rule_type: RuleType
    condition: Callable[[Any], bool]
    error_message: str
    warning_message: Optional[str] = None
    severity: str = "error"  # error, warning, info
    priority: int = 1
    status: RuleStatus = RuleStatus.ACTIVE
    created_time: datetime = field(default_factory=datetime.now)
    last_modified: datetime = field(default_factory=datetime.now)
    created_by: str = "system"
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # 统计信息
    execution_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    last_executed: Optional[datetime] = None
    average_execution_time: float = 0.0


@dataclass
class RuleExecutionResult:
    """规则执行结果"""
    rule_id: str
    execution_id: str
    is_valid: bool
    execution_time: float
    error_message: Optional[str] = None
    warning_message: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    input_data: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RulePerformanceMetrics:
    """规则性能指标"""
    rule_id: str
    total_executions: int
    success_rate: float
    average_execution_time: float
    error_rate: float
    last_30_days_executions: int
    trend: str  # improving, stable, declining
    efficiency_score: float


class ValidationRuleManager:
    """验证规则管理器实现类
    
    负责动态验证规则配置、规则验证和管理、规则学习和优化，
    提供灵活的验证规则生命周期管理。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化验证规则管理器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False
        
        # 规则存储
        self._validation_rules: Dict[str, ValidationRule] = {}
        self._rules_by_type: Dict[RuleType, Set[str]] = defaultdict(set)
        self._rules_by_tag: Dict[str, Set[str]] = defaultdict(set)
        
        # 执行历史
        self._execution_history: deque = deque(maxlen=10000)
        self._performance_cache: Dict[str, RulePerformanceMetrics] = {}
        
        # 规则学习
        self._learning_enabled = True
        self._learning_data: Dict[str, List[Any]] = defaultdict(list)
        self._optimization_suggestions: List[Dict[str, Any]] = []
        
        # 规则统计
        self._rule_statistics = {
            'total_rules': 0,
            'active_rules': 0,
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'average_execution_time': 0.0
        }
        
        # 锁和同步
        self._rule_lock = threading.RLock()
        
        # 监控任务
        self._monitoring_task: Optional[asyncio.Task] = None
        self._optimization_task: Optional[asyncio.Task] = None
        
        # 注册默认规则
        self._register_default_rules()
        
        logger.info("ValidationRuleManager initialized")
    
    async def start(self) -> bool:
        """启动验证规则管理器
        
        Returns:
            bool: 启动是否成功
        """
        try:
            if self._is_running:
                logger.warning("ValidationRuleManager is already running")
                return True
            
            # 启动监控任务
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            
            if self._learning_enabled:
                self._optimization_task = asyncio.create_task(self._optimization_loop())
            
            self._is_running = True
            logger.info("ValidationRuleManager started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start ValidationRuleManager: {e}")
            return False
    
    async def stop(self) -> bool:
        """停止验证规则管理器
        
        Returns:
            bool: 停止是否成功
        """
        try:
            if not self._is_running:
                return True
            
            logger.info("Stopping ValidationRuleManager...")
            self._is_running = False
            
            # 停止监控任务
            if self._monitoring_task:
                self._monitoring_task.cancel()
            if self._optimization_task:
                self._optimization_task.cancel()
            
            await asyncio.gather(
                self._monitoring_task, self._optimization_task, 
                return_exceptions=True
            )
            
            logger.info("ValidationRuleManager stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping ValidationRuleManager: {e}")
            return False
    
    def register_rule(self, rule: ValidationRule) -> bool:
        """注册验证规则
        
        Args:
            rule: 验证规则
            
        Returns:
            bool: 注册是否成功
        """
        try:
            with self._rule_lock:
                # 验证规则
                if not self._validate_rule(rule):
                    return False
                
                # 存储规则
                self._validation_rules[rule.rule_id] = rule
                self._rules_by_type[rule.rule_type].add(rule.rule_id)
                
                # 更新标签索引
                for tag in rule.tags:
                    self._rules_by_tag[tag].add(rule.rule_id)
                
                # 更新统计
                self._rule_statistics['total_rules'] += 1
                if rule.status == RuleStatus.ACTIVE:
                    self._rule_statistics['active_rules'] += 1
            
            logger.info(f"Validation rule registered: {rule.rule_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register rule {rule.rule_id}: {e}")
            return False
    
    def unregister_rule(self, rule_id: str) -> bool:
        """注销验证规则
        
        Args:
            rule_id: 规则ID
            
        Returns:
            bool: 注销是否成功
        """
        try:
            with self._rule_lock:
                if rule_id not in self._validation_rules:
                    return False
                
                rule = self._validation_rules[rule_id]
                
                # 从索引中移除
                self._rules_by_type[rule.rule_type].discard(rule_id)
                for tag in rule.tags:
                    self._rules_by_tag[tag].discard(rule_id)
                
                # 删除规则
                del self._validation_rules[rule_id]
                
                # 更新统计
                self._rule_statistics['total_rules'] -= 1
                if rule.status == RuleStatus.ACTIVE:
                    self._rule_statistics['active_rules'] -= 1
            
            logger.info(f"Validation rule unregistered: {rule_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to unregister rule {rule_id}: {e}")
            return False
    
    async def execute_rules(self, data: Any, rule_types: Optional[List[RuleType]] = None,
                          tags: Optional[List[str]] = None) -> List[RuleExecutionResult]:
        """执行验证规则
        
        Args:
            data: 待验证数据
            rule_types: 规则类型过滤
            tags: 标签过滤
            
        Returns:
            List[RuleExecutionResult]: 执行结果列表
        """
        if not self._is_running:
            raise ValidationException("ValidationRuleManager is not running")
        
        execution_results = []
        
        try:
            # 获取要执行的规则
            rules_to_execute = self._get_rules_to_execute(rule_types, tags)
            
            # 并发执行规则
            execution_tasks = []
            for rule in rules_to_execute:
                task = asyncio.create_task(self._execute_single_rule(rule, data))
                execution_tasks.append(task)
            
            # 等待所有规则执行完成
            results = await asyncio.gather(*execution_tasks, return_exceptions=True)
            
            # 处理结果
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Rule execution failed: {result}")
                else:
                    execution_results.append(result)
                    
                    # 记录执行历史
                    self._execution_history.append(result)
                    
                    # 更新统计
                    self._update_rule_statistics(result)
            
            # 学习数据收集
            if self._learning_enabled:
                await self._collect_learning_data(data, execution_results)
            
            return execution_results
            
        except Exception as e:
            logger.error(f"Rules execution failed: {e}")
            raise ValidationException(f"Rules execution failed: {e}")
    
    def get_rule(self, rule_id: str) -> Optional[ValidationRule]:
        """获取验证规则
        
        Args:
            rule_id: 规则ID
            
        Returns:
            Optional[ValidationRule]: 验证规则
        """
        with self._rule_lock:
            return self._validation_rules.get(rule_id)
    
    def get_rules_by_type(self, rule_type: RuleType) -> List[ValidationRule]:
        """按类型获取规则
        
        Args:
            rule_type: 规则类型
            
        Returns:
            List[ValidationRule]: 规则列表
        """
        with self._rule_lock:
            rule_ids = self._rules_by_type.get(rule_type, set())
            return [self._validation_rules[rid] for rid in rule_ids 
                   if rid in self._validation_rules]
    
    def get_rules_by_tag(self, tag: str) -> List[ValidationRule]:
        """按标签获取规则
        
        Args:
            tag: 标签
            
        Returns:
            List[ValidationRule]: 规则列表
        """
        with self._rule_lock:
            rule_ids = self._rules_by_tag.get(tag, set())
            return [self._validation_rules[rid] for rid in rule_ids 
                   if rid in self._validation_rules]
    
    def get_rule_performance(self, rule_id: str) -> Optional[RulePerformanceMetrics]:
        """获取规则性能指标
        
        Args:
            rule_id: 规则ID
            
        Returns:
            Optional[RulePerformanceMetrics]: 性能指标
        """
        return self._performance_cache.get(rule_id)
    
    def get_rule_statistics(self) -> Dict[str, Any]:
        """获取规则统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        with self._rule_lock:
            stats = self._rule_statistics.copy()
            stats.update({
                'rules_by_type': {
                    rule_type.value: len(rule_ids) 
                    for rule_type, rule_ids in self._rules_by_type.items()
                },
                'rules_by_status': self._get_rules_by_status_count(),
                'execution_history_size': len(self._execution_history),
                'optimization_suggestions': len(self._optimization_suggestions)
            })
            return stats
    
    async def optimize_rules(self) -> List[Dict[str, Any]]:
        """优化规则
        
        Returns:
            List[Dict[str, Any]]: 优化建议列表
        """
        try:
            optimization_suggestions = []
            
            # 分析规则性能
            await self._analyze_rule_performance()
            
            # 生成优化建议
            for rule_id, rule in self._validation_rules.items():
                suggestions = await self._generate_rule_optimization_suggestions(rule)
                if suggestions:
                    optimization_suggestions.extend(suggestions)
            
            # 存储建议
            self._optimization_suggestions = optimization_suggestions
            
            logger.info(f"Rule optimization completed: {len(optimization_suggestions)} suggestions generated")
            return optimization_suggestions
            
        except Exception as e:
            logger.error(f"Rule optimization failed: {e}")
            return []

    # 私有方法实现
    def _validate_rule(self, rule: ValidationRule) -> bool:
        """验证规则定义"""
        try:
            # 检查必要字段
            if not rule.rule_id or not rule.name or not rule.condition:
                logger.error("Rule missing required fields")
                return False

            # 检查规则ID唯一性
            if rule.rule_id in self._validation_rules:
                logger.error(f"Rule ID already exists: {rule.rule_id}")
                return False

            # 测试规则条件函数
            try:
                test_result = rule.condition(None)
                if not isinstance(test_result, bool):
                    logger.error("Rule condition must return boolean")
                    return False
            except Exception as e:
                logger.warning(f"Rule condition test failed: {e}")

            return True

        except Exception as e:
            logger.error(f"Rule validation failed: {e}")
            return False

    def _get_rules_to_execute(self, rule_types: Optional[List[RuleType]] = None,
                            tags: Optional[List[str]] = None) -> List[ValidationRule]:
        """获取要执行的规则"""
        with self._rule_lock:
            candidate_rules = set()

            # 按类型过滤
            if rule_types:
                for rule_type in rule_types:
                    candidate_rules.update(self._rules_by_type.get(rule_type, set()))
            else:
                # 所有活跃规则
                candidate_rules = {
                    rule_id for rule_id, rule in self._validation_rules.items()
                    if rule.status == RuleStatus.ACTIVE
                }

            # 按标签过滤
            if tags:
                tag_rules = set()
                for tag in tags:
                    tag_rules.update(self._rules_by_tag.get(tag, set()))
                candidate_rules = candidate_rules.intersection(tag_rules)

            # 获取规则对象并按优先级排序
            rules = [
                self._validation_rules[rule_id]
                for rule_id in candidate_rules
                if rule_id in self._validation_rules
            ]

            rules.sort(key=lambda r: r.priority, reverse=True)
            return rules

    async def _execute_single_rule(self, rule: ValidationRule, data: Any) -> RuleExecutionResult:
        """执行单个规则"""
        execution_id = str(uuid.uuid4())
        start_time = datetime.now()

        try:
            # 执行规则条件
            is_valid = rule.condition(data)

            # 计算执行时间
            execution_time = (datetime.now() - start_time).total_seconds()

            # 创建执行结果
            result = RuleExecutionResult(
                rule_id=rule.rule_id,
                execution_id=execution_id,
                is_valid=is_valid,
                execution_time=execution_time,
                input_data=data,
                error_message=rule.error_message if not is_valid and rule.severity == "error" else None,
                warning_message=rule.warning_message if not is_valid and rule.severity == "warning" else None
            )

            # 更新规则统计
            with self._rule_lock:
                rule.execution_count += 1
                rule.last_executed = datetime.now()

                if is_valid:
                    rule.success_count += 1
                else:
                    rule.failure_count += 1

                # 更新平均执行时间
                rule.average_execution_time = (
                    (rule.average_execution_time * (rule.execution_count - 1) + execution_time)
                    / rule.execution_count
                )

            return result

        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()

            # 创建错误结果
            result = RuleExecutionResult(
                rule_id=rule.rule_id,
                execution_id=execution_id,
                is_valid=False,
                execution_time=execution_time,
                input_data=data,
                error_message=f"Rule execution failed: {e}"
            )

            # 更新规则统计
            with self._rule_lock:
                rule.execution_count += 1
                rule.failure_count += 1
                rule.last_executed = datetime.now()

            logger.error(f"Rule execution failed: {rule.rule_id}, error: {e}")
            return result

    def _update_rule_statistics(self, result: RuleExecutionResult) -> None:
        """更新规则统计"""
        self._rule_statistics['total_executions'] += 1

        if result.is_valid:
            self._rule_statistics['successful_executions'] += 1
        else:
            self._rule_statistics['failed_executions'] += 1

        # 更新平均执行时间
        total_executions = self._rule_statistics['total_executions']
        current_avg = self._rule_statistics['average_execution_time']
        self._rule_statistics['average_execution_time'] = (
            (current_avg * (total_executions - 1) + result.execution_time) / total_executions
        )

    async def _collect_learning_data(self, data: Any, results: List[RuleExecutionResult]) -> None:
        """收集学习数据"""
        try:
            for result in results:
                learning_entry = {
                    'input_data': data,
                    'result': result,
                    'timestamp': datetime.now()
                }
                self._learning_data[result.rule_id].append(learning_entry)

                # 限制学习数据大小
                if len(self._learning_data[result.rule_id]) > 1000:
                    self._learning_data[result.rule_id] = self._learning_data[result.rule_id][-1000:]

        except Exception as e:
            logger.error(f"Failed to collect learning data: {e}")

    def _get_rules_by_status_count(self) -> Dict[str, int]:
        """获取按状态分组的规则数量"""
        status_count = defaultdict(int)
        for rule in self._validation_rules.values():
            status_count[rule.status.value] += 1
        return dict(status_count)

    async def _analyze_rule_performance(self) -> None:
        """分析规则性能"""
        try:
            current_time = datetime.now()
            thirty_days_ago = current_time - timedelta(days=30)

            for rule_id, rule in self._validation_rules.items():
                # 计算最近30天的执行次数
                recent_executions = sum(
                    1 for result in self._execution_history
                    if result.rule_id == rule_id and result.timestamp >= thirty_days_ago
                )

                # 计算成功率
                success_rate = (rule.success_count / rule.execution_count
                              if rule.execution_count > 0 else 0.0)

                # 计算错误率
                error_rate = (rule.failure_count / rule.execution_count
                            if rule.execution_count > 0 else 0.0)

                # 计算趋势
                trend = self._calculate_performance_trend(rule_id)

                # 计算效率评分
                efficiency_score = self._calculate_efficiency_score(rule)

                # 创建性能指标
                performance_metrics = RulePerformanceMetrics(
                    rule_id=rule_id,
                    total_executions=rule.execution_count,
                    success_rate=success_rate,
                    average_execution_time=rule.average_execution_time,
                    error_rate=error_rate,
                    last_30_days_executions=recent_executions,
                    trend=trend,
                    efficiency_score=efficiency_score
                )

                self._performance_cache[rule_id] = performance_metrics

        except Exception as e:
            logger.error(f"Rule performance analysis failed: {e}")

    def _calculate_performance_trend(self, rule_id: str) -> str:
        """计算性能趋势"""
        try:
            # 获取最近的执行结果
            recent_results = [
                result for result in self._execution_history
                if result.rule_id == rule_id
            ][-100:]  # 最近100次执行

            if len(recent_results) < 10:
                return "stable"

            # 分析成功率趋势
            first_half = recent_results[:len(recent_results)//2]
            second_half = recent_results[len(recent_results)//2:]

            first_success_rate = sum(1 for r in first_half if r.is_valid) / len(first_half)
            second_success_rate = sum(1 for r in second_half if r.is_valid) / len(second_half)

            if second_success_rate > first_success_rate + 0.1:
                return "improving"
            elif second_success_rate < first_success_rate - 0.1:
                return "declining"
            else:
                return "stable"

        except Exception:
            return "stable"

    def _calculate_efficiency_score(self, rule: ValidationRule) -> float:
        """计算效率评分"""
        try:
            # 基础效率评分
            base_score = 0.5

            # 成功率因子
            if rule.execution_count > 0:
                success_rate = rule.success_count / rule.execution_count
                base_score += success_rate * 0.3

            # 执行时间因子
            if rule.average_execution_time < 0.1:  # 小于100ms
                base_score += 0.2
            elif rule.average_execution_time < 1.0:  # 小于1s
                base_score += 0.1

            # 使用频率因子
            if rule.execution_count > 100:
                base_score += 0.1
            elif rule.execution_count > 10:
                base_score += 0.05

            return min(1.0, base_score)

        except Exception:
            return 0.5

    async def _generate_rule_optimization_suggestions(self, rule: ValidationRule) -> List[Dict[str, Any]]:
        """生成规则优化建议"""
        suggestions = []

        try:
            performance = self._performance_cache.get(rule.rule_id)
            if not performance:
                return suggestions

            # 低成功率建议
            if performance.success_rate < 0.5:
                suggestions.append({
                    'type': 'low_success_rate',
                    'rule_id': rule.rule_id,
                    'message': f"规则成功率较低({performance.success_rate:.1%})，建议检查规则逻辑",
                    'priority': 'high'
                })

            # 高执行时间建议
            if performance.average_execution_time > 1.0:
                suggestions.append({
                    'type': 'high_execution_time',
                    'rule_id': rule.rule_id,
                    'message': f"规则执行时间较长({performance.average_execution_time:.2f}s)，建议优化算法",
                    'priority': 'medium'
                })

            # 低使用频率建议
            if performance.last_30_days_executions < 5:
                suggestions.append({
                    'type': 'low_usage',
                    'rule_id': rule.rule_id,
                    'message': "规则使用频率较低，考虑是否需要保留",
                    'priority': 'low'
                })

            # 性能下降建议
            if performance.trend == "declining":
                suggestions.append({
                    'type': 'declining_performance',
                    'rule_id': rule.rule_id,
                    'message': "规则性能呈下降趋势，建议检查和更新",
                    'priority': 'high'
                })

        except Exception as e:
            logger.error(f"Failed to generate optimization suggestions for rule {rule.rule_id}: {e}")

        return suggestions

    def _register_default_rules(self) -> None:
        """注册默认验证规则"""
        # 数据格式规则
        format_rule = ValidationRule(
            rule_id="data_format_check",
            name="数据格式检查",
            description="检查数据是否为有效格式",
            rule_type=RuleType.FORMAT_RULE,
            condition=lambda data: data is not None,
            error_message="数据格式无效",
            tags={"format", "basic"}
        )
        self.register_rule(format_rule)

        # 数值范围规则
        range_rule = ValidationRule(
            rule_id="score_range_check",
            name="评分范围检查",
            description="检查评分是否在有效范围内",
            rule_type=RuleType.RANGE_RULE,
            condition=lambda data: (
                hasattr(data, 'validation_score') and
                0 <= data.validation_score <= 1
            ),
            error_message="评分超出有效范围[0,1]",
            tags={"range", "score"}
        )
        self.register_rule(range_rule)

        # 业务逻辑规则
        business_rule = ValidationRule(
            rule_id="execution_time_check",
            name="执行时间检查",
            description="检查执行时间是否合理",
            rule_type=RuleType.BUSINESS_RULE,
            condition=lambda data: (
                hasattr(data, 'execution_time') and
                data.execution_time >= 0 and
                data.execution_time < 3600  # 小于1小时
            ),
            error_message="执行时间异常",
            warning_message="执行时间较长，请检查",
            severity="warning",
            tags={"business", "performance"}
        )
        self.register_rule(business_rule)

    async def _monitoring_loop(self) -> None:
        """监控循环"""
        while self._is_running:
            try:
                # 更新性能缓存
                await self._analyze_rule_performance()

                # 清理过期数据
                await self._cleanup_old_data()

                await asyncio.sleep(300)  # 每5分钟更新一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Rule monitoring loop error: {e}")
                await asyncio.sleep(300)

    async def _optimization_loop(self) -> None:
        """优化循环"""
        while self._is_running:
            try:
                # 执行规则优化
                await self.optimize_rules()

                await asyncio.sleep(3600)  # 每小时优化一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Rule optimization loop error: {e}")
                await asyncio.sleep(3600)

    async def _cleanup_old_data(self) -> None:
        """清理过期数据"""
        try:
            # 清理学习数据
            cutoff_time = datetime.now() - timedelta(days=30)

            for rule_id in list(self._learning_data.keys()):
                self._learning_data[rule_id] = [
                    entry for entry in self._learning_data[rule_id]
                    if entry['timestamp'] > cutoff_time
                ]

                # 如果没有数据，删除键
                if not self._learning_data[rule_id]:
                    del self._learning_data[rule_id]

        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
