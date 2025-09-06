"""
性能监控器实现

负责独立的性能分析、性能趋势预测、自动优化建议生成，
提供深度的性能洞察和智能优化建议。
"""

import asyncio
import logging
import uuid
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from collections import deque, defaultdict
from enum import Enum
from dataclasses import dataclass, field
import threading
from scipy import stats
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings('ignore')

from ..models import PerformanceMetrics
from ..config import IntegrationConfig
from ..exceptions import MonitoringException

logger = logging.getLogger(__name__)


class TrendDirection(Enum):
    """趋势方向枚举"""
    IMPROVING = "improving"
    STABLE = "stable"
    DECLINING = "declining"
    VOLATILE = "volatile"


class PerformanceLevel(Enum):
    """性能级别枚举"""
    EXCELLENT = "excellent"
    GOOD = "good"
    AVERAGE = "average"
    POOR = "poor"
    CRITICAL = "critical"


@dataclass
class PerformanceTrend:
    """性能趋势"""
    metric_name: str
    direction: TrendDirection
    slope: float
    confidence: float
    prediction_horizon: int  # 预测时间范围（小时）
    predicted_values: List[float]
    trend_start_time: datetime
    last_update_time: datetime = field(default_factory=datetime.now)


@dataclass
class PerformanceAnomaly:
    """性能异常"""
    anomaly_id: str
    metric_name: str
    component: str
    anomaly_type: str  # spike, drop, drift, outlier
    severity: str  # low, medium, high, critical
    detected_time: datetime
    value: float
    expected_range: Tuple[float, float]
    confidence: float
    description: str
    resolved: bool = False
    resolved_time: Optional[datetime] = None


@dataclass
class OptimizationSuggestion:
    """优化建议"""
    suggestion_id: str
    component: str
    category: str  # performance, resource, configuration
    priority: str  # low, medium, high, critical
    title: str
    description: str
    expected_improvement: str
    implementation_effort: str  # low, medium, high
    created_time: datetime = field(default_factory=datetime.now)
    status: str = "pending"  # pending, implemented, dismissed
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PerformanceReport:
    """性能报告"""
    report_id: str
    report_type: str  # daily, weekly, monthly, custom
    start_time: datetime
    end_time: datetime
    overall_score: float
    component_scores: Dict[str, float]
    trends: List[PerformanceTrend]
    anomalies: List[PerformanceAnomaly]
    suggestions: List[OptimizationSuggestion]
    key_insights: List[str]
    generated_time: datetime = field(default_factory=datetime.now)


class PerformanceMonitor:
    """性能监控器实现类
    
    负责独立的性能分析、性能趋势预测、自动优化建议生成，
    提供深度的性能洞察和智能优化建议。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化性能监控器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False
        
        # 性能数据存储
        self._performance_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self._component_metrics: Dict[str, PerformanceMetrics] = {}
        
        # 趋势分析
        self._performance_trends: Dict[str, PerformanceTrend] = {}
        self._trend_models: Dict[str, Any] = {}
        
        # 异常检测
        self._anomalies: List[PerformanceAnomaly] = []
        self._anomaly_thresholds: Dict[str, Dict[str, float]] = {}
        self._baseline_metrics: Dict[str, Dict[str, float]] = {}
        
        # 优化建议
        self._optimization_suggestions: List[OptimizationSuggestion] = []
        self._suggestion_history: List[OptimizationSuggestion] = []
        
        # 性能报告
        self._performance_reports: List[PerformanceReport] = []
        
        # 监控统计
        self._monitoring_statistics = {
            'total_metrics_collected': 0,
            'anomalies_detected': 0,
            'suggestions_generated': 0,
            'reports_generated': 0,
            'trend_accuracy': 0.0
        }
        
        # 锁和同步
        self._monitor_lock = threading.RLock()
        
        # 监控任务
        self._analysis_task: Optional[asyncio.Task] = None
        self._prediction_task: Optional[asyncio.Task] = None
        self._reporting_task: Optional[asyncio.Task] = None
        
        # 初始化异常检测阈值
        self._initialize_anomaly_thresholds()
        
        logger.info("PerformanceMonitor initialized")
    
    async def start(self) -> bool:
        """启动性能监控器
        
        Returns:
            bool: 启动是否成功
        """
        try:
            if self._is_running:
                logger.warning("PerformanceMonitor is already running")
                return True
            
            if not self.config.monitoring.enable_performance_monitoring:
                logger.info("Performance monitoring is disabled in config")
                return True
            
            # 启动监控任务
            self._analysis_task = asyncio.create_task(self._analysis_loop())
            self._prediction_task = asyncio.create_task(self._prediction_loop())
            self._reporting_task = asyncio.create_task(self._reporting_loop())
            
            self._is_running = True
            logger.info("PerformanceMonitor started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start PerformanceMonitor: {e}")
            return False
    
    async def stop(self) -> bool:
        """停止性能监控器
        
        Returns:
            bool: 停止是否成功
        """
        try:
            if not self._is_running:
                return True
            
            logger.info("Stopping PerformanceMonitor...")
            self._is_running = False
            
            # 停止监控任务
            tasks = [self._analysis_task, self._prediction_task, self._reporting_task]
            for task in tasks:
                if task:
                    task.cancel()
            
            await asyncio.gather(*[t for t in tasks if t], return_exceptions=True)
            
            logger.info("PerformanceMonitor stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping PerformanceMonitor: {e}")
            return False
    
    async def collect_performance_data(self, metrics: PerformanceMetrics) -> bool:
        """收集性能数据
        
        Args:
            metrics: 性能指标
            
        Returns:
            bool: 收集是否成功
        """
        try:
            if not self._is_running:
                return False
            
            with self._monitor_lock:
                # 存储组件指标
                self._component_metrics[metrics.component_name] = metrics
                
                # 存储历史数据
                self._performance_history[metrics.component_name].append(metrics)
                
                # 更新统计
                self._monitoring_statistics['total_metrics_collected'] += 1
            
            # 实时异常检测
            await self._detect_anomalies(metrics)
            
            logger.debug(f"Performance data collected for {metrics.component_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to collect performance data: {e}")
            return False
    
    async def analyze_performance_trends(self, component: Optional[str] = None,
                                       time_window_hours: int = 24) -> List[PerformanceTrend]:
        """分析性能趋势
        
        Args:
            component: 组件名称（可选）
            time_window_hours: 时间窗口（小时）
            
        Returns:
            List[PerformanceTrend]: 趋势分析结果
        """
        try:
            trends = []
            cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
            
            components_to_analyze = [component] if component else list(self._performance_history.keys())
            
            for comp in components_to_analyze:
                if comp not in self._performance_history:
                    continue
                
                # 获取时间窗口内的数据
                recent_data = [
                    metrics for metrics in self._performance_history[comp]
                    if metrics.timestamp >= cutoff_time
                ]
                
                if len(recent_data) < 10:  # 数据点太少
                    continue
                
                # 分析各个指标的趋势
                metric_trends = await self._analyze_component_trends(comp, recent_data)
                trends.extend(metric_trends)
            
            # 更新趋势缓存
            with self._monitor_lock:
                for trend in trends:
                    key = f"{trend.metric_name}_{component or 'all'}"
                    self._performance_trends[key] = trend
            
            return trends
            
        except Exception as e:
            logger.error(f"Performance trend analysis failed: {e}")
            raise MonitoringException(f"Trend analysis failed: {e}")
    
    async def predict_performance(self, component: str, metric_name: str,
                                prediction_hours: int = 24) -> Optional[List[float]]:
        """预测性能指标
        
        Args:
            component: 组件名称
            metric_name: 指标名称
            prediction_hours: 预测时间范围（小时）
            
        Returns:
            Optional[List[float]]: 预测值列表
        """
        try:
            if component not in self._performance_history:
                return None
            
            # 获取历史数据
            historical_data = list(self._performance_history[component])
            if len(historical_data) < 50:  # 数据不足
                return None
            
            # 提取指标值
            values = []
            timestamps = []
            
            for metrics in historical_data:
                if hasattr(metrics, metric_name):
                    values.append(getattr(metrics, metric_name))
                    timestamps.append(metrics.timestamp.timestamp())
            
            if len(values) < 20:
                return None
            
            # 训练预测模型
            predictions = await self._train_and_predict(
                timestamps, values, prediction_hours
            )
            
            return predictions
            
        except Exception as e:
            logger.error(f"Performance prediction failed: {e}")
            return None
    
    async def generate_optimization_suggestions(self, component: Optional[str] = None) -> List[OptimizationSuggestion]:
        """生成优化建议
        
        Args:
            component: 组件名称（可选）
            
        Returns:
            List[OptimizationSuggestion]: 优化建议列表
        """
        try:
            suggestions = []
            
            components_to_analyze = [component] if component else list(self._component_metrics.keys())
            
            for comp in components_to_analyze:
                if comp not in self._component_metrics:
                    continue
                
                metrics = self._component_metrics[comp]
                comp_suggestions = await self._analyze_component_for_optimization(comp, metrics)
                suggestions.extend(comp_suggestions)
            
            # 存储建议
            with self._monitor_lock:
                self._optimization_suggestions.extend(suggestions)
                self._monitoring_statistics['suggestions_generated'] += len(suggestions)
            
            logger.info(f"Generated {len(suggestions)} optimization suggestions")
            return suggestions
            
        except Exception as e:
            logger.error(f"Optimization suggestion generation failed: {e}")
            return []
    
    async def generate_performance_report(self, report_type: str = "daily",
                                        start_time: Optional[datetime] = None,
                                        end_time: Optional[datetime] = None) -> PerformanceReport:
        """生成性能报告
        
        Args:
            report_type: 报告类型
            start_time: 开始时间
            end_time: 结束时间
            
        Returns:
            PerformanceReport: 性能报告
        """
        try:
            # 确定时间范围
            if not end_time:
                end_time = datetime.now()
            
            if not start_time:
                if report_type == "daily":
                    start_time = end_time - timedelta(days=1)
                elif report_type == "weekly":
                    start_time = end_time - timedelta(weeks=1)
                elif report_type == "monthly":
                    start_time = end_time - timedelta(days=30)
                else:
                    start_time = end_time - timedelta(days=1)
            
            # 生成报告
            report = await self._generate_comprehensive_report(
                report_type, start_time, end_time
            )
            
            # 存储报告
            with self._monitor_lock:
                self._performance_reports.append(report)
                self._monitoring_statistics['reports_generated'] += 1
            
            logger.info(f"Performance report generated: {report.report_id}")
            return report
            
        except Exception as e:
            logger.error(f"Performance report generation failed: {e}")
            raise MonitoringException(f"Report generation failed: {e}")
    
    def get_current_performance_status(self) -> Dict[str, Any]:
        """获取当前性能状态
        
        Returns:
            Dict[str, Any]: 性能状态信息
        """
        with self._monitor_lock:
            status = {
                'components': {},
                'overall_health': 'unknown',
                'active_anomalies': len([a for a in self._anomalies if not a.resolved]),
                'pending_suggestions': len([s for s in self._optimization_suggestions if s.status == 'pending']),
                'monitoring_statistics': self._monitoring_statistics.copy()
            }
            
            # 组件状态
            for comp, metrics in self._component_metrics.items():
                status['components'][comp] = {
                    'cpu_usage': metrics.cpu_usage,
                    'memory_usage': metrics.memory_usage,
                    'response_time': metrics.response_time,
                    'throughput': metrics.throughput,
                    'error_rate': metrics.error_rate,
                    'availability': metrics.availability,
                    'last_update': metrics.timestamp.isoformat()
                }
            
            # 计算整体健康状态
            status['overall_health'] = self._calculate_overall_health()
            
            return status
    
    def get_performance_statistics(self) -> Dict[str, Any]:
        """获取性能统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        with self._monitor_lock:
            return self._monitoring_statistics.copy()

    # 私有方法实现
    def _initialize_anomaly_thresholds(self) -> None:
        """初始化异常检测阈值"""
        self._anomaly_thresholds = {
            'cpu_usage': {'upper': 0.9, 'lower': 0.0, 'spike_factor': 2.0},
            'memory_usage': {'upper': 0.9, 'lower': 0.0, 'spike_factor': 2.0},
            'response_time': {'upper': 5000, 'lower': 0, 'spike_factor': 3.0},
            'error_rate': {'upper': 0.1, 'lower': 0.0, 'spike_factor': 5.0},
            'throughput': {'upper': float('inf'), 'lower': 0, 'drop_factor': 0.5}
        }

    async def _detect_anomalies(self, metrics: PerformanceMetrics) -> None:
        """检测性能异常"""
        try:
            component = metrics.component_name

            # 检查各个指标
            metric_checks = [
                ('cpu_usage', metrics.cpu_usage),
                ('memory_usage', metrics.memory_usage),
                ('response_time', metrics.response_time),
                ('error_rate', metrics.error_rate),
                ('throughput', metrics.throughput)
            ]

            for metric_name, value in metric_checks:
                anomaly = await self._check_metric_anomaly(component, metric_name, value)
                if anomaly:
                    with self._monitor_lock:
                        self._anomalies.append(anomaly)
                        self._monitoring_statistics['anomalies_detected'] += 1

                    logger.warning(f"Performance anomaly detected: {anomaly.description}")

        except Exception as e:
            logger.error(f"Anomaly detection failed: {e}")

    async def _check_metric_anomaly(self, component: str, metric_name: str,
                                  value: float) -> Optional[PerformanceAnomaly]:
        """检查单个指标异常"""
        try:
            thresholds = self._anomaly_thresholds.get(metric_name, {})
            if not thresholds:
                return None

            # 基本阈值检查
            upper_threshold = thresholds.get('upper', float('inf'))
            lower_threshold = thresholds.get('lower', 0)

            anomaly_type = None
            severity = "low"

            if value > upper_threshold:
                anomaly_type = "spike"
                severity = "high" if value > upper_threshold * 1.5 else "medium"
            elif value < lower_threshold:
                anomaly_type = "drop"
                severity = "medium"

            # 统计异常检查（基于历史数据）
            if not anomaly_type and component in self._performance_history:
                historical_values = [
                    getattr(m, metric_name) for m in self._performance_history[component]
                    if hasattr(m, metric_name)
                ][-100:]  # 最近100个值

                if len(historical_values) >= 10:
                    mean_val = np.mean(historical_values)
                    std_val = np.std(historical_values)

                    # Z-score异常检测
                    if std_val > 0:
                        z_score = abs(value - mean_val) / std_val
                        if z_score > 3:  # 3-sigma规则
                            anomaly_type = "outlier"
                            severity = "high" if z_score > 4 else "medium"

            if anomaly_type:
                return PerformanceAnomaly(
                    anomaly_id=str(uuid.uuid4()),
                    metric_name=metric_name,
                    component=component,
                    anomaly_type=anomaly_type,
                    severity=severity,
                    detected_time=datetime.now(),
                    value=value,
                    expected_range=(lower_threshold, upper_threshold),
                    confidence=0.8,
                    description=f"{component}的{metric_name}出现{anomaly_type}异常: {value}"
                )

            return None

        except Exception as e:
            logger.error(f"Metric anomaly check failed: {e}")
            return None

    async def _analyze_component_trends(self, component: str,
                                      data: List[PerformanceMetrics]) -> List[PerformanceTrend]:
        """分析组件趋势"""
        trends = []

        try:
            # 分析各个指标
            metrics_to_analyze = ['cpu_usage', 'memory_usage', 'response_time', 'error_rate', 'throughput']

            for metric_name in metrics_to_analyze:
                values = []
                timestamps = []

                for metrics in data:
                    if hasattr(metrics, metric_name):
                        values.append(getattr(metrics, metric_name))
                        timestamps.append(metrics.timestamp.timestamp())

                if len(values) < 5:
                    continue

                # 计算趋势
                trend = await self._calculate_trend(component, metric_name, timestamps, values)
                if trend:
                    trends.append(trend)

        except Exception as e:
            logger.error(f"Component trend analysis failed: {e}")

        return trends

    async def _calculate_trend(self, component: str, metric_name: str,
                             timestamps: List[float], values: List[float]) -> Optional[PerformanceTrend]:
        """计算趋势"""
        try:
            if len(values) < 5:
                return None

            # 线性回归分析趋势
            X = np.array(timestamps).reshape(-1, 1)
            y = np.array(values)

            model = LinearRegression()
            model.fit(X, y)

            slope = model.coef_[0]
            r_squared = model.score(X, y)

            # 确定趋势方向
            if abs(slope) < 1e-6:
                direction = TrendDirection.STABLE
            elif slope > 0:
                direction = TrendDirection.IMPROVING if metric_name == 'throughput' else TrendDirection.DECLINING
            else:
                direction = TrendDirection.DECLINING if metric_name == 'throughput' else TrendDirection.IMPROVING

            # 检查波动性
            if r_squared < 0.3:  # 低相关性表示高波动
                direction = TrendDirection.VOLATILE

            # 预测未来值
            future_timestamps = np.arange(
                timestamps[-1],
                timestamps[-1] + 24 * 3600,  # 未来24小时
                3600  # 每小时一个点
            ).reshape(-1, 1)

            predicted_values = model.predict(future_timestamps).tolist()

            return PerformanceTrend(
                metric_name=f"{component}_{metric_name}",
                direction=direction,
                slope=slope,
                confidence=r_squared,
                prediction_horizon=24,
                predicted_values=predicted_values,
                trend_start_time=datetime.fromtimestamp(timestamps[0])
            )

        except Exception as e:
            logger.error(f"Trend calculation failed: {e}")
            return None

    async def _train_and_predict(self, timestamps: List[float], values: List[float],
                               prediction_hours: int) -> List[float]:
        """训练模型并预测"""
        try:
            # 数据预处理
            X = np.array(timestamps).reshape(-1, 1)
            y = np.array(values)

            # 简单线性回归预测
            model = LinearRegression()
            model.fit(X, y)

            # 生成未来时间点
            last_timestamp = timestamps[-1]
            future_timestamps = np.arange(
                last_timestamp,
                last_timestamp + prediction_hours * 3600,
                3600  # 每小时一个预测点
            ).reshape(-1, 1)

            # 预测
            predictions = model.predict(future_timestamps)

            # 确保预测值在合理范围内
            predictions = np.clip(predictions, 0, None)

            return predictions.tolist()

        except Exception as e:
            logger.error(f"Prediction model training failed: {e}")
            return []

    async def _analyze_component_for_optimization(self, component: str,
                                                metrics: PerformanceMetrics) -> List[OptimizationSuggestion]:
        """分析组件优化建议"""
        suggestions = []

        try:
            # CPU使用率优化
            if metrics.cpu_usage > 0.8:
                suggestions.append(OptimizationSuggestion(
                    suggestion_id=str(uuid.uuid4()),
                    component=component,
                    category="performance",
                    priority="high",
                    title="CPU使用率过高",
                    description=f"CPU使用率达到{metrics.cpu_usage:.1%}，建议优化算法或增加计算资源",
                    expected_improvement="降低CPU使用率20-30%",
                    implementation_effort="medium"
                ))

            # 内存使用率优化
            if metrics.memory_usage > 0.8:
                suggestions.append(OptimizationSuggestion(
                    suggestion_id=str(uuid.uuid4()),
                    component=component,
                    category="resource",
                    priority="high",
                    title="内存使用率过高",
                    description=f"内存使用率达到{metrics.memory_usage:.1%}，建议优化内存管理或增加内存",
                    expected_improvement="降低内存使用率15-25%",
                    implementation_effort="medium"
                ))

            # 响应时间优化
            if metrics.response_time > 1000:  # 大于1秒
                suggestions.append(OptimizationSuggestion(
                    suggestion_id=str(uuid.uuid4()),
                    component=component,
                    category="performance",
                    priority="medium",
                    title="响应时间过长",
                    description=f"平均响应时间{metrics.response_time:.0f}ms，建议优化处理逻辑或增加缓存",
                    expected_improvement="响应时间减少30-50%",
                    implementation_effort="high"
                ))

            # 错误率优化
            if metrics.error_rate > 0.05:  # 大于5%
                suggestions.append(OptimizationSuggestion(
                    suggestion_id=str(uuid.uuid4()),
                    component=component,
                    category="performance",
                    priority="critical",
                    title="错误率过高",
                    description=f"错误率达到{metrics.error_rate:.1%}，需要立即检查和修复",
                    expected_improvement="错误率降低到1%以下",
                    implementation_effort="high"
                ))

            # 吞吐量优化
            if metrics.throughput < 50:  # 低于50 req/s
                suggestions.append(OptimizationSuggestion(
                    suggestion_id=str(uuid.uuid4()),
                    component=component,
                    category="performance",
                    priority="medium",
                    title="吞吐量较低",
                    description=f"当前吞吐量{metrics.throughput:.1f} req/s，建议优化并发处理",
                    expected_improvement="吞吐量提升50-100%",
                    implementation_effort="medium"
                ))

        except Exception as e:
            logger.error(f"Component optimization analysis failed: {e}")

        return suggestions

    async def _generate_comprehensive_report(self, report_type: str,
                                           start_time: datetime,
                                           end_time: datetime) -> PerformanceReport:
        """生成综合性能报告"""
        try:
            report_id = str(uuid.uuid4())

            # 收集时间范围内的数据
            component_scores = {}
            all_trends = []
            relevant_anomalies = []

            for component, history in self._performance_history.items():
                # 过滤时间范围内的数据
                period_data = [
                    metrics for metrics in history
                    if start_time <= metrics.timestamp <= end_time
                ]

                if not period_data:
                    continue

                # 计算组件评分
                component_scores[component] = self._calculate_component_score(period_data)

                # 分析趋势
                trends = await self._analyze_component_trends(component, period_data)
                all_trends.extend(trends)

            # 过滤相关异常
            relevant_anomalies = [
                anomaly for anomaly in self._anomalies
                if start_time <= anomaly.detected_time <= end_time
            ]

            # 生成优化建议
            suggestions = await self.generate_optimization_suggestions()

            # 计算整体评分
            overall_score = np.mean(list(component_scores.values())) if component_scores else 0.0

            # 生成关键洞察
            key_insights = self._generate_key_insights(
                component_scores, all_trends, relevant_anomalies, suggestions
            )

            return PerformanceReport(
                report_id=report_id,
                report_type=report_type,
                start_time=start_time,
                end_time=end_time,
                overall_score=overall_score,
                component_scores=component_scores,
                trends=all_trends,
                anomalies=relevant_anomalies,
                suggestions=suggestions,
                key_insights=key_insights
            )

        except Exception as e:
            logger.error(f"Comprehensive report generation failed: {e}")
            raise MonitoringException(f"Report generation failed: {e}")

    def _calculate_component_score(self, metrics_list: List[PerformanceMetrics]) -> float:
        """计算组件评分"""
        try:
            if not metrics_list:
                return 0.0

            scores = []
            for metrics in metrics_list:
                # 各指标评分（0-1）
                cpu_score = max(0, 1 - metrics.cpu_usage)
                memory_score = max(0, 1 - metrics.memory_usage)
                response_score = max(0, 1 - min(metrics.response_time / 1000, 1))  # 1秒为基准
                error_score = max(0, 1 - min(metrics.error_rate * 10, 1))  # 10%为基准
                availability_score = metrics.availability

                # 加权平均
                component_score = (
                    cpu_score * 0.2 +
                    memory_score * 0.2 +
                    response_score * 0.3 +
                    error_score * 0.2 +
                    availability_score * 0.1
                )

                scores.append(component_score)

            return np.mean(scores)

        except Exception as e:
            logger.error(f"Component score calculation failed: {e}")
            return 0.0

    def _generate_key_insights(self, component_scores: Dict[str, float],
                             trends: List[PerformanceTrend],
                             anomalies: List[PerformanceAnomaly],
                             suggestions: List[OptimizationSuggestion]) -> List[str]:
        """生成关键洞察"""
        insights = []

        try:
            # 整体性能洞察
            if component_scores:
                avg_score = np.mean(list(component_scores.values()))
                if avg_score > 0.8:
                    insights.append("系统整体性能表现优秀")
                elif avg_score > 0.6:
                    insights.append("系统整体性能表现良好")
                else:
                    insights.append("系统整体性能需要改进")

            # 趋势洞察
            declining_trends = [t for t in trends if t.direction == TrendDirection.DECLINING]
            if declining_trends:
                insights.append(f"发现{len(declining_trends)}个性能下降趋势，需要关注")

            # 异常洞察
            critical_anomalies = [a for a in anomalies if a.severity == "critical"]
            if critical_anomalies:
                insights.append(f"检测到{len(critical_anomalies)}个严重性能异常")

            # 优化建议洞察
            high_priority_suggestions = [s for s in suggestions if s.priority == "critical"]
            if high_priority_suggestions:
                insights.append(f"有{len(high_priority_suggestions)}个紧急优化建议需要立即处理")

        except Exception as e:
            logger.error(f"Key insights generation failed: {e}")

        return insights

    def _calculate_overall_health(self) -> str:
        """计算整体健康状态"""
        try:
            if not self._component_metrics:
                return "unknown"

            scores = []
            for metrics in self._component_metrics.values():
                # 简单健康评分
                health_score = (
                    (1 - metrics.cpu_usage) * 0.3 +
                    (1 - metrics.memory_usage) * 0.3 +
                    (1 - min(metrics.error_rate * 10, 1)) * 0.4
                )
                scores.append(health_score)

            avg_score = np.mean(scores)

            if avg_score > 0.8:
                return "excellent"
            elif avg_score > 0.6:
                return "good"
            elif avg_score > 0.4:
                return "average"
            elif avg_score > 0.2:
                return "poor"
            else:
                return "critical"

        except Exception:
            return "unknown"

    async def _analysis_loop(self) -> None:
        """分析循环"""
        while self._is_running:
            try:
                # 执行趋势分析
                await self.analyze_performance_trends()

                await asyncio.sleep(300)  # 每5分钟分析一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Performance analysis loop error: {e}")
                await asyncio.sleep(300)

    async def _prediction_loop(self) -> None:
        """预测循环"""
        while self._is_running:
            try:
                # 生成优化建议
                await self.generate_optimization_suggestions()

                await asyncio.sleep(1800)  # 每30分钟生成一次建议
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Performance prediction loop error: {e}")
                await asyncio.sleep(1800)

    async def _reporting_loop(self) -> None:
        """报告循环"""
        while self._is_running:
            try:
                # 生成日报
                await self.generate_performance_report("daily")

                await asyncio.sleep(86400)  # 每天生成一次报告
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Performance reporting loop error: {e}")
                await asyncio.sleep(86400)
