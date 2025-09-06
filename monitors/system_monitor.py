"""
系统监控器实现

负责监控三层系统的健康状态、性能指标和异常情况。
"""

import asyncio
import logging
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import deque, defaultdict

from ..models import SystemStatus, PerformanceMetrics, AlertInfo, SystemHealthStatus
from ..config import IntegrationConfig
from ..exceptions import MonitoringException

logger = logging.getLogger(__name__)


class SystemMonitor:
    """系统监控器实现类
    
    监控三层系统的健康状态、性能指标和异常情况，
    提供实时监控和历史数据分析功能。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化系统监控器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False
        
        # 监控数据存储
        self._performance_history: deque = deque(maxlen=1000)
        self._health_history: deque = deque(maxlen=100)
        self._alert_history: deque = deque(maxlen=500)
        
        # 当前状态
        self._current_system_status = SystemStatus(
            overall_health=SystemHealthStatus.HEALTHY,
            macro_system_status=SystemHealthStatus.HEALTHY,
            portfolio_system_status=SystemHealthStatus.HEALTHY,
            tactical_system_status=SystemHealthStatus.HEALTHY,
            data_pipeline_status=SystemHealthStatus.HEALTHY,
            last_update_time=datetime.now()
        )
        
        # 性能阈值
        self._performance_thresholds = self.config.monitoring.performance_threshold
        
        # 监控统计
        self._monitoring_statistics = {
            'total_checks': 0,
            'health_issues_detected': 0,
            'performance_alerts': 0,
            'system_restarts': 0,
            'uptime_start': datetime.now()
        }
        
        # 组件监控状态
        self._component_status = {
            'system_coordinator': {'status': 'unknown', 'last_check': None},
            'signal_router': {'status': 'unknown', 'last_check': None},
            'data_flow_manager': {'status': 'unknown', 'last_check': None},
            'macro_adapter': {'status': 'unknown', 'last_check': None},
            'portfolio_adapter': {'status': 'unknown', 'last_check': None},
            'tactical_adapter': {'status': 'unknown', 'last_check': None}
        }
        
        # 监控任务
        self._monitoring_task: Optional[asyncio.Task] = None
        self._performance_task: Optional[asyncio.Task] = None
        
        logger.info("SystemMonitor initialized")
    
    async def start(self) -> bool:
        """启动系统监控器
        
        Returns:
            bool: 启动是否成功
        """
        try:
            if self._is_running:
                logger.warning("SystemMonitor is already running")
                return True
            
            if not self.config.monitoring.enable_system_monitoring:
                logger.info("System monitoring is disabled in config")
                return True
            
            # 启动监控任务
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            
            if self.config.monitoring.enable_performance_monitoring:
                self._performance_task = asyncio.create_task(self._performance_monitoring_loop())
            
            self._is_running = True
            self._monitoring_statistics['uptime_start'] = datetime.now()
            
            logger.info("SystemMonitor started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start SystemMonitor: {e}")
            return False
    
    async def stop(self) -> bool:
        """停止系统监控器
        
        Returns:
            bool: 停止是否成功
        """
        try:
            if not self._is_running:
                return True
            
            logger.info("Stopping SystemMonitor...")
            self._is_running = False
            
            # 停止监控任务
            if self._monitoring_task:
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass
            
            if self._performance_task:
                self._performance_task.cancel()
                try:
                    await self._performance_task
                except asyncio.CancelledError:
                    pass
            
            logger.info("SystemMonitor stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping SystemMonitor: {e}")
            return False
    
    async def check_system_health(self) -> SystemStatus:
        """检查系统健康状态
        
        Returns:
            SystemStatus: 系统健康状态
        """
        try:
            current_time = datetime.now()
            
            # 检查各组件健康状态
            component_health = await self._check_all_components_health()
            
            # 更新系统状态
            self._current_system_status.macro_system_status = component_health.get('macro_adapter', SystemHealthStatus.CRITICAL)
            self._current_system_status.portfolio_system_status = component_health.get('portfolio_adapter', SystemHealthStatus.CRITICAL)
            self._current_system_status.tactical_system_status = component_health.get('tactical_adapter', SystemHealthStatus.CRITICAL)
            self._current_system_status.data_pipeline_status = component_health.get('data_flow_manager', SystemHealthStatus.CRITICAL)
            
            # 计算整体健康状态
            self._update_overall_health_status()
            
            # 更新时间戳
            self._current_system_status.last_update_time = current_time
            
            # 记录健康历史
            self._health_history.append({
                'timestamp': current_time,
                'overall_health': self._current_system_status.overall_health,
                'component_health': component_health
            })
            
            # 更新统计
            self._monitoring_statistics['total_checks'] += 1
            
            # 检查是否有健康问题
            if self._current_system_status.overall_health in [SystemHealthStatus.WARNING, SystemHealthStatus.CRITICAL]:
                self._monitoring_statistics['health_issues_detected'] += 1
            
            return self._current_system_status
            
        except Exception as e:
            logger.error(f"System health check failed: {e}")
            raise MonitoringException(f"Health check failed: {e}")
    
    async def collect_performance_metrics(self) -> PerformanceMetrics:
        """收集性能指标
        
        Returns:
            PerformanceMetrics: 性能指标
        """
        try:
            current_time = datetime.now()
            
            # 收集系统资源使用情况
            cpu_usage = psutil.cpu_percent(interval=1) / 100.0
            memory_info = psutil.virtual_memory()
            memory_usage = memory_info.percent / 100.0
            
            # 收集网络和磁盘信息
            disk_usage = psutil.disk_usage('/').percent / 100.0
            
            # 模拟应用层性能指标
            response_time = await self._measure_response_time()
            throughput = await self._measure_throughput()
            error_rate = await self._calculate_error_rate()
            availability = await self._calculate_availability()
            
            # 创建性能指标对象
            metrics = PerformanceMetrics(
                metric_id=f"metrics_{current_time.timestamp()}",
                component_name="integration_layer",
                cpu_usage=cpu_usage,
                memory_usage=memory_usage,
                response_time=response_time,
                throughput=throughput,
                error_rate=error_rate,
                availability=availability,
                additional_metrics={
                    'disk_usage': disk_usage,
                    'active_connections': self._get_active_connections(),
                    'queue_sizes': self._get_queue_sizes()
                }
            )
            
            # 记录性能历史
            self._performance_history.append(metrics)
            
            # 检查性能阈值
            await self._check_performance_thresholds(metrics)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Performance metrics collection failed: {e}")
            raise MonitoringException(f"Metrics collection failed: {e}")
    
    def get_system_status(self) -> SystemStatus:
        """获取当前系统状态
        
        Returns:
            SystemStatus: 当前系统状态
        """
        return self._current_system_status
    
    def get_monitoring_statistics(self) -> Dict[str, Any]:
        """获取监控统计信息
        
        Returns:
            Dict[str, Any]: 监控统计
        """
        current_time = datetime.now()
        uptime = (current_time - self._monitoring_statistics['uptime_start']).total_seconds()
        
        stats = self._monitoring_statistics.copy()
        stats.update({
            'uptime_seconds': uptime,
            'uptime_hours': uptime / 3600,
            'performance_history_size': len(self._performance_history),
            'health_history_size': len(self._health_history),
            'alert_history_size': len(self._alert_history),
            'component_status': self._component_status.copy()
        })
        
        return stats
    
    def get_performance_history(self, hours: int = 1) -> List[PerformanceMetrics]:
        """获取性能历史数据
        
        Args:
            hours: 获取最近几小时的数据
            
        Returns:
            List[PerformanceMetrics]: 性能历史数据
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [
            metrics for metrics in self._performance_history
            if metrics.timestamp >= cutoff_time
        ]
    
    def get_health_history(self, hours: int = 1) -> List[Dict[str, Any]]:
        """获取健康状态历史
        
        Args:
            hours: 获取最近几小时的数据
            
        Returns:
            List[Dict[str, Any]]: 健康状态历史
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [
            record for record in self._health_history
            if record['timestamp'] >= cutoff_time
        ]
    
    # 私有方法实现
    async def _monitoring_loop(self) -> None:
        """主监控循环"""
        interval = self.config.monitoring.monitoring_interval
        
        while self._is_running:
            try:
                # 执行健康检查
                await self.check_system_health()
                
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(interval)
    
    async def _performance_monitoring_loop(self) -> None:
        """性能监控循环"""
        interval = self.config.monitoring.monitoring_interval * 2  # 性能监控频率较低
        
        while self._is_running:
            try:
                # 收集性能指标
                await self.collect_performance_metrics()
                
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Performance monitoring loop error: {e}")
                await asyncio.sleep(interval)
    
    async def _check_all_components_health(self) -> Dict[str, SystemHealthStatus]:
        """检查所有组件健康状态"""
        component_health = {}
        current_time = datetime.now()
        
        for component_name in self._component_status.keys():
            try:
                # 模拟组件健康检查
                health_status = await self._check_component_health(component_name)
                component_health[component_name] = health_status
                
                # 更新组件状态
                self._component_status[component_name] = {
                    'status': health_status.value,
                    'last_check': current_time
                }
                
            except Exception as e:
                logger.warning(f"Health check failed for {component_name}: {e}")
                component_health[component_name] = SystemHealthStatus.CRITICAL
                self._component_status[component_name] = {
                    'status': 'error',
                    'last_check': current_time
                }
        
        return component_health
    
    async def _check_component_health(self, component_name: str) -> SystemHealthStatus:
        """检查单个组件健康状态"""
        # 模拟组件健康检查
        await asyncio.sleep(0.01)
        
        # 这里应该实际调用各组件的健康检查接口
        # 现在返回模拟结果
        return SystemHealthStatus.HEALTHY
    
    def _update_overall_health_status(self) -> None:
        """更新整体健康状态"""
        component_statuses = [
            self._current_system_status.macro_system_status,
            self._current_system_status.portfolio_system_status,
            self._current_system_status.tactical_system_status,
            self._current_system_status.data_pipeline_status
        ]
        
        # 如果有任何组件处于关键状态，整体状态为关键
        if any(status == SystemHealthStatus.CRITICAL for status in component_statuses):
            self._current_system_status.overall_health = SystemHealthStatus.CRITICAL
        # 如果有任何组件处于警告状态，整体状态为警告
        elif any(status == SystemHealthStatus.WARNING for status in component_statuses):
            self._current_system_status.overall_health = SystemHealthStatus.WARNING
        # 否则整体状态为健康
        else:
            self._current_system_status.overall_health = SystemHealthStatus.HEALTHY
    
    async def _measure_response_time(self) -> float:
        """测量响应时间"""
        # 模拟响应时间测量
        return 150.0  # 毫秒
    
    async def _measure_throughput(self) -> float:
        """测量吞吐量"""
        # 模拟吞吐量测量
        return 100.0  # 请求/秒
    
    async def _calculate_error_rate(self) -> float:
        """计算错误率"""
        # 模拟错误率计算
        return 0.02  # 2%
    
    async def _calculate_availability(self) -> float:
        """计算可用性"""
        # 模拟可用性计算
        return 0.999  # 99.9%
    
    def _get_active_connections(self) -> int:
        """获取活跃连接数"""
        # 模拟活跃连接数
        return 25
    
    def _get_queue_sizes(self) -> Dict[str, int]:
        """获取队列大小"""
        # 模拟队列大小
        return {
            'signal_queue': 10,
            'data_queue': 5,
            'validation_queue': 3
        }
    
    async def _check_performance_thresholds(self, metrics: PerformanceMetrics) -> None:
        """检查性能阈值"""
        alerts = []
        
        # 检查CPU使用率
        if metrics.cpu_usage > self._performance_thresholds['cpu_usage']:
            alerts.append(f"High CPU usage: {metrics.cpu_usage:.1%}")
        
        # 检查内存使用率
        if metrics.memory_usage > self._performance_thresholds['memory_usage']:
            alerts.append(f"High memory usage: {metrics.memory_usage:.1%}")
        
        # 检查响应时间
        if metrics.response_time > self._performance_thresholds['response_time']:
            alerts.append(f"High response time: {metrics.response_time:.1f}ms")
        
        # 检查错误率
        if metrics.error_rate > self._performance_thresholds['error_rate']:
            alerts.append(f"High error rate: {metrics.error_rate:.1%}")
        
        # 生成告警
        for alert_message in alerts:
            alert = AlertInfo(
                alert_id=f"perf_alert_{datetime.now().timestamp()}",
                alert_level="P2",
                alert_type="performance",
                component="integration_layer",
                message=alert_message
            )
            self._alert_history.append(alert)
            self._monitoring_statistics['performance_alerts'] += 1
            
            logger.warning(f"Performance alert: {alert_message}")
