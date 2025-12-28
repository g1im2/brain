"""
资源管理器实现

负责动态资源分配、负载均衡、容量规划和预测，
提供智能的资源管理和优化功能。
"""

import asyncio
import logging
import os
import resource
import shutil
import threading
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Any, Optional, Tuple

import numpy as np

from config import IntegrationConfig
from exceptions import IntegrationException

logger = logging.getLogger(__name__)


class ResourceType(Enum):
    """资源类型枚举"""
    CPU = "cpu"
    MEMORY = "memory"
    DISK = "disk"
    NETWORK = "network"
    CONNECTION = "connection"
    THREAD = "thread"


class AllocationStrategy(Enum):
    """分配策略枚举"""
    ROUND_ROBIN = "round_robin"
    LEAST_LOADED = "least_loaded"
    WEIGHTED = "weighted"
    PRIORITY_BASED = "priority_based"
    ADAPTIVE = "adaptive"


@dataclass
class ResourceQuota:
    """资源配额"""
    resource_type: ResourceType
    max_allocation: float
    current_allocation: float = 0.0
    reserved_allocation: float = 0.0
    warning_threshold: float = 0.8
    critical_threshold: float = 0.9
    
    @property
    def available_allocation(self) -> float:
        """可用分配量"""
        return self.max_allocation - self.current_allocation - self.reserved_allocation
    
    @property
    def utilization_rate(self) -> float:
        """利用率"""
        return self.current_allocation / self.max_allocation if self.max_allocation > 0 else 0.0


@dataclass
class ResourceRequest:
    """资源请求"""
    request_id: str
    component: str
    resource_type: ResourceType
    amount: float
    priority: int = 1
    duration: Optional[int] = None  # 秒
    created_time: datetime = field(default_factory=datetime.now)
    status: str = "pending"  # pending, allocated, denied, expired
    allocated_amount: float = 0.0
    allocation_time: Optional[datetime] = None


@dataclass
class ResourceAllocation:
    """资源分配"""
    allocation_id: str
    request_id: str
    component: str
    resource_type: ResourceType
    allocated_amount: float
    allocation_time: datetime
    expiry_time: Optional[datetime] = None
    is_active: bool = True


@dataclass
class LoadBalancingNode:
    """负载均衡节点"""
    node_id: str
    node_name: str
    capacity: Dict[ResourceType, float]
    current_load: Dict[ResourceType, float] = field(default_factory=dict)
    is_active: bool = True
    weight: float = 1.0
    health_score: float = 1.0
    last_health_check: Optional[datetime] = None
    
    def get_load_ratio(self, resource_type: ResourceType) -> float:
        """获取负载比率"""
        capacity = self.capacity.get(resource_type, 0)
        load = self.current_load.get(resource_type, 0)
        return load / capacity if capacity > 0 else 0.0


@dataclass
class CapacityPrediction:
    """容量预测"""
    resource_type: ResourceType
    prediction_horizon: int  # 小时
    predicted_demand: List[float]
    confidence_interval: Tuple[float, float]
    recommendation: str
    generated_time: datetime = field(default_factory=datetime.now)


class ResourceManager:
    """资源管理器实现类
    
    负责动态资源分配、负载均衡、容量规划和预测，
    提供智能的资源管理和优化功能。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化资源管理器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False
        
        # 资源配额管理
        self._resource_quotas: Dict[ResourceType, ResourceQuota] = {}
        self._initialize_resource_quotas()
        
        # 资源请求和分配
        self._pending_requests: deque = deque()
        self._active_allocations: Dict[str, ResourceAllocation] = {}
        self._allocation_history: deque = deque(maxlen=10000)
        
        # 负载均衡
        self._load_balancing_nodes: Dict[str, LoadBalancingNode] = {}
        self._allocation_strategy = AllocationStrategy.ADAPTIVE
        
        # 容量规划
        self._resource_usage_history: Dict[ResourceType, deque] = {
            rt: deque(maxlen=1000) for rt in ResourceType
        }
        self._capacity_predictions: Dict[ResourceType, CapacityPrediction] = {}
        
        # 资源统计
        self._resource_statistics = {
            'total_requests': 0,
            'successful_allocations': 0,
            'denied_requests': 0,
            'expired_allocations': 0,
            'average_allocation_time': 0.0,
            'resource_utilization': {}
        }
        
        # 锁和同步
        self._resource_lock = threading.RLock()
        
        # 管理任务
        self._allocation_task: Optional[asyncio.Task] = None
        self._monitoring_task: Optional[asyncio.Task] = None
        self._prediction_task: Optional[asyncio.Task] = None
        
        logger.info("ResourceManager initialized")
    
    async def start(self) -> bool:
        """启动资源管理器
        
        Returns:
            bool: 启动是否成功
        """
        try:
            if self._is_running:
                logger.warning("ResourceManager is already running")
                return True
            
            # 启动管理任务
            self._allocation_task = asyncio.create_task(self._allocation_loop())
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            self._prediction_task = asyncio.create_task(self._prediction_loop())
            
            self._is_running = True
            logger.info("ResourceManager started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start ResourceManager: {e}")
            return False
    
    async def stop(self) -> bool:
        """停止资源管理器
        
        Returns:
            bool: 停止是否成功
        """
        try:
            if not self._is_running:
                return True
            
            logger.info("Stopping ResourceManager...")
            self._is_running = False
            
            # 停止管理任务
            tasks = [self._allocation_task, self._monitoring_task, self._prediction_task]
            for task in tasks:
                if task:
                    task.cancel()
            
            await asyncio.gather(*[t for t in tasks if t], return_exceptions=True)
            
            # 释放所有活跃分配
            await self._release_all_allocations()
            
            logger.info("ResourceManager stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping ResourceManager: {e}")
            return False
    
    async def request_resource(self, component: str, resource_type: ResourceType,
                             amount: float, priority: int = 1,
                             duration: Optional[int] = None) -> str:
        """请求资源
        
        Args:
            component: 组件名称
            resource_type: 资源类型
            amount: 请求数量
            priority: 优先级
            duration: 持续时间（秒）
            
        Returns:
            str: 请求ID
        """
        try:
            request_id = str(uuid.uuid4())
            
            request = ResourceRequest(
                request_id=request_id,
                component=component,
                resource_type=resource_type,
                amount=amount,
                priority=priority,
                duration=duration
            )
            
            with self._resource_lock:
                self._pending_requests.append(request)
                self._resource_statistics['total_requests'] += 1
            
            logger.info(f"Resource request created: {request_id} for {component}")
            return request_id
            
        except Exception as e:
            logger.error(f"Failed to create resource request: {e}")
            raise IntegrationException(f"Resource request failed: {e}")
    
    async def release_resource(self, allocation_id: str) -> bool:
        """释放资源
        
        Args:
            allocation_id: 分配ID
            
        Returns:
            bool: 释放是否成功
        """
        try:
            with self._resource_lock:
                if allocation_id not in self._active_allocations:
                    return False
                
                allocation = self._active_allocations[allocation_id]
                
                # 更新配额
                quota = self._resource_quotas.get(allocation.resource_type)
                if quota:
                    quota.current_allocation -= allocation.allocated_amount
                
                # 标记为非活跃
                allocation.is_active = False
                
                # 移动到历史记录
                self._allocation_history.append(allocation)
                del self._active_allocations[allocation_id]
            
            logger.info(f"Resource released: {allocation_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to release resource {allocation_id}: {e}")
            return False
    
    def register_load_balancing_node(self, node: LoadBalancingNode) -> bool:
        """注册负载均衡节点
        
        Args:
            node: 负载均衡节点
            
        Returns:
            bool: 注册是否成功
        """
        try:
            with self._resource_lock:
                self._load_balancing_nodes[node.node_id] = node
            
            logger.info(f"Load balancing node registered: {node.node_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register node {node.node_id}: {e}")
            return False
    
    def unregister_load_balancing_node(self, node_id: str) -> bool:
        """注销负载均衡节点
        
        Args:
            node_id: 节点ID
            
        Returns:
            bool: 注销是否成功
        """
        try:
            with self._resource_lock:
                if node_id in self._load_balancing_nodes:
                    del self._load_balancing_nodes[node_id]
                    logger.info(f"Load balancing node unregistered: {node_id}")
                    return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to unregister node {node_id}: {e}")
            return False
    
    async def select_optimal_node(self, resource_type: ResourceType,
                                amount: float) -> Optional[str]:
        """选择最优节点
        
        Args:
            resource_type: 资源类型
            amount: 资源数量
            
        Returns:
            Optional[str]: 最优节点ID
        """
        try:
            with self._resource_lock:
                available_nodes = [
                    node for node in self._load_balancing_nodes.values()
                    if (node.is_active and 
                        node.capacity.get(resource_type, 0) >= 
                        node.current_load.get(resource_type, 0) + amount)
                ]
            
            if not available_nodes:
                return None
            
            # 根据策略选择节点
            if self._allocation_strategy == AllocationStrategy.LEAST_LOADED:
                selected_node = min(available_nodes, 
                                  key=lambda n: n.get_load_ratio(resource_type))
            elif self._allocation_strategy == AllocationStrategy.WEIGHTED:
                selected_node = max(available_nodes, 
                                  key=lambda n: n.weight * n.health_score)
            elif self._allocation_strategy == AllocationStrategy.ROUND_ROBIN:
                # 简化的轮询实现
                selected_node = available_nodes[0]
            else:  # ADAPTIVE
                selected_node = await self._adaptive_node_selection(available_nodes, resource_type)
            
            return selected_node.node_id if selected_node else None
            
        except Exception as e:
            logger.error(f"Node selection failed: {e}")
            return None
    
    async def predict_capacity_demand(self, resource_type: ResourceType,
                                    prediction_hours: int = 24) -> Optional[CapacityPrediction]:
        """预测容量需求
        
        Args:
            resource_type: 资源类型
            prediction_hours: 预测时间范围（小时）
            
        Returns:
            Optional[CapacityPrediction]: 容量预测
        """
        try:
            usage_history = self._resource_usage_history.get(resource_type)
            if not usage_history or len(usage_history) < 24:
                return None
            
            # 提取历史使用数据
            historical_usage = list(usage_history)
            
            # 简单的线性预测（实际应用中可以使用更复杂的模型）
            recent_trend = np.mean(historical_usage[-12:]) - np.mean(historical_usage[-24:-12])
            base_demand = np.mean(historical_usage[-6:])
            
            # 生成预测值
            predicted_demand = []
            for hour in range(prediction_hours):
                predicted_value = base_demand + recent_trend * hour
                predicted_demand.append(max(0, predicted_value))
            
            # 计算置信区间（简化）
            std_dev = np.std(historical_usage[-24:])
            confidence_lower = max(0, base_demand - 2 * std_dev)
            confidence_upper = base_demand + 2 * std_dev
            
            # 生成建议
            max_predicted = max(predicted_demand)
            current_capacity = self._resource_quotas.get(resource_type, ResourceQuota(resource_type, 0)).max_allocation
            
            if max_predicted > current_capacity * 0.9:
                recommendation = "建议增加容量以应对预期需求增长"
            elif max_predicted < current_capacity * 0.5:
                recommendation = "当前容量充足，可以考虑优化资源配置"
            else:
                recommendation = "当前容量基本满足预期需求"
            
            prediction = CapacityPrediction(
                resource_type=resource_type,
                prediction_horizon=prediction_hours,
                predicted_demand=predicted_demand,
                confidence_interval=(confidence_lower, confidence_upper),
                recommendation=recommendation
            )
            
            # 缓存预测结果
            self._capacity_predictions[resource_type] = prediction
            
            return prediction
            
        except Exception as e:
            logger.error(f"Capacity prediction failed: {e}")
            return None
    
    def get_resource_status(self) -> Dict[str, Any]:
        """获取资源状态
        
        Returns:
            Dict[str, Any]: 资源状态信息
        """
        with self._resource_lock:
            status = {
                'resource_quotas': {},
                'active_allocations': len(self._active_allocations),
                'pending_requests': len(self._pending_requests),
                'load_balancing_nodes': len(self._load_balancing_nodes),
                'statistics': self._resource_statistics.copy()
            }
            
            # 资源配额状态
            for resource_type, quota in self._resource_quotas.items():
                status['resource_quotas'][resource_type.value] = {
                    'max_allocation': quota.max_allocation,
                    'current_allocation': quota.current_allocation,
                    'available_allocation': quota.available_allocation,
                    'utilization_rate': quota.utilization_rate,
                    'warning_threshold': quota.warning_threshold,
                    'critical_threshold': quota.critical_threshold
                }
            
            return status
    
    def get_resource_statistics(self) -> Dict[str, Any]:
        """获取资源统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        with self._resource_lock:
            return self._resource_statistics.copy()

    # 私有方法实现
    def _initialize_resource_quotas(self) -> None:
        """初始化资源配额"""
        cpu_count, memory_total, disk_total = self._get_system_capacity()

        # 设置资源配额
        self._resource_quotas = {
            ResourceType.CPU: ResourceQuota(
                resource_type=ResourceType.CPU,
                max_allocation=cpu_count * 0.8,  # 保留20%
                warning_threshold=0.7,
                critical_threshold=0.9
            ),
            ResourceType.MEMORY: ResourceQuota(
                resource_type=ResourceType.MEMORY,
                max_allocation=memory_total * 0.8,  # 保留20%
                warning_threshold=0.7,
                critical_threshold=0.9
            ),
            ResourceType.DISK: ResourceQuota(
                resource_type=ResourceType.DISK,
                max_allocation=disk_total * 0.8,  # 保留20%
                warning_threshold=0.8,
                critical_threshold=0.95
            ),
            ResourceType.CONNECTION: ResourceQuota(
                resource_type=ResourceType.CONNECTION,
                max_allocation=1000,  # 最大连接数
                warning_threshold=0.8,
                critical_threshold=0.95
            ),
            ResourceType.THREAD: ResourceQuota(
                resource_type=ResourceType.THREAD,
                max_allocation=200,  # 最大线程数
                warning_threshold=0.8,
                critical_threshold=0.9
            )
        }

    async def _allocation_loop(self) -> None:
        """资源分配循环"""
        while self._is_running:
            try:
                await self._process_pending_requests()
                await self._check_allocation_expiry()
                await asyncio.sleep(1)  # 每秒处理一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Resource allocation loop error: {e}")
                await asyncio.sleep(1)

    async def _process_pending_requests(self) -> None:
        """处理待分配请求"""
        try:
            with self._resource_lock:
                if not self._pending_requests:
                    return

                # 按优先级排序
                sorted_requests = sorted(self._pending_requests, key=lambda r: r.priority, reverse=True)
                self._pending_requests.clear()

                for request in sorted_requests:
                    success = await self._try_allocate_resource(request)
                    if not success:
                        # 如果分配失败，重新加入队列（降低优先级）
                        request.priority = max(1, request.priority - 1)
                        self._pending_requests.append(request)

        except Exception as e:
            logger.error(f"Failed to process pending requests: {e}")

    async def _try_allocate_resource(self, request: ResourceRequest) -> bool:
        """尝试分配资源"""
        try:
            quota = self._resource_quotas.get(request.resource_type)
            if not quota:
                request.status = "denied"
                self._resource_statistics['denied_requests'] += 1
                return False

            # 检查是否有足够资源
            if quota.available_allocation < request.amount:
                return False  # 资源不足，稍后重试

            # 分配资源
            allocation_id = str(uuid.uuid4())
            allocation_time = datetime.now()
            expiry_time = None

            if request.duration:
                expiry_time = allocation_time + timedelta(seconds=request.duration)

            allocation = ResourceAllocation(
                allocation_id=allocation_id,
                request_id=request.request_id,
                component=request.component,
                resource_type=request.resource_type,
                allocated_amount=request.amount,
                allocation_time=allocation_time,
                expiry_time=expiry_time
            )

            # 更新配额
            quota.current_allocation += request.amount

            # 存储分配
            self._active_allocations[allocation_id] = allocation

            # 更新请求状态
            request.status = "allocated"
            request.allocated_amount = request.amount
            request.allocation_time = allocation_time

            # 更新统计
            self._resource_statistics['successful_allocations'] += 1

            logger.info(f"Resource allocated: {allocation_id} for {request.component}")
            return True

        except Exception as e:
            logger.error(f"Resource allocation failed: {e}")
            request.status = "denied"
            self._resource_statistics['denied_requests'] += 1
            return False

    async def _check_allocation_expiry(self) -> None:
        """检查分配过期"""
        try:
            current_time = datetime.now()
            expired_allocations = []

            with self._resource_lock:
                for allocation_id, allocation in self._active_allocations.items():
                    if (allocation.expiry_time and
                        current_time > allocation.expiry_time):
                        expired_allocations.append(allocation_id)

            # 释放过期分配
            for allocation_id in expired_allocations:
                await self.release_resource(allocation_id)
                self._resource_statistics['expired_allocations'] += 1
                logger.info(f"Resource allocation expired: {allocation_id}")

        except Exception as e:
            logger.error(f"Allocation expiry check failed: {e}")

    async def _adaptive_node_selection(self, nodes: List[LoadBalancingNode],
                                     resource_type: ResourceType) -> Optional[LoadBalancingNode]:
        """自适应节点选择"""
        try:
            if not nodes:
                return None

            # 计算综合评分
            best_node = None
            best_score = -1

            for node in nodes:
                # 负载评分（负载越低评分越高）
                load_ratio = node.get_load_ratio(resource_type)
                load_score = 1.0 - load_ratio

                # 健康评分
                health_score = node.health_score

                # 权重评分
                weight_score = node.weight

                # 综合评分
                total_score = (load_score * 0.5 +
                             health_score * 0.3 +
                             weight_score * 0.2)

                if total_score > best_score:
                    best_score = total_score
                    best_node = node

            return best_node

        except Exception as e:
            logger.error(f"Adaptive node selection failed: {e}")
            return nodes[0] if nodes else None

    async def _release_all_allocations(self) -> None:
        """释放所有分配"""
        try:
            with self._resource_lock:
                allocation_ids = list(self._active_allocations.keys())

            for allocation_id in allocation_ids:
                await self.release_resource(allocation_id)

            logger.info(f"Released {len(allocation_ids)} active allocations")

        except Exception as e:
            logger.error(f"Failed to release all allocations: {e}")

    async def _monitoring_loop(self) -> None:
        """监控循环"""
        while self._is_running:
            try:
                # 收集资源使用数据
                await self._collect_resource_usage()

                # 检查资源阈值
                await self._check_resource_thresholds()

                # 更新节点健康状态
                await self._update_node_health()

                await asyncio.sleep(60)  # 每分钟监控一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Resource monitoring loop error: {e}")
                await asyncio.sleep(60)

    async def _collect_resource_usage(self) -> None:
        """收集资源使用数据"""
        try:
            current_time = datetime.now()

            cpu_usage, memory_usage, disk_usage = self._get_system_usage()

            # 记录使用历史
            self._resource_usage_history[ResourceType.CPU].append(cpu_usage)
            self._resource_usage_history[ResourceType.MEMORY].append(memory_usage)
            self._resource_usage_history[ResourceType.DISK].append(disk_usage)

            # 更新统计
            self._resource_statistics['resource_utilization'] = {
                'cpu': cpu_usage,
                'memory': memory_usage,
                'disk': disk_usage,
                'timestamp': current_time.isoformat()
            }

        except Exception as e:
            logger.error(f"Resource usage collection failed: {e}")

    def _get_system_capacity(self) -> tuple[int, float, float]:
        """获取系统容量（CPU核数/内存GB/磁盘GB）"""
        cpu_count = os.cpu_count() or 1
        memory_total_gb = 0.0
        disk_total_gb = 0.0

        if hasattr(os, "sysconf"):
            try:
                page_size = os.sysconf("SC_PAGE_SIZE")
                phys_pages = os.sysconf("SC_PHYS_PAGES")
                memory_total_gb = (float(page_size) * float(phys_pages)) / (1024 ** 3)
            except (ValueError, OSError):
                memory_total_gb = 0.0

        try:
            usage = shutil.disk_usage("/")
            disk_total_gb = usage.total / (1024 ** 3)
        except Exception:
            disk_total_gb = 0.0

        return cpu_count, memory_total_gb or 1.0, disk_total_gb or 1.0

    def _get_system_usage(self) -> tuple[float, float, float]:
        """获取系统使用率（CPU/内存/磁盘）"""
        cpu_usage = 0.0
        memory_usage = 0.0
        disk_usage = 0.0

        cpu_count = os.cpu_count() or 1
        if hasattr(os, "getloadavg"):
            load1, _, _ = os.getloadavg()
            cpu_usage = min(1.0, float(load1) / float(cpu_count))

        total_mem = None
        if hasattr(os, "sysconf"):
            try:
                page_size = os.sysconf("SC_PAGE_SIZE")
                phys_pages = os.sysconf("SC_PHYS_PAGES")
                total_mem = float(page_size) * float(phys_pages)
            except (ValueError, OSError):
                total_mem = None

        try:
            rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            rss_bytes = float(rss_kb) * 1024.0
            if total_mem and total_mem > 0:
                memory_usage = min(1.0, rss_bytes / total_mem)
        except Exception:
            pass

        try:
            usage = shutil.disk_usage("/")
            if usage.total > 0:
                disk_usage = (usage.total - usage.free) / usage.total
        except Exception:
            pass

        return cpu_usage, memory_usage, disk_usage

    async def _check_resource_thresholds(self) -> None:
        """检查资源阈值"""
        try:
            for resource_type, quota in self._resource_quotas.items():
                utilization = quota.utilization_rate

                if utilization > quota.critical_threshold:
                    logger.critical(f"Critical resource usage: {resource_type.value} at {utilization:.1%}")
                elif utilization > quota.warning_threshold:
                    logger.warning(f"High resource usage: {resource_type.value} at {utilization:.1%}")

        except Exception as e:
            logger.error(f"Resource threshold check failed: {e}")

    async def _update_node_health(self) -> None:
        """更新节点健康状态"""
        try:
            current_time = datetime.now()

            with self._resource_lock:
                for node in self._load_balancing_nodes.values():
                    # 简化的健康检查（实际应用中应该ping节点）
                    if node.last_health_check:
                        time_since_check = (current_time - node.last_health_check).total_seconds()
                        if time_since_check > 300:  # 5分钟没有更新
                            node.health_score = max(0.1, node.health_score - 0.1)

                    node.last_health_check = current_time

        except Exception as e:
            logger.error(f"Node health update failed: {e}")

    async def _prediction_loop(self) -> None:
        """预测循环"""
        while self._is_running:
            try:
                # 为每种资源类型生成容量预测
                for resource_type in ResourceType:
                    await self.predict_capacity_demand(resource_type)

                await asyncio.sleep(3600)  # 每小时预测一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Resource prediction loop error: {e}")
                await asyncio.sleep(3600)
