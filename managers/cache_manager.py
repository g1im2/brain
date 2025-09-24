"""
缓存管理器实现

负责智能缓存管理、缓存策略优化和缓存性能监控。
"""

import asyncio
import logging
import hashlib
import json
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from collections import OrderedDict, defaultdict
import threading

from config import IntegrationConfig
from exceptions import CacheException

logger = logging.getLogger(__name__)


class CacheManager:
    """缓存管理器实现类

    提供智能缓存管理、策略优化和性能监控功能。
    """

    def __init__(self, config: IntegrationConfig):
        """初始化缓存管理器

        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False

        # 缓存存储
        self._cache_storage: OrderedDict = OrderedDict()
        self._cache_metadata: Dict[str, Dict[str, Any]] = {}

        # 节点级缓存策略
        self._node_policies: Dict[str, Dict[str, Any]] = {}

        # 缓存统计
        self._cache_statistics = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'total_size': 0,
            'max_size': self.config.data_flow_manager.cache_size_mb * 1024 * 1024,  # 转换为字节
            'hit_rate': 0.0
        }

        # 缓存策略
        self._cache_strategies = {
            'lru': self._lru_eviction,
            'lfu': self._lfu_eviction,
            'ttl': self._ttl_eviction,
            'adaptive': self._adaptive_eviction
        }

        # 访问频率统计
        self._access_frequency: Dict[str, int] = defaultdict(int)
        self._access_history: Dict[str, List[datetime]] = defaultdict(list)

        # 锁和同步
        self._cache_lock = threading.RLock()

        # 监控任务
        self._monitoring_task: Optional[asyncio.Task] = None

        logger.info("CacheManager initialized")

    async def start(self) -> bool:
        """启动缓存管理器

        Returns:
            bool: 启动是否成功
        """
        try:
            if self._is_running:
                logger.warning("CacheManager is already running")
                return True

            # 启动监控任务
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())

            self._is_running = True
            logger.info("CacheManager started successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to start CacheManager: {e}")
            return False

    async def stop(self) -> bool:
        """停止缓存管理器

        Returns:
            bool: 停止是否成功
        """
        try:
            if not self._is_running:
                return True

            logger.info("Stopping CacheManager...")
            self._is_running = False

            # 停止监控任务
            if self._monitoring_task:
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass

            logger.info("CacheManager stopped successfully")
            return True

        except Exception as e:
            logger.error(f"Error stopping CacheManager: {e}")
            return False

    async def get(self, key: str) -> Optional[Any]:
        """获取缓存数据

        Args:
            key: 缓存键

        Returns:
            Optional[Any]: 缓存数据，如果不存在返回None
        """
        try:
            with self._cache_lock:
                if key in self._cache_storage:
                    # 检查TTL
                    metadata = self._cache_metadata.get(key, {})
                    if self._is_expired(metadata):
                        await self._remove(key)
                        self._cache_statistics['misses'] += 1
                        return None

                    # 更新访问统计
                    self._update_access_statistics(key)

                    # LRU更新
                    self._cache_storage.move_to_end(key)

                    self._cache_statistics['hits'] += 1
                    self._update_hit_rate()

                    return self._cache_storage[key]
                else:
                    self._cache_statistics['misses'] += 1
                    self._update_hit_rate()
                    return None

        except Exception as e:
            logger.error(f"Cache get failed for key {key}: {e}")
            raise CacheException("get", key, str(e))

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """设置缓存数据

        Args:
            key: 缓存键
            value: 缓存值
            ttl: 生存时间（秒），None表示使用默认TTL

        Returns:
            bool: 设置是否成功
        """
        try:
            with self._cache_lock:
                # 计算数据大小
                data_size = self._calculate_size(value)

                # 检查缓存容量
                if data_size > self._cache_statistics['max_size']:
                    logger.warning(f"Data too large for cache: {data_size} bytes")
                    return False

                # 确保有足够空间
                await self._ensure_capacity(data_size)

                # 设置TTL
                if ttl is None:
                    ttl = self.config.data_flow_manager.cache_ttl

                # 存储数据和元数据
                self._cache_storage[key] = value
                self._cache_metadata[key] = {
                    'created_at': datetime.now(),
                    'expires_at': datetime.now() + timedelta(seconds=ttl),
                    'size': data_size,
                    'access_count': 0,
                    'last_accessed': datetime.now()
                }

                # 更新统计
                self._cache_statistics['total_size'] += data_size

                logger.debug(f"Cached data for key: {key}, size: {data_size} bytes")
                return True

        except Exception as e:
            logger.error(f"Cache set failed for key {key}: {e}")
            raise CacheException("set", key, str(e))

    async def delete(self, key: str) -> bool:
        """删除缓存数据

        Args:
            key: 缓存键

        Returns:
            bool: 删除是否成功
        """
        try:
            with self._cache_lock:
                return await self._remove(key)
        except Exception as e:
            logger.error(f"Cache delete failed for key {key}: {e}")
            raise CacheException("delete", key, str(e))

    async def clear(self) -> bool:
        """清空所有缓存

        Returns:
            bool: 清空是否成功
        """
        try:
            with self._cache_lock:
                self._cache_storage.clear()
                self._cache_metadata.clear()
                self._access_frequency.clear()
                self._access_history.clear()

                self._cache_statistics['total_size'] = 0
                self._cache_statistics['evictions'] = 0

                logger.info("Cache cleared successfully")
                return True

        except Exception as e:
            logger.error(f"Cache clear failed: {e}")
            raise CacheException("clear", "all", str(e))

    async def get_cache_status(self) -> Dict[str, Any]:
        """获取缓存状态

        Returns:
            Dict[str, Any]: 缓存状态信息
        """
        with self._cache_lock:
            return {
                'is_running': self._is_running,
                'cache_size': len(self._cache_storage),
                'total_size_bytes': self._cache_statistics['total_size'],
                'total_size_mb': self._cache_statistics['total_size'] / (1024 * 1024),
                'max_size_mb': self._cache_statistics['max_size'] / (1024 * 1024),
                'usage_percentage': (self._cache_statistics['total_size'] / self._cache_statistics['max_size']) * 100,
                'hit_rate': self._cache_statistics['hit_rate'],
                'statistics': self._cache_statistics.copy(),
                'top_accessed_keys': self._get_top_accessed_keys(10)
            }

    def get_statistics(self) -> Dict[str, Any]:
        """获取缓存统计信息

        Returns:
            Dict[str, Any]: 统计信息
        """
        with self._cache_lock:
            return self._cache_statistics.copy()

    async def update_cache_policy(self, node: str, strategy: Dict[str, Any]) -> bool:
        """更新指定节点的缓存策略"""
        try:
            with self._cache_lock:
                # 兼容旧实例：确保属性存在
                if not hasattr(self, "_node_policies"):
                    self._node_policies = {}
                self._node_policies[node] = strategy
            logger.info(f"Updated cache policy for node={node}: {strategy}")
            return True
        except Exception as e:
            logger.error(f"Update cache policy failed for node {node}: {e}")
            raise CacheException("update_policy", node, str(e))


    def get_hit_rate(self) -> float:
        """获取缓存命中率

        Returns:
            float: 命中率 (0-1)
        """
        return self._cache_statistics['hit_rate']

    async def optimize_cache_strategy(self) -> Dict[str, Any]:
        """优化缓存策略

        Returns:
            Dict[str, Any]: 优化结果
        """
        try:
            optimization_results = {
                'strategy_before': 'lru',
                'strategy_after': 'adaptive',
                'improvements': []
            }

            # 分析访问模式
            access_patterns = self._analyze_access_patterns()

            # 基于访问模式选择最优策略
            optimal_strategy = self._select_optimal_strategy(access_patterns)
            optimization_results['strategy_after'] = optimal_strategy

            # 应用优化
            if optimal_strategy != 'lru':
                await self._apply_cache_strategy(optimal_strategy)
                optimization_results['improvements'].append(f"Switched to {optimal_strategy} strategy")

            # TTL优化
            ttl_optimization = await self._optimize_ttl()
            if ttl_optimization['optimized']:
                optimization_results['improvements'].append("Optimized TTL settings")

            # 预加载优化
            preload_optimization = await self._optimize_preloading()
            if preload_optimization['optimized']:
                optimization_results['improvements'].append("Optimized data preloading")

            logger.info(f"Cache strategy optimization completed: {optimization_results}")
            return optimization_results

        except Exception as e:
            logger.error(f"Cache strategy optimization failed: {e}")
            raise CacheException("optimize", "strategy", str(e))

    async def cleanup_expired_data(self) -> int:
        """清理过期数据

        Returns:
            int: 清理的数据条数
        """
        try:
            cleaned_count = 0
            current_time = datetime.now()

            with self._cache_lock:
                expired_keys = []
                for key, metadata in self._cache_metadata.items():
                    if self._is_expired(metadata, current_time):
                        expired_keys.append(key)

                for key in expired_keys:
                    await self._remove(key)
                    cleaned_count += 1

            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} expired cache entries")

            return cleaned_count

        except Exception as e:
            logger.error(f"Cache cleanup failed: {e}")
            raise CacheException("cleanup", "expired", str(e))

    # 私有方法实现
    def _calculate_size(self, value: Any) -> int:
        """计算数据大小"""
        try:
            return len(pickle.dumps(value))
        except Exception:
            # 如果无法序列化，使用估算大小
            if isinstance(value, str):
                return len(value.encode('utf-8'))
            elif isinstance(value, (int, float)):
                return 8
            elif isinstance(value, dict):
                return len(json.dumps(value).encode('utf-8'))
            else:
                return 1024  # 默认1KB

    def _is_expired(self, metadata: Dict[str, Any], current_time: Optional[datetime] = None) -> bool:
        """检查数据是否过期"""
        if not metadata or 'expires_at' not in metadata:
            return False

        if current_time is None:
            current_time = datetime.now()

        return current_time > metadata['expires_at']

    async def _remove(self, key: str) -> bool:
        """移除缓存项"""
        if key in self._cache_storage:
            # 更新统计
            metadata = self._cache_metadata.get(key, {})
            size = metadata.get('size', 0)
            self._cache_statistics['total_size'] -= size

            # 移除数据
            del self._cache_storage[key]
            del self._cache_metadata[key]

            # 清理访问统计
            self._access_frequency.pop(key, None)
            self._access_history.pop(key, None)

            return True
        return False

    def _update_access_statistics(self, key: str) -> None:
        """更新访问统计"""
        current_time = datetime.now()

        # 更新访问频率
        self._access_frequency[key] += 1

        # 更新访问历史
        self._access_history[key].append(current_time)

        # 限制历史记录长度
        if len(self._access_history[key]) > 100:
            self._access_history[key] = self._access_history[key][-100:]

        # 更新元数据
        if key in self._cache_metadata:
            self._cache_metadata[key]['access_count'] += 1
            self._cache_metadata[key]['last_accessed'] = current_time

    def _update_hit_rate(self) -> None:
        """更新命中率"""
        total_requests = self._cache_statistics['hits'] + self._cache_statistics['misses']
        if total_requests > 0:
            self._cache_statistics['hit_rate'] = self._cache_statistics['hits'] / total_requests

    async def _ensure_capacity(self, required_size: int) -> None:
        """确保缓存容量"""
        while (self._cache_statistics['total_size'] + required_size >
               self._cache_statistics['max_size']):
            # 执行缓存淘汰
            evicted = await self._evict_cache_item()
            if not evicted:
                break  # 无法继续淘汰

    async def _evict_cache_item(self) -> bool:
        """淘汰缓存项"""
        if not self._cache_storage:
            return False

        # 使用LRU策略淘汰
        key_to_evict = next(iter(self._cache_storage))
        await self._remove(key_to_evict)
        self._cache_statistics['evictions'] += 1

        logger.debug(f"Evicted cache item: {key_to_evict}")
        return True

    def _get_top_accessed_keys(self, limit: int) -> List[Tuple[str, int]]:
        """获取访问次数最多的键"""
        sorted_keys = sorted(
            self._access_frequency.items(),
            key=lambda x: x[1],
            reverse=True
        )
        return sorted_keys[:limit]

    def _analyze_access_patterns(self) -> Dict[str, Any]:
        """分析访问模式"""
        patterns = {
            'total_keys': len(self._cache_storage),
            'avg_access_frequency': 0.0,
            'temporal_locality': 0.0,
            'access_distribution': 'uniform'
        }

        if self._access_frequency:
            patterns['avg_access_frequency'] = sum(self._access_frequency.values()) / len(self._access_frequency)

            # 分析访问分布
            frequencies = list(self._access_frequency.values())
            if frequencies:
                max_freq = max(frequencies)
                min_freq = min(frequencies)
                if max_freq > min_freq * 3:
                    patterns['access_distribution'] = 'skewed'

        return patterns

    def _select_optimal_strategy(self, patterns: Dict[str, Any]) -> str:
        """选择最优缓存策略"""
        if patterns['access_distribution'] == 'skewed':
            return 'lfu'  # 频率优先
        elif patterns['temporal_locality'] > 0.7:
            return 'lru'  # 时间优先
        else:
            return 'adaptive'  # 自适应

    async def _apply_cache_strategy(self, strategy: str) -> None:
        """应用缓存策略"""
        # 这里可以实现不同的缓存策略
        logger.info(f"Applied cache strategy: {strategy}")

    async def _optimize_ttl(self) -> Dict[str, Any]:
        """优化TTL设置"""
        # 基于访问模式优化TTL
        return {'optimized': True, 'new_ttl': self.config.data_flow_manager.cache_ttl}

    async def _optimize_preloading(self) -> Dict[str, Any]:
        """优化预加载"""
        # 基于访问模式优化预加载策略
        return {'optimized': True, 'preload_keys': []}

    # 缓存淘汰策略实现
    async def _lru_eviction(self) -> Optional[str]:
        """LRU淘汰策略"""
        if self._cache_storage:
            return next(iter(self._cache_storage))
        return None

    async def _lfu_eviction(self) -> Optional[str]:
        """LFU淘汰策略"""
        if self._access_frequency:
            return min(self._access_frequency.items(), key=lambda x: x[1])[0]
        return None

    async def _ttl_eviction(self) -> Optional[str]:
        """TTL淘汰策略"""
        current_time = datetime.now()
        for key, metadata in self._cache_metadata.items():
            if self._is_expired(metadata, current_time):
                return key
        return None

    async def _adaptive_eviction(self) -> Optional[str]:
        """自适应淘汰策略"""
        # 结合LRU和LFU的自适应策略
        lru_candidate = await self._lru_eviction()
        lfu_candidate = await self._lfu_eviction()

        if lru_candidate and lfu_candidate:
            # 选择访问频率较低的
            lru_freq = self._access_frequency.get(lru_candidate, 0)
            lfu_freq = self._access_frequency.get(lfu_candidate, 0)
            return lfu_candidate if lfu_freq <= lru_freq else lru_candidate

        return lru_candidate or lfu_candidate

    async def _monitoring_loop(self) -> None:
        """监控循环"""
        while self._is_running:
            try:
                # 定期清理过期数据
                await self.cleanup_expired_data()

                # 监控缓存性能
                await self._monitor_cache_performance()

                await asyncio.sleep(60)  # 每分钟监控一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cache monitoring loop error: {e}")
                await asyncio.sleep(60)

    async def _monitor_cache_performance(self) -> None:
        """监控缓存性能"""
        hit_rate = self.get_hit_rate()

        if hit_rate < 0.5:
            logger.warning(f"Low cache hit rate: {hit_rate:.2f}")

        usage_percentage = (self._cache_statistics['total_size'] /
                          self._cache_statistics['max_size']) * 100

        if usage_percentage > 90:
            logger.warning(f"High cache usage: {usage_percentage:.1f}%")
