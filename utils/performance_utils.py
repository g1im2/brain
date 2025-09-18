"""
性能监控工具

提供性能监控、缓存、限流等功能。
"""

import asyncio
import functools
import logging
import time
import weakref
from typing import Any, Callable, Dict, Optional, TypeVar, Union
from datetime import datetime, timedelta
import psutil
import threading
from collections import defaultdict, OrderedDict

logger = logging.getLogger(__name__)

T = TypeVar('T')


def measure_execution_time(
    log_level: int = logging.INFO,
    include_args: bool = False,
    threshold_ms: Optional[float] = None
):
    """
    测量函数执行时间的装饰器
    
    Args:
        log_level: 日志级别
        include_args: 是否包含参数信息
        threshold_ms: 只记录超过阈值的执行时间
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                end_time = time.perf_counter()
                execution_time_ms = (end_time - start_time) * 1000
                
                if threshold_ms is None or execution_time_ms >= threshold_ms:
                    log_message = f"{func.__name__} executed in {execution_time_ms:.2f}ms"
                    if include_args:
                        log_message += f" with args={args}, kwargs={kwargs}"
                    logger.log(log_level, log_message)
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                end_time = time.perf_counter()
                execution_time_ms = (end_time - start_time) * 1000
                
                if threshold_ms is None or execution_time_ms >= threshold_ms:
                    log_message = f"{func.__name__} executed in {execution_time_ms:.2f}ms"
                    if include_args:
                        log_message += f" with args={args}, kwargs={kwargs}"
                    logger.log(log_level, log_message)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


class MemoryProfiler:
    """内存使用监控器"""
    
    def __init__(self, name: str = "operation"):
        self.name = name
        self.start_memory = 0
        self.end_memory = 0
    
    def __enter__(self):
        self.start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        memory_diff = self.end_memory - self.start_memory
        logger.info(f"{self.name} memory usage: {memory_diff:.2f}MB")
    
    @property
    def memory_used(self) -> float:
        """获取内存使用量（MB）"""
        return self.end_memory - self.start_memory


def memory_profiler(name: str = "operation"):
    """内存监控装饰器"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            with MemoryProfiler(f"{name}:{func.__name__}"):
                return await func(*args, **kwargs)
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            with MemoryProfiler(f"{name}:{func.__name__}"):
                return func(*args, **kwargs)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


class AsyncLRUCache:
    """异步LRU缓存"""
    
    def __init__(self, maxsize: int = 128, ttl: Optional[float] = None):
        self.maxsize = maxsize
        self.ttl = ttl
        self.cache: OrderedDict = OrderedDict()
        self.timestamps: Dict[str, float] = {}
        self.lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[Any]:
        """获取缓存值"""
        async with self.lock:
            if key not in self.cache:
                return None
            
            # 检查TTL
            if self.ttl and time.time() - self.timestamps[key] > self.ttl:
                del self.cache[key]
                del self.timestamps[key]
                return None
            
            # 移动到末尾（最近使用）
            self.cache.move_to_end(key)
            return self.cache[key]
    
    async def set(self, key: str, value: Any) -> None:
        """设置缓存值"""
        async with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            else:
                if len(self.cache) >= self.maxsize:
                    # 删除最久未使用的项
                    oldest_key = next(iter(self.cache))
                    del self.cache[oldest_key]
                    del self.timestamps[oldest_key]
            
            self.cache[key] = value
            self.timestamps[key] = time.time()
    
    async def clear(self) -> None:
        """清空缓存"""
        async with self.lock:
            self.cache.clear()
            self.timestamps.clear()


def async_cache(maxsize: int = 128, ttl: Optional[float] = None):
    """异步缓存装饰器"""
    def decorator(func: Callable) -> Callable:
        cache = AsyncLRUCache(maxsize, ttl)
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # 生成缓存键
            key = f"{func.__name__}:{hash((args, tuple(sorted(kwargs.items()))))}"
            
            # 尝试从缓存获取
            cached_result = await cache.get(key)
            if cached_result is not None:
                return cached_result
            
            # 执行函数并缓存结果
            result = await func(*args, **kwargs)
            await cache.set(key, result)
            return result
        
        # 添加缓存管理方法
        wrapper.cache_clear = cache.clear
        wrapper.cache_info = lambda: {
            'size': len(cache.cache),
            'maxsize': cache.maxsize,
            'ttl': cache.ttl
        }
        
        return wrapper
    return decorator


class RateLimiter:
    """速率限制器"""
    
    def __init__(self, max_calls: int, time_window: float):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls: Dict[str, list] = defaultdict(list)
        self.lock = threading.Lock()
    
    def is_allowed(self, key: str = "default") -> bool:
        """检查是否允许调用"""
        with self.lock:
            now = time.time()
            # 清理过期的调用记录
            self.calls[key] = [
                call_time for call_time in self.calls[key]
                if now - call_time < self.time_window
            ]
            
            # 检查是否超过限制
            if len(self.calls[key]) >= self.max_calls:
                return False
            
            # 记录本次调用
            self.calls[key].append(now)
            return True
    
    async def wait_if_needed(self, key: str = "default") -> None:
        """如果需要则等待"""
        if not self.is_allowed(key):
            # 计算需要等待的时间
            with self.lock:
                if self.calls[key]:
                    oldest_call = min(self.calls[key])
                    wait_time = self.time_window - (time.time() - oldest_call)
                    if wait_time > 0:
                        await asyncio.sleep(wait_time)


def rate_limiter(max_calls: int, time_window: float, key_func: Optional[Callable] = None):
    """速率限制装饰器"""
    limiter = RateLimiter(max_calls, time_window)
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            key = key_func(*args, **kwargs) if key_func else "default"
            await limiter.wait_if_needed(key)
            return await func(*args, **kwargs)
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            key = key_func(*args, **kwargs) if key_func else "default"
            # 同步版本只检查不等待
            if not limiter.is_allowed(key):
                raise RuntimeError("Rate limit exceeded")
            return func(*args, **kwargs)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self):
        self.metrics: Dict[str, list] = defaultdict(list)
        self.lock = threading.Lock()
    
    def record_metric(self, name: str, value: float, timestamp: Optional[float] = None):
        """记录性能指标"""
        with self.lock:
            self.metrics[name].append({
                'value': value,
                'timestamp': timestamp or time.time()
            })
    
    def get_stats(self, name: str, time_window: Optional[float] = None) -> Dict[str, float]:
        """获取统计信息"""
        with self.lock:
            values = self.metrics.get(name, [])
            
            if time_window:
                now = time.time()
                values = [
                    v for v in values 
                    if now - v['timestamp'] <= time_window
                ]
            
            if not values:
                return {}
            
            values_only = [v['value'] for v in values]
            return {
                'count': len(values_only),
                'min': min(values_only),
                'max': max(values_only),
                'avg': sum(values_only) / len(values_only),
                'latest': values_only[-1]
            }


# 全局性能监控器实例
performance_monitor = PerformanceMonitor()
