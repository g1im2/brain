"""
异步工具函数

提供异步编程相关的工具函数和装饰器。
"""

import asyncio
import functools
import logging
from typing import Any, Awaitable, Callable, List, Optional, TypeVar, Union
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

T = TypeVar('T')


async def run_with_timeout(
    coro: Awaitable[T], 
    timeout: float, 
    default: Optional[T] = None
) -> Optional[T]:
    """
    在指定超时时间内运行协程
    
    Args:
        coro: 要运行的协程
        timeout: 超时时间（秒）
        default: 超时时返回的默认值
        
    Returns:
        协程结果或默认值
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"Operation timed out after {timeout} seconds")
        return default
    except Exception as e:
        logger.error(f"Operation failed: {e}")
        return default


async def gather_with_concurrency(
    *coros: Awaitable[T], 
    max_concurrency: int = 10
) -> List[T]:
    """
    限制并发数量的gather操作
    
    Args:
        *coros: 协程列表
        max_concurrency: 最大并发数
        
    Returns:
        结果列表
    """
    semaphore = asyncio.Semaphore(max_concurrency)
    
    async def _run_with_semaphore(coro):
        async with semaphore:
            return await coro
    
    tasks = [_run_with_semaphore(coro) for coro in coros]
    return await asyncio.gather(*tasks, return_exceptions=True)


def retry_async(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    异步重试装饰器
    
    Args:
        max_attempts: 最大重试次数
        delay: 初始延迟时间
        backoff: 退避倍数
        exceptions: 需要重试的异常类型
    """
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_attempts - 1:
                        break
                    
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {current_delay} seconds..."
                    )
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
            
            logger.error(f"All {max_attempts} attempts failed")
            raise last_exception
        
        return wrapper
    return decorator


async def safe_async_call(
    func: Callable[..., Awaitable[T]], 
    *args, 
    default: Optional[T] = None,
    log_errors: bool = True,
    **kwargs
) -> Optional[T]:
    """
    安全的异步函数调用
    
    Args:
        func: 要调用的异步函数
        *args: 位置参数
        default: 出错时返回的默认值
        log_errors: 是否记录错误日志
        **kwargs: 关键字参数
        
    Returns:
        函数结果或默认值
    """
    try:
        return await func(*args, **kwargs)
    except Exception as e:
        if log_errors:
            logger.error(f"Safe async call failed: {func.__name__}: {e}")
        return default


class AsyncContextManager:
    """异步上下文管理器基类"""
    
    async def __aenter__(self):
        await self.setup()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()
    
    async def setup(self):
        """设置资源"""
        pass
    
    async def cleanup(self):
        """清理资源"""
        pass


class AsyncTimer:
    """异步计时器"""
    
    def __init__(self):
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    async def __aenter__(self):
        self.start_time = datetime.now()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.end_time = datetime.now()
    
    @property
    def elapsed(self) -> Optional[timedelta]:
        """获取执行时间"""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None
    
    @property
    def elapsed_seconds(self) -> Optional[float]:
        """获取执行时间（秒）"""
        elapsed = self.elapsed
        return elapsed.total_seconds() if elapsed else None


# 使用示例
async def example_usage():
    """使用示例"""
    
    # 超时控制
    result = await run_with_timeout(
        some_slow_operation(), 
        timeout=5.0, 
        default="timeout"
    )
    
    # 并发控制
    results = await gather_with_concurrency(
        *[fetch_data(i) for i in range(100)],
        max_concurrency=10
    )
    
    # 重试机制
    @retry_async(max_attempts=3, delay=1.0)
    async def unreliable_operation():
        # 可能失败的操作
        pass
    
    # 计时器
    async with AsyncTimer() as timer:
        await some_operation()
    print(f"Operation took {timer.elapsed_seconds} seconds")


async def some_slow_operation():
    """示例慢操作"""
    await asyncio.sleep(2)
    return "completed"


async def fetch_data(item_id: int):
    """示例数据获取"""
    await asyncio.sleep(0.1)
    return f"data_{item_id}"


async def some_operation():
    """示例操作"""
    await asyncio.sleep(1)
