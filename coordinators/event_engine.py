"""
事件引擎实现

负责事件的订阅/发布、持久化存储、过滤路由和回放功能，
提供高性能的事件驱动架构支持。
"""

import asyncio
import logging
import uuid
import json
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Set
from collections import defaultdict, deque
from enum import Enum
from dataclasses import dataclass, field
import threading
import weakref

from models import MarketEvent, EventType
from config import IntegrationConfig
from exceptions import IntegrationException

logger = logging.getLogger(__name__)


class EventPriority(Enum):
    """事件优先级枚举"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4
    EMERGENCY = 5


@dataclass
class EventSubscription:
    """事件订阅信息"""
    subscription_id: str
    subscriber_id: str
    event_types: Set[EventType]
    event_filter: Optional[Callable[[MarketEvent], bool]] = None
    callback: Optional[Callable] = None
    priority: EventPriority = EventPriority.NORMAL
    created_time: datetime = field(default_factory=datetime.now)
    last_triggered: Optional[datetime] = None
    trigger_count: int = 0
    is_active: bool = True


@dataclass
class EventRecord:
    """事件记录"""
    record_id: str
    event: MarketEvent
    published_time: datetime
    processed_time: Optional[datetime] = None
    subscriber_count: int = 0
    processing_duration: float = 0.0
    status: str = "pending"  # pending, processing, completed, failed
    error_message: Optional[str] = None


class EventStorage:
    """事件存储接口"""
    
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self._events: deque = deque(maxlen=max_size)
        self._event_index: Dict[str, EventRecord] = {}
        self._lock = threading.RLock()
    
    def store_event(self, record: EventRecord) -> bool:
        """存储事件记录"""
        try:
            with self._lock:
                self._events.append(record)
                self._event_index[record.record_id] = record
                
                # 清理过期索引
                if len(self._event_index) > self.max_size:
                    oldest_record = self._events[0] if self._events else None
                    if oldest_record and oldest_record.record_id in self._event_index:
                        del self._event_index[oldest_record.record_id]
                
                return True
        except Exception as e:
            logger.error(f"Failed to store event: {e}")
            return False
    
    def get_event(self, record_id: str) -> Optional[EventRecord]:
        """获取事件记录"""
        with self._lock:
            return self._event_index.get(record_id)
    
    def get_events_by_type(self, event_type: EventType, 
                          start_time: Optional[datetime] = None,
                          end_time: Optional[datetime] = None) -> List[EventRecord]:
        """按类型获取事件记录"""
        with self._lock:
            results = []
            for record in self._events:
                if record.event.event_type == event_type:
                    if start_time and record.published_time < start_time:
                        continue
                    if end_time and record.published_time > end_time:
                        continue
                    results.append(record)
            return results
    
    def get_events_by_time_range(self, start_time: datetime, 
                                end_time: datetime) -> List[EventRecord]:
        """按时间范围获取事件记录"""
        with self._lock:
            results = []
            for record in self._events:
                if start_time <= record.published_time <= end_time:
                    results.append(record)
            return results
    
    def cleanup_old_events(self, cutoff_time: datetime) -> int:
        """清理旧事件"""
        with self._lock:
            cleaned_count = 0
            while self._events and self._events[0].published_time < cutoff_time:
                old_record = self._events.popleft()
                if old_record.record_id in self._event_index:
                    del self._event_index[old_record.record_id]
                cleaned_count += 1
            return cleaned_count


class EventEngine:
    """事件引擎实现类
    
    负责事件的订阅/发布、持久化存储、过滤路由和回放功能，
    提供高性能的事件驱动架构支持。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化事件引擎
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False
        
        # 事件存储
        self._event_storage = EventStorage(max_size=10000)
        
        # 订阅管理
        self._subscriptions: Dict[str, EventSubscription] = {}
        self._subscribers_by_type: Dict[EventType, Set[str]] = defaultdict(set)
        self._subscriber_callbacks: Dict[str, Callable] = {}
        
        # 事件队列（按优先级）
        self._event_queues: Dict[EventPriority, asyncio.Queue] = {
            priority: asyncio.Queue() for priority in EventPriority
        }
        
        # 事件统计
        self._event_statistics = {
            'total_published': 0,
            'total_processed': 0,
            'total_subscribers': 0,
            'events_by_type': defaultdict(int),
            'average_processing_time': 0.0,
            'failed_events': 0
        }
        
        # 锁和同步
        self._subscription_lock = threading.RLock()
        
        # 处理任务
        self._processing_tasks: List[asyncio.Task] = []
        self._monitoring_task: Optional[asyncio.Task] = None
        
        logger.info("EventEngine initialized")
    
    async def start(self) -> bool:
        """启动事件引擎
        
        Returns:
            bool: 启动是否成功
        """
        try:
            if self._is_running:
                logger.warning("EventEngine is already running")
                return True
            
            # 启动事件处理任务
            for priority in EventPriority:
                task = asyncio.create_task(self._process_events_by_priority(priority))
                self._processing_tasks.append(task)
            
            # 启动监控任务
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            
            self._is_running = True
            logger.info("EventEngine started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start EventEngine: {e}")
            return False
    
    async def stop(self) -> bool:
        """停止事件引擎
        
        Returns:
            bool: 停止是否成功
        """
        try:
            if not self._is_running:
                return True
            
            logger.info("Stopping EventEngine...")
            self._is_running = False
            
            # 停止处理任务
            for task in self._processing_tasks:
                task.cancel()
            
            if self._monitoring_task:
                self._monitoring_task.cancel()
            
            # 等待任务完成
            await asyncio.gather(*self._processing_tasks, self._monitoring_task, return_exceptions=True)
            
            self._processing_tasks.clear()
            
            logger.info("EventEngine stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping EventEngine: {e}")
            return False
    
    def subscribe(self, subscriber_id: str, event_types: List[EventType],
                 callback: Callable, event_filter: Optional[Callable] = None,
                 priority: EventPriority = EventPriority.NORMAL) -> str:
        """订阅事件
        
        Args:
            subscriber_id: 订阅者ID
            event_types: 事件类型列表
            callback: 回调函数
            event_filter: 事件过滤器
            priority: 订阅优先级
            
        Returns:
            str: 订阅ID
        """
        try:
            subscription_id = str(uuid.uuid4())
            
            subscription = EventSubscription(
                subscription_id=subscription_id,
                subscriber_id=subscriber_id,
                event_types=set(event_types),
                event_filter=event_filter,
                callback=callback,
                priority=priority
            )
            
            with self._subscription_lock:
                self._subscriptions[subscription_id] = subscription
                
                # 更新类型索引
                for event_type in event_types:
                    self._subscribers_by_type[event_type].add(subscription_id)
                
                # 存储回调函数（使用弱引用避免内存泄漏）
                self._subscriber_callbacks[subscription_id] = callback
                
                self._event_statistics['total_subscribers'] += 1
            
            logger.info(f"Event subscription created: {subscription_id} for {subscriber_id}")
            return subscription_id
            
        except Exception as e:
            logger.error(f"Failed to create subscription: {e}")
            raise IntegrationException(f"Subscription failed: {e}", "SUBSCRIPTION_FAILED")
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """取消订阅
        
        Args:
            subscription_id: 订阅ID
            
        Returns:
            bool: 取消是否成功
        """
        try:
            with self._subscription_lock:
                if subscription_id not in self._subscriptions:
                    return False
                
                subscription = self._subscriptions[subscription_id]
                
                # 从类型索引中移除
                for event_type in subscription.event_types:
                    self._subscribers_by_type[event_type].discard(subscription_id)
                
                # 移除订阅和回调
                del self._subscriptions[subscription_id]
                self._subscriber_callbacks.pop(subscription_id, None)
                
                self._event_statistics['total_subscribers'] -= 1
            
            logger.info(f"Event subscription removed: {subscription_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to unsubscribe: {e}")
            return False
    
    async def publish(self, event: MarketEvent, 
                     priority: EventPriority = EventPriority.NORMAL) -> str:
        """发布事件
        
        Args:
            event: 市场事件
            priority: 事件优先级
            
        Returns:
            str: 事件记录ID
        """
        try:
            if not self._is_running:
                raise IntegrationException("EventEngine is not running", "EVENT_ENGINE_NOT_RUNNING")
            
            # 创建事件记录
            record_id = str(uuid.uuid4())
            record = EventRecord(
                record_id=record_id,
                event=event,
                published_time=datetime.now()
            )
            
            # 存储事件
            if not self._event_storage.store_event(record):
                raise IntegrationException("Failed to store event", "EVENT_STORAGE_FAILED")
            
            # 添加到处理队列
            await self._event_queues[priority].put((record, priority))
            
            # 更新统计
            self._event_statistics['total_published'] += 1
            self._event_statistics['events_by_type'][event.event_type] += 1
            
            logger.debug(f"Event published: {record_id}, type: {event.event_type}")
            return record_id
            
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            raise IntegrationException(f"Event publishing failed: {e}", "EVENT_PUBLISH_FAILED")

    async def replay_events(self, start_time: datetime, end_time: datetime,
                           event_types: Optional[List[EventType]] = None,
                           target_subscriber: Optional[str] = None) -> int:
        """回放事件

        Args:
            start_time: 开始时间
            end_time: 结束时间
            event_types: 事件类型过滤
            target_subscriber: 目标订阅者

        Returns:
            int: 回放事件数量
        """
        try:
            # 获取时间范围内的事件
            events = self._event_storage.get_events_by_time_range(start_time, end_time)

            # 过滤事件类型
            if event_types:
                events = [e for e in events if e.event.event_type in event_types]

            replayed_count = 0

            for record in events:
                # 获取匹配的订阅者
                matching_subscriptions = self._get_matching_subscriptions(
                    record.event, target_subscriber
                )

                if matching_subscriptions:
                    # 重新发布事件
                    await self._process_event_record(record, matching_subscriptions, is_replay=True)
                    replayed_count += 1

            logger.info(f"Event replay completed: {replayed_count} events replayed")
            return replayed_count

        except Exception as e:
            logger.error(f"Event replay failed: {e}")
            raise IntegrationException(f"Event replay failed: {e}", "EVENT_REPLAY_FAILED")

    def get_event_statistics(self) -> Dict[str, Any]:
        """获取事件统计信息

        Returns:
            Dict[str, Any]: 统计信息
        """
        with self._subscription_lock:
            stats = self._event_statistics.copy()
            stats.update({
                'active_subscriptions': len(self._subscriptions),
                'queue_sizes': {
                    priority.name: queue.qsize()
                    for priority, queue in self._event_queues.items()
                },
                'storage_size': len(self._event_storage._events)
            })

            # 计算处理成功率
            total_processed = stats['total_processed']
            if total_processed > 0:
                stats['success_rate'] = (total_processed - stats['failed_events']) / total_processed
            else:
                stats['success_rate'] = 0.0

            return stats

    def get_subscription_info(self, subscription_id: str) -> Optional[Dict[str, Any]]:
        """获取订阅信息

        Args:
            subscription_id: 订阅ID

        Returns:
            Optional[Dict[str, Any]]: 订阅信息
        """
        with self._subscription_lock:
            if subscription_id in self._subscriptions:
                subscription = self._subscriptions[subscription_id]
                return {
                    'subscription_id': subscription_id,
                    'subscriber_id': subscription.subscriber_id,
                    'event_types': [et.value for et in subscription.event_types],
                    'priority': subscription.priority.name,
                    'created_time': subscription.created_time.isoformat(),
                    'last_triggered': subscription.last_triggered.isoformat() if subscription.last_triggered else None,
                    'trigger_count': subscription.trigger_count,
                    'is_active': subscription.is_active
                }
        return None

    # 私有方法实现
    async def _process_events_by_priority(self, priority: EventPriority) -> None:
        """按优先级处理事件"""
        queue = self._event_queues[priority]

        while self._is_running:
            try:
                # 获取事件（带超时）
                record, event_priority = await asyncio.wait_for(queue.get(), timeout=1.0)

                # 处理事件
                await self._process_event_record(record)

            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing event with priority {priority.name}: {e}")

    async def _process_event_record(self, record: EventRecord,
                                  target_subscriptions: Optional[List[EventSubscription]] = None,
                                  is_replay: bool = False) -> None:
        """处理事件记录"""
        try:
            start_time = datetime.now()
            record.status = "processing"

            # 获取匹配的订阅
            if target_subscriptions is None:
                matching_subscriptions = self._get_matching_subscriptions(record.event)
            else:
                matching_subscriptions = target_subscriptions

            record.subscriber_count = len(matching_subscriptions)

            if not matching_subscriptions:
                record.status = "completed"
                record.processed_time = datetime.now()
                return

            # 并发通知订阅者
            notification_tasks = []
            for subscription in matching_subscriptions:
                task = asyncio.create_task(
                    self._notify_subscriber(subscription, record.event, is_replay)
                )
                notification_tasks.append(task)

            # 等待所有通知完成
            results = await asyncio.gather(*notification_tasks, return_exceptions=True)

            # 检查结果
            failed_notifications = sum(1 for result in results if isinstance(result, Exception))

            if failed_notifications > 0:
                record.status = "failed"
                record.error_message = f"{failed_notifications} notifications failed"
                self._event_statistics['failed_events'] += 1
            else:
                record.status = "completed"

            # 更新处理时间
            record.processed_time = datetime.now()
            record.processing_duration = (record.processed_time - start_time).total_seconds()

            # 更新统计
            self._event_statistics['total_processed'] += 1
            self._update_average_processing_time(record.processing_duration)

        except Exception as e:
            record.status = "failed"
            record.error_message = str(e)
            record.processed_time = datetime.now()
            self._event_statistics['failed_events'] += 1
            logger.error(f"Failed to process event record {record.record_id}: {e}")

    def _get_matching_subscriptions(self, event: MarketEvent,
                                  target_subscriber: Optional[str] = None) -> List[EventSubscription]:
        """获取匹配的订阅"""
        matching_subscriptions = []

        with self._subscription_lock:
            # 获取订阅该事件类型的订阅者
            subscription_ids = self._subscribers_by_type.get(event.event_type, set())

            for subscription_id in subscription_ids:
                subscription = self._subscriptions.get(subscription_id)
                if not subscription or not subscription.is_active:
                    continue

                # 检查目标订阅者过滤
                if target_subscriber and subscription.subscriber_id != target_subscriber:
                    continue

                # 应用事件过滤器
                if subscription.event_filter:
                    try:
                        if not subscription.event_filter(event):
                            continue
                    except Exception as e:
                        logger.warning(f"Event filter failed for subscription {subscription_id}: {e}")
                        continue

                matching_subscriptions.append(subscription)

        # 按优先级排序
        matching_subscriptions.sort(key=lambda s: s.priority.value, reverse=True)
        return matching_subscriptions

    async def _notify_subscriber(self, subscription: EventSubscription,
                               event: MarketEvent, is_replay: bool = False) -> None:
        """通知订阅者"""
        try:
            callback = self._subscriber_callbacks.get(subscription.subscription_id)
            if not callback:
                logger.warning(f"No callback found for subscription {subscription.subscription_id}")
                return

            # 更新订阅统计
            subscription.last_triggered = datetime.now()
            subscription.trigger_count += 1

            # 调用回调函数
            if asyncio.iscoroutinefunction(callback):
                await callback(event, is_replay=is_replay)
            else:
                # 在线程池中执行同步回调
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: callback(event, is_replay=is_replay)
                )

        except Exception as e:
            logger.error(f"Failed to notify subscriber {subscription.subscriber_id}: {e}")
            raise

    def _update_average_processing_time(self, processing_time: float) -> None:
        """更新平均处理时间"""
        total_processed = self._event_statistics['total_processed']
        current_avg = self._event_statistics['average_processing_time']

        if total_processed > 0:
            self._event_statistics['average_processing_time'] = (
                (current_avg * (total_processed - 1) + processing_time) / total_processed
            )

    async def _monitoring_loop(self) -> None:
        """监控循环"""
        while self._is_running:
            try:
                # 清理过期事件
                await self._cleanup_old_events()

                # 检查队列健康状态
                await self._check_queue_health()

                # 清理无效订阅
                await self._cleanup_invalid_subscriptions()

                await asyncio.sleep(60)  # 每分钟检查一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Event monitoring loop error: {e}")
                await asyncio.sleep(60)

    async def _cleanup_old_events(self) -> None:
        """清理过期事件"""
        cutoff_time = datetime.now() - timedelta(hours=24)  # 保留24小时
        cleaned_count = self._event_storage.cleanup_old_events(cutoff_time)

        if cleaned_count > 0:
            logger.info(f"Cleaned up {cleaned_count} old events")

    async def _check_queue_health(self) -> None:
        """检查队列健康状态"""
        for priority, queue in self._event_queues.items():
            queue_size = queue.qsize()
            if queue_size > 1000:  # 队列过大警告
                logger.warning(f"Event queue {priority.name} is large: {queue_size} events")

    async def _cleanup_invalid_subscriptions(self) -> None:
        """清理无效订阅"""
        with self._subscription_lock:
            invalid_subscriptions = []

            for subscription_id, subscription in self._subscriptions.items():
                # 检查回调函数是否仍然有效
                if subscription_id not in self._subscriber_callbacks:
                    invalid_subscriptions.append(subscription_id)

            # 移除无效订阅
            for subscription_id in invalid_subscriptions:
                self.unsubscribe(subscription_id)
                logger.info(f"Removed invalid subscription: {subscription_id}")
