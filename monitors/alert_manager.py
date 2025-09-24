"""
告警管理器实现

负责告警规则管理、多渠道通知、告警升级和历史管理，
提供完整的告警生命周期管理功能。
"""

import asyncio
import logging
import uuid
import json
import smtplib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Set
from collections import defaultdict, deque
from enum import Enum
from dataclasses import dataclass, field
from email.mime.text import MIMEText as MimeText
from email.mime.multipart import MIMEMultipart as MimeMultipart
import threading

from models import AlertInfo, PerformanceMetrics, SystemStatus
from config import IntegrationConfig
from exceptions import MonitoringException, AlertException

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """告警级别枚举"""
    P1 = "P1"  # 紧急
    P2 = "P2"  # 高
    P3 = "P3"  # 中
    P4 = "P4"  # 低


class AlertChannel(Enum):
    """告警渠道枚举"""
    EMAIL = "email"
    SMS = "sms"
    WEBHOOK = "webhook"
    SYSTEM = "system"
    SLACK = "slack"


class AlertStatus(Enum):
    """告警状态枚举"""
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


@dataclass
class AlertRule:
    """告警规则定义"""
    rule_id: str
    name: str
    description: str
    condition: Callable[[Any], bool]
    alert_level: AlertLevel
    component: str
    channels: List[AlertChannel]
    cooldown_minutes: int = 5
    escalation_minutes: int = 30
    auto_resolve: bool = True
    is_enabled: bool = True
    created_time: datetime = field(default_factory=datetime.now)
    last_triggered: Optional[datetime] = None
    trigger_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AlertNotification:
    """告警通知"""
    notification_id: str
    alert_id: str
    channel: AlertChannel
    recipient: str
    message: str
    sent_time: Optional[datetime] = None
    delivery_status: str = "pending"  # pending, sent, failed, delivered
    retry_count: int = 0
    error_message: Optional[str] = None


@dataclass
class AlertEscalation:
    """告警升级"""
    escalation_id: str
    alert_id: str
    from_level: AlertLevel
    to_level: AlertLevel
    escalation_time: datetime
    reason: str
    escalated_by: str = "system"


class NotificationChannel:
    """通知渠道基类"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.is_enabled = config.get('enabled', True)
    
    async def send_notification(self, notification: AlertNotification) -> bool:
        """发送通知 - 基类方法，由子类实现"""
        logger.warning(f"NotificationChannel.send_notification called - should be implemented by subclass")
        return False


class EmailChannel(NotificationChannel):
    """邮件通知渠道"""
    
    async def send_notification(self, notification: AlertNotification) -> bool:
        """发送邮件通知"""
        try:
            if not self.is_enabled:
                return False
            
            # 配置SMTP
            smtp_server = self.config.get('smtp_server', 'localhost')
            smtp_port = self.config.get('smtp_port', 587)
            username = self.config.get('username', '')
            password = self.config.get('password', '')
            
            # 创建邮件
            msg = MimeMultipart()
            msg['From'] = self.config.get('from_email', 'noreply@trading-system.com')
            msg['To'] = notification.recipient
            msg['Subject'] = f"[告警] {notification.alert_id}"
            
            msg.attach(MimeText(notification.message, 'plain', 'utf-8'))
            
            # 发送邮件
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                if username and password:
                    server.starttls()
                    server.login(username, password)
                
                server.send_message(msg)
            
            notification.sent_time = datetime.now()
            notification.delivery_status = "sent"
            return True
            
        except Exception as e:
            notification.error_message = str(e)
            notification.delivery_status = "failed"
            logger.error(f"Failed to send email notification: {e}")
            return False


class WebhookChannel(NotificationChannel):
    """Webhook通知渠道"""
    
    async def send_notification(self, notification: AlertNotification) -> bool:
        """发送Webhook通知"""
        try:
            if not self.is_enabled:
                return False
            
            import aiohttp
            
            webhook_url = self.config.get('webhook_url')
            if not webhook_url:
                return False
            
            payload = {
                'alert_id': notification.alert_id,
                'message': notification.message,
                'timestamp': datetime.now().isoformat(),
                'recipient': notification.recipient
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    webhook_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        notification.sent_time = datetime.now()
                        notification.delivery_status = "sent"
                        return True
                    else:
                        notification.error_message = f"HTTP {response.status}"
                        notification.delivery_status = "failed"
                        return False
            
        except Exception as e:
            notification.error_message = str(e)
            notification.delivery_status = "failed"
            logger.error(f"Failed to send webhook notification: {e}")
            return False


class SystemChannel(NotificationChannel):
    """系统通知渠道"""
    
    async def send_notification(self, notification: AlertNotification) -> bool:
        """发送系统通知"""
        try:
            # 记录到系统日志
            logger.warning(f"ALERT: {notification.message}")
            
            notification.sent_time = datetime.now()
            notification.delivery_status = "sent"
            return True
            
        except Exception as e:
            notification.error_message = str(e)
            notification.delivery_status = "failed"
            return False


class AlertManager:
    """告警管理器实现类
    
    负责告警规则管理、多渠道通知、告警升级和历史管理，
    提供完整的告警生命周期管理功能。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化告警管理器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False
        
        # 告警规则管理
        self._alert_rules: Dict[str, AlertRule] = {}
        self._rules_by_component: Dict[str, Set[str]] = defaultdict(set)
        
        # 活跃告警
        self._active_alerts: Dict[str, AlertInfo] = {}
        self._alert_history: deque = deque(maxlen=10000)
        
        # 通知管理
        self._pending_notifications: deque = deque()
        self._notification_history: deque = deque(maxlen=5000)
        
        # 升级管理
        self._escalations: List[AlertEscalation] = []
        
        # 通知渠道
        self._notification_channels: Dict[AlertChannel, NotificationChannel] = {}
        self._initialize_notification_channels()
        
        # 告警统计
        self._alert_statistics = {
            'total_alerts': 0,
            'active_alerts': 0,
            'resolved_alerts': 0,
            'escalated_alerts': 0,
            'notifications_sent': 0,
            'notification_failures': 0,
            'alerts_by_level': defaultdict(int),
            'alerts_by_component': defaultdict(int)
        }
        
        # 锁和同步
        self._alert_lock = threading.RLock()
        
        # 处理任务
        self._processing_task: Optional[asyncio.Task] = None
        self._escalation_task: Optional[asyncio.Task] = None
        self._monitoring_task: Optional[asyncio.Task] = None
        
        # 注册默认告警规则
        self._register_default_rules()
        
        logger.info("AlertManager initialized")
    
    async def start(self) -> bool:
        """启动告警管理器
        
        Returns:
            bool: 启动是否成功
        """
        try:
            if self._is_running:
                logger.warning("AlertManager is already running")
                return True
            
            if not self.config.monitoring.enable_alerting:
                logger.info("Alerting is disabled in config")
                return True
            
            # 启动处理任务
            self._processing_task = asyncio.create_task(self._notification_processing_loop())
            self._escalation_task = asyncio.create_task(self._escalation_processing_loop())
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            
            self._is_running = True
            logger.info("AlertManager started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start AlertManager: {e}")
            return False
    
    async def stop(self) -> bool:
        """停止告警管理器
        
        Returns:
            bool: 停止是否成功
        """
        try:
            if not self._is_running:
                return True
            
            logger.info("Stopping AlertManager...")
            self._is_running = False
            
            # 停止处理任务
            tasks = [self._processing_task, self._escalation_task, self._monitoring_task]
            for task in tasks:
                if task:
                    task.cancel()
            
            await asyncio.gather(*[t for t in tasks if t], return_exceptions=True)
            
            logger.info("AlertManager stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping AlertManager: {e}")
            return False
    
    def register_alert_rule(self, rule: AlertRule) -> bool:
        """注册告警规则
        
        Args:
            rule: 告警规则
            
        Returns:
            bool: 注册是否成功
        """
        try:
            with self._alert_lock:
                self._alert_rules[rule.rule_id] = rule
                self._rules_by_component[rule.component].add(rule.rule_id)
            
            logger.info(f"Alert rule registered: {rule.rule_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register alert rule {rule.rule_id}: {e}")
            return False
    
    def unregister_alert_rule(self, rule_id: str) -> bool:
        """注销告警规则
        
        Args:
            rule_id: 规则ID
            
        Returns:
            bool: 注销是否成功
        """
        try:
            with self._alert_lock:
                if rule_id in self._alert_rules:
                    rule = self._alert_rules[rule_id]
                    del self._alert_rules[rule_id]
                    self._rules_by_component[rule.component].discard(rule_id)
                    logger.info(f"Alert rule unregistered: {rule_id}")
                    return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to unregister alert rule {rule_id}: {e}")
            return False

    async def check_and_trigger_alerts(self, data: Any, component: str) -> List[str]:
        """检查并触发告警

        Args:
            data: 检查数据
            component: 组件名称

        Returns:
            List[str]: 触发的告警ID列表
        """
        if not self._is_running:
            return []

        triggered_alerts = []

        try:
            with self._alert_lock:
                # 获取该组件的告警规则
                rule_ids = self._rules_by_component.get(component, set())

                for rule_id in rule_ids:
                    rule = self._alert_rules.get(rule_id)
                    if not rule or not rule.is_enabled:
                        continue

                    # 检查冷却时间
                    if self._is_in_cooldown(rule):
                        continue

                    # 评估告警条件
                    try:
                        if rule.condition(data):
                            alert_id = await self._trigger_alert(rule, data)
                            if alert_id:
                                triggered_alerts.append(alert_id)
                    except Exception as e:
                        logger.error(f"Failed to evaluate alert rule {rule_id}: {e}")

            return triggered_alerts

        except Exception as e:
            logger.error(f"Failed to check alerts for component {component}: {e}")
            return []

    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """确认告警

        Args:
            alert_id: 告警ID
            acknowledged_by: 确认人

        Returns:
            bool: 确认是否成功
        """
        try:
            with self._alert_lock:
                if alert_id in self._active_alerts:
                    alert = self._active_alerts[alert_id]
                    alert.status = AlertStatus.ACKNOWLEDGED.value
                    alert.acknowledged_by = acknowledged_by
                    alert.acknowledged_time = datetime.now()

                    logger.info(f"Alert acknowledged: {alert_id} by {acknowledged_by}")
                    return True
            return False

        except Exception as e:
            logger.error(f"Failed to acknowledge alert {alert_id}: {e}")
            return False

    async def resolve_alert(self, alert_id: str, resolved_by: str = "system") -> bool:
        """解决告警

        Args:
            alert_id: 告警ID
            resolved_by: 解决人

        Returns:
            bool: 解决是否成功
        """
        try:
            with self._alert_lock:
                if alert_id in self._active_alerts:
                    alert = self._active_alerts[alert_id]
                    alert.status = AlertStatus.RESOLVED.value
                    alert.resolved_time = datetime.now()

                    # 移动到历史记录
                    self._alert_history.append(alert)
                    del self._active_alerts[alert_id]

                    # 更新统计
                    self._alert_statistics['active_alerts'] -= 1
                    self._alert_statistics['resolved_alerts'] += 1

                    logger.info(f"Alert resolved: {alert_id} by {resolved_by}")
                    return True
            return False

        except Exception as e:
            logger.error(f"Failed to resolve alert {alert_id}: {e}")
            return False

    def get_active_alerts(self, component: Optional[str] = None,
                         level: Optional[AlertLevel] = None) -> List[AlertInfo]:
        """获取活跃告警

        Args:
            component: 组件过滤
            level: 级别过滤

        Returns:
            List[AlertInfo]: 活跃告警列表
        """
        with self._alert_lock:
            alerts = list(self._active_alerts.values())

            # 应用过滤器
            if component:
                alerts = [a for a in alerts if a.component == component]

            if level:
                alerts = [a for a in alerts if a.alert_level == level.value]

            # 按创建时间排序
            alerts.sort(key=lambda a: a.created_time, reverse=True)
            return alerts

    def get_alert_statistics(self) -> Dict[str, Any]:
        """获取告警统计信息

        Returns:
            Dict[str, Any]: 统计信息
        """
        with self._alert_lock:
            stats = self._alert_statistics.copy()
            stats.update({
                'registered_rules': len(self._alert_rules),
                'pending_notifications': len(self._pending_notifications),
                'escalations': len(self._escalations),
                'notification_channels': len(self._notification_channels)
            })
            return stats

    # 私有方法实现
    def _initialize_notification_channels(self) -> None:
        """初始化通知渠道"""
        # 邮件渠道
        email_config = {
            'enabled': True,
            'smtp_server': 'localhost',
            'smtp_port': 587,
            'username': '',
            'password': '',
            'from_email': 'noreply@trading-system.com'
        }
        self._notification_channels[AlertChannel.EMAIL] = EmailChannel(email_config)

        # Webhook渠道
        webhook_config = {
            'enabled': False,
            'webhook_url': ''
        }
        self._notification_channels[AlertChannel.WEBHOOK] = WebhookChannel(webhook_config)

        # 系统渠道
        system_config = {'enabled': True}
        self._notification_channels[AlertChannel.SYSTEM] = SystemChannel(system_config)

    def _register_default_rules(self) -> None:
        """注册默认告警规则"""
        # CPU使用率告警
        cpu_rule = AlertRule(
            rule_id="cpu_high_usage",
            name="CPU使用率过高",
            description="CPU使用率超过阈值",
            condition=lambda data: hasattr(data, 'cpu_usage') and data.cpu_usage > 0.8,
            alert_level=AlertLevel.P2,
            component="system",
            channels=[AlertChannel.SYSTEM, AlertChannel.EMAIL],
            cooldown_minutes=5
        )
        self.register_alert_rule(cpu_rule)

        # 内存使用率告警
        memory_rule = AlertRule(
            rule_id="memory_high_usage",
            name="内存使用率过高",
            description="内存使用率超过阈值",
            condition=lambda data: hasattr(data, 'memory_usage') and data.memory_usage > 0.8,
            alert_level=AlertLevel.P2,
            component="system",
            channels=[AlertChannel.SYSTEM, AlertChannel.EMAIL],
            cooldown_minutes=5
        )
        self.register_alert_rule(memory_rule)

        # 响应时间告警
        response_time_rule = AlertRule(
            rule_id="response_time_high",
            name="响应时间过长",
            description="系统响应时间超过阈值",
            condition=lambda data: hasattr(data, 'response_time') and data.response_time > 1000,
            alert_level=AlertLevel.P3,
            component="system",
            channels=[AlertChannel.SYSTEM],
            cooldown_minutes=10
        )
        self.register_alert_rule(response_time_rule)

        # 错误率告警
        error_rate_rule = AlertRule(
            rule_id="error_rate_high",
            name="错误率过高",
            description="系统错误率超过阈值",
            condition=lambda data: hasattr(data, 'error_rate') and data.error_rate > 0.05,
            alert_level=AlertLevel.P2,
            component="system",
            channels=[AlertChannel.SYSTEM, AlertChannel.EMAIL],
            cooldown_minutes=5
        )
        self.register_alert_rule(error_rate_rule)

    def _is_in_cooldown(self, rule: AlertRule) -> bool:
        """检查是否在冷却期"""
        if not rule.last_triggered:
            return False

        cooldown_end = rule.last_triggered + timedelta(minutes=rule.cooldown_minutes)
        return datetime.now() < cooldown_end

    async def _trigger_alert(self, rule: AlertRule, data: Any) -> Optional[str]:
        """触发告警"""
        try:
            alert_id = str(uuid.uuid4())

            # 创建告警信息
            alert = AlertInfo(
                alert_id=alert_id,
                alert_level=rule.alert_level.value,
                alert_type=rule.name,
                component=rule.component,
                message=self._generate_alert_message(rule, data),
                created_time=datetime.now(),
                status=AlertStatus.ACTIVE.value
            )

            # 更新规则统计
            rule.last_triggered = datetime.now()
            rule.trigger_count += 1

            # 存储活跃告警
            self._active_alerts[alert_id] = alert

            # 更新统计
            self._alert_statistics['total_alerts'] += 1
            self._alert_statistics['active_alerts'] += 1
            self._alert_statistics['alerts_by_level'][rule.alert_level.value] += 1
            self._alert_statistics['alerts_by_component'][rule.component] += 1

            # 创建通知
            await self._create_notifications(alert, rule)

            logger.info(f"Alert triggered: {alert_id} - {rule.name}")
            return alert_id

        except Exception as e:
            logger.error(f"Failed to trigger alert for rule {rule.rule_id}: {e}")
            return None

    def _generate_alert_message(self, rule: AlertRule, data: Any) -> str:
        """生成告警消息"""
        message = f"告警: {rule.name}\n"
        message += f"组件: {rule.component}\n"
        message += f"描述: {rule.description}\n"
        message += f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"

        # 添加数据信息
        if hasattr(data, '__dict__'):
            message += f"数据: {data.__dict__}\n"
        else:
            message += f"数据: {data}\n"

        return message

    async def _create_notifications(self, alert: AlertInfo, rule: AlertRule) -> None:
        """创建通知"""
        try:
            # 获取收件人列表（这里简化为固定列表）
            recipients = {
                AlertChannel.EMAIL: ['admin@trading-system.com'],
                AlertChannel.WEBHOOK: ['webhook'],
                AlertChannel.SYSTEM: ['system']
            }

            for channel in rule.channels:
                channel_recipients = recipients.get(channel, [])

                for recipient in channel_recipients:
                    notification = AlertNotification(
                        notification_id=str(uuid.uuid4()),
                        alert_id=alert.alert_id,
                        channel=channel,
                        recipient=recipient,
                        message=alert.message
                    )

                    self._pending_notifications.append(notification)

        except Exception as e:
            logger.error(f"Failed to create notifications for alert {alert.alert_id}: {e}")

    async def _notification_processing_loop(self) -> None:
        """通知处理循环"""
        while self._is_running:
            try:
                if self._pending_notifications:
                    notification = self._pending_notifications.popleft()
                    await self._send_notification(notification)
                else:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Notification processing loop error: {e}")
                await asyncio.sleep(1)

    async def _send_notification(self, notification: AlertNotification) -> None:
        """发送通知"""
        try:
            channel = self._notification_channels.get(notification.channel)
            if not channel:
                notification.delivery_status = "failed"
                notification.error_message = "Channel not available"
                return

            # 发送通知
            success = await channel.send_notification(notification)

            if success:
                self._alert_statistics['notifications_sent'] += 1
            else:
                self._alert_statistics['notification_failures'] += 1

                # 重试逻辑
                if notification.retry_count < 3:
                    notification.retry_count += 1
                    self._pending_notifications.append(notification)

            # 记录通知历史
            self._notification_history.append(notification)

        except Exception as e:
            logger.error(f"Failed to send notification {notification.notification_id}: {e}")
            notification.delivery_status = "failed"
            notification.error_message = str(e)
            self._alert_statistics['notification_failures'] += 1

    async def _escalation_processing_loop(self) -> None:
        """升级处理循环"""
        while self._is_running:
            try:
                await self._check_alert_escalations()
                await asyncio.sleep(60)  # 每分钟检查一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Escalation processing loop error: {e}")
                await asyncio.sleep(60)

    async def _monitoring_loop(self) -> None:
        """监控循环"""
        while self._is_running:
            try:
                # 自动解决过期的抑制告警
                await self._check_suppressed_alerts()

                # 清理历史记录
                await self._cleanup_history()

                await asyncio.sleep(self.config.monitoring.alert_cooldown)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Alert monitoring loop error: {e}")
                await asyncio.sleep(60)

    async def _check_suppressed_alerts(self) -> None:
        """检查抑制的告警"""
        current_time = datetime.now()

        with self._alert_lock:
            for alert in list(self._active_alerts.values()):
                if (alert.status == AlertStatus.SUPPRESSED.value and
                    alert.suppressed_until and
                    current_time > alert.suppressed_until):
                    alert.status = AlertStatus.ACTIVE.value
                    alert.suppressed_until = None
                    logger.info(f"Alert suppression expired: {alert.alert_id}")

    async def _cleanup_history(self) -> None:
        """清理历史记录"""
        # 清理过期的升级记录
        cutoff_time = datetime.now() - timedelta(days=30)
        self._escalations = [
            e for e in self._escalations
            if e.escalation_time > cutoff_time
        ]

    async def _check_alert_escalations(self) -> None:
        """检查告警升级"""
        current_time = datetime.now()

        with self._alert_lock:
            for alert in list(self._active_alerts.values()):
                if alert.status != AlertStatus.ACTIVE.value:
                    continue

                # 获取对应的规则
                rule = None
                for r in self._alert_rules.values():
                    if r.component == alert.component and r.name == alert.alert_type:
                        rule = r
                        break

                if not rule:
                    continue

                # 检查是否需要升级
                time_since_created = (current_time - alert.created_time).total_seconds() / 60

                if time_since_created >= rule.escalation_minutes:
                    await self._escalate_alert(alert, rule)

    async def _escalate_alert(self, alert: AlertInfo, rule: AlertRule) -> None:
        """升级告警"""
        try:
            # 确定新的告警级别
            current_level = AlertLevel(alert.alert_level)
            new_level = self._get_escalated_level(current_level)

            if new_level == current_level:
                return  # 已经是最高级别

            # 创建升级记录
            escalation = AlertEscalation(
                escalation_id=str(uuid.uuid4()),
                alert_id=alert.alert_id,
                from_level=current_level,
                to_level=new_level,
                escalation_time=datetime.now(),
                reason="Automatic escalation due to timeout"
            )

            # 更新告警级别
            alert.alert_level = new_level.value

            # 记录升级
            self._escalations.append(escalation)
            self._alert_statistics['escalated_alerts'] += 1

            logger.warning(f"Alert escalated: {alert.alert_id} from {current_level.value} to {new_level.value}")

        except Exception as e:
            logger.error(f"Failed to escalate alert {alert.alert_id}: {e}")

    def _get_escalated_level(self, current_level: AlertLevel) -> AlertLevel:
        """获取升级后的告警级别"""
        escalation_map = {
            AlertLevel.P4: AlertLevel.P3,
            AlertLevel.P3: AlertLevel.P2,
            AlertLevel.P2: AlertLevel.P1,
            AlertLevel.P1: AlertLevel.P1  # 最高级别
        }
        return escalation_map.get(current_level, current_level)
