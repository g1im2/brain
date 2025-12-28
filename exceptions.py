"""
集成层异常处理模块

定义集成层各组件的异常类型和错误处理机制，
提供统一的异常处理和错误恢复策略。
"""

import re
from datetime import datetime
from typing import Dict, Any, Optional, List


class IntegrationException(Exception):
    """集成层基础异常类"""
    
    def __init__(self, message: str, error_code: str = "INTEGRATION_ERROR", 
                 component: str = "unknown", details: Optional[Dict[str, Any]] = None):
        """初始化异常
        
        Args:
            message: 错误消息
            error_code: 错误代码
            component: 出错组件
            details: 错误详情
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.component = component
        self.details = details or {}
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'error_code': self.error_code,
            'message': self.message,
            'component': self.component,
            'details': self.details,
            'timestamp': self.timestamp.isoformat()
        }


class SystemCoordinatorException(IntegrationException):
    """系统协调器异常"""
    
    def __init__(self, message: str, error_code: str = "COORDINATOR_ERROR", 
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message, error_code, "SystemCoordinator", details)


class CycleExecutionException(SystemCoordinatorException):
    """分析周期执行异常"""
    
    def __init__(self, cycle_id: str, stage: str, message: str, 
                 details: Optional[Dict[str, Any]] = None):
        self.cycle_id = cycle_id
        self.stage = stage
        error_details = details or {}
        error_details.update({
            'cycle_id': cycle_id,
            'failed_stage': stage
        })
        super().__init__(
            f"Cycle {cycle_id} failed at stage {stage}: {message}",
            "CYCLE_EXECUTION_ERROR",
            error_details
        )


class ResourceAllocationException(SystemCoordinatorException):
    """资源分配异常"""
    
    def __init__(self, resource_type: str, requested: float, available: float,
                 details: Optional[Dict[str, Any]] = None):
        self.resource_type = resource_type
        self.requested = requested
        self.available = available
        error_details = details or {}
        error_details.update({
            'resource_type': resource_type,
            'requested': requested,
            'available': available
        })
        super().__init__(
            f"Insufficient {resource_type}: requested {requested}, available {available}",
            "RESOURCE_ALLOCATION_ERROR",
            error_details
        )


class SignalRouterException(IntegrationException):
    """信号路由器异常"""
    
    def __init__(self, message: str, error_code: str = "SIGNAL_ROUTER_ERROR",
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message, error_code, "SignalRouter", details)


class SignalConflictException(SignalRouterException):
    """信号冲突异常"""
    
    def __init__(self, conflicting_signals: List[str], conflict_type: str,
                 details: Optional[Dict[str, Any]] = None):
        self.conflicting_signals = conflicting_signals
        self.conflict_type = conflict_type
        error_details = details or {}
        error_details.update({
            'conflicting_signals': conflicting_signals,
            'conflict_type': conflict_type
        })
        super().__init__(
            f"Signal conflict detected: {conflict_type} among signals {conflicting_signals}",
            "SIGNAL_CONFLICT_ERROR",
            error_details
        )


class SignalValidationException(SignalRouterException):
    """信号验证异常"""
    
    def __init__(self, signal_id: str, validation_errors: List[str],
                 details: Optional[Dict[str, Any]] = None):
        self.signal_id = signal_id
        self.validation_errors = validation_errors
        error_details = details or {}
        error_details.update({
            'signal_id': signal_id,
            'validation_errors': validation_errors
        })
        super().__init__(
            f"Signal {signal_id} validation failed: {', '.join(validation_errors)}",
            "SIGNAL_VALIDATION_ERROR",
            error_details
        )


class DataFlowException(IntegrationException):
    """数据流异常"""
    
    def __init__(self, message: str, error_code: str = "DATA_FLOW_ERROR",
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message, error_code, "DataFlowManager", details)


class DataQualityException(DataFlowException):
    """数据质量异常"""
    
    def __init__(self, data_source: str, quality_score: float, threshold: float,
                 details: Optional[Dict[str, Any]] = None):
        self.data_source = data_source
        self.quality_score = quality_score
        self.threshold = threshold
        error_details = details or {}
        error_details.update({
            'data_source': data_source,
            'quality_score': quality_score,
            'threshold': threshold
        })
        super().__init__(
            f"Data quality below threshold for {data_source}: {quality_score} < {threshold}",
            "DATA_QUALITY_ERROR",
            error_details
        )


class CacheException(DataFlowException):
    """缓存异常"""
    
    def __init__(self, operation: str, cache_key: str, message: str,
                 details: Optional[Dict[str, Any]] = None):
        self.operation = operation
        self.cache_key = cache_key
        error_details = details or {}
        error_details.update({
            'operation': operation,
            'cache_key': cache_key
        })
        super().__init__(
            f"Cache {operation} failed for key {cache_key}: {message}",
            "CACHE_ERROR",
            error_details
        )


class ValidationException(IntegrationException):
    """验证异常"""
    
    def __init__(self, message: str, error_code: str = "VALIDATION_ERROR",
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message, error_code, "TemporalValidationCoordinator", details)


class ValidationTimeoutException(ValidationException):
    """验证超时异常"""
    
    def __init__(self, validation_id: str, timeout: int,
                 details: Optional[Dict[str, Any]] = None):
        self.validation_id = validation_id
        self.timeout = timeout
        error_details = details or {}
        error_details.update({
            'validation_id': validation_id,
            'timeout': timeout
        })
        super().__init__(
            f"Validation {validation_id} timed out after {timeout} seconds",
            "VALIDATION_TIMEOUT_ERROR",
            error_details
        )


class ValidationSynchronizationException(ValidationException):
    """验证同步异常"""
    
    def __init__(self, historical_status: str, forward_status: str,
                 details: Optional[Dict[str, Any]] = None):
        self.historical_status = historical_status
        self.forward_status = forward_status
        error_details = details or {}
        error_details.update({
            'historical_status': historical_status,
            'forward_status': forward_status
        })
        super().__init__(
            f"Validation synchronization failed: historical={historical_status}, forward={forward_status}",
            "VALIDATION_SYNC_ERROR",
            error_details
        )


class AdapterException(IntegrationException):
    """适配器异常"""
    
    def __init__(self, adapter_name: str, message: str, error_code: str = "ADAPTER_ERROR",
                 details: Optional[Dict[str, Any]] = None):
        self.adapter_name = adapter_name
        error_details = details or {}
        error_details.update({'adapter_name': adapter_name})
        super().__init__(message, error_code, f"{adapter_name}Adapter", error_details)


class ConnectionException(AdapterException):
    """连接异常"""
    
    def __init__(self, adapter_name: str, target_system: str, message: str,
                 details: Optional[Dict[str, Any]] = None):
        self.target_system = target_system
        error_details = details or {}
        error_details.update({'target_system': target_system})
        super().__init__(
            adapter_name,
            f"Failed to connect to {target_system}: {message}",
            "CONNECTION_ERROR",
            error_details
        )


class HealthCheckException(AdapterException):
    """健康检查异常"""
    
    def __init__(self, adapter_name: str, target_system: str, health_status: str,
                 details: Optional[Dict[str, Any]] = None):
        self.target_system = target_system
        self.health_status = health_status
        error_details = details or {}
        error_details.update({
            'target_system': target_system,
            'health_status': health_status
        })
        super().__init__(
            adapter_name,
            f"Health check failed for {target_system}: status={health_status}",
            "HEALTH_CHECK_ERROR",
            error_details
        )


class MonitoringException(IntegrationException):
    """监控异常"""
    
    def __init__(self, message: str, error_code: str = "MONITORING_ERROR",
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message, error_code, "SystemMonitor", details)


class AlertException(MonitoringException):
    """告警异常"""
    
    def __init__(self, alert_type: str, component: str, message: str,
                 details: Optional[Dict[str, Any]] = None):
        self.alert_type = alert_type
        self.alert_component = component
        error_details = details or {}
        error_details.update({
            'alert_type': alert_type,
            'alert_component': component
        })
        super().__init__(
            f"Alert {alert_type} for {component}: {message}",
            "ALERT_ERROR",
            error_details
        )


class ConfigurationException(IntegrationException):
    """配置异常"""
    
    def __init__(self, config_key: str, message: str, 
                 details: Optional[Dict[str, Any]] = None):
        self.config_key = config_key
        error_details = details or {}
        error_details.update({'config_key': config_key})
        super().__init__(
            f"Configuration error for {config_key}: {message}",
            "CONFIGURATION_ERROR",
            "ConfigManager",
            error_details
        )


# 异常处理工具函数
def handle_exception(exception: Exception, component: str = "unknown",
                    context: Optional[Dict[str, Any]] = None,
                    mask_sensitive: bool = True) -> IntegrationException:
    """统一异常处理函数

    Args:
        exception: 原始异常
        component: 出错组件
        context: 上下文信息
        mask_sensitive: 是否屏蔽敏感信息

    Returns:
        IntegrationException: 标准化异常
    """
    if isinstance(exception, IntegrationException):
        return exception

    # 将标准异常转换为集成层异常
    details = context or {}

    # 屏蔽敏感信息
    if mask_sensitive:
        message = _mask_sensitive_info(str(exception))
        details = _mask_sensitive_details(details)
    else:
        message = str(exception)

    details.update({
        'original_exception_type': type(exception).__name__,
        'original_exception_message': message
    })

    return IntegrationException(
        message=message,
        error_code="WRAPPED_EXCEPTION",
        component=component,
        details=details
    )


def _mask_sensitive_info(message: str) -> str:
    """屏蔽敏感信息"""
    # 屏蔽密码、token等敏感信息
    patterns = [
        (r'password["\s]*[:=]["\s]*[^"\s]+', 'password=***'),
        (r'token["\s]*[:=]["\s]*[^"\s]+', 'token=***'),
        (r'key["\s]*[:=]["\s]*[^"\s]+', 'key=***'),
        (r'secret["\s]*[:=]["\s]*[^"\s]+', 'secret=***'),
    ]

    masked_message = message
    for pattern, replacement in patterns:
        masked_message = re.sub(pattern, replacement, masked_message, flags=re.IGNORECASE)

    return masked_message


def _mask_sensitive_details(details: Dict[str, Any]) -> Dict[str, Any]:
    """屏蔽详情中的敏感信息"""
    sensitive_keys = {'password', 'token', 'key', 'secret', 'auth', 'credential'}

    masked_details = {}
    for key, value in details.items():
        if any(sensitive_key in key.lower() for sensitive_key in sensitive_keys):
            masked_details[key] = '***'
        else:
            masked_details[key] = value

    return masked_details


def create_error_response(exception: IntegrationException) -> Dict[str, Any]:
    """创建标准错误响应
    
    Args:
        exception: 集成层异常
        
    Returns:
        Dict[str, Any]: 错误响应
    """
    return {
        'success': False,
        'error': exception.to_dict(),
        'timestamp': datetime.now().isoformat()
    }


def is_recoverable_error(exception: IntegrationException) -> bool:
    """判断错误是否可恢复
    
    Args:
        exception: 集成层异常
        
    Returns:
        bool: 是否可恢复
    """
    recoverable_errors = {
        'CONNECTION_ERROR',
        'TIMEOUT_ERROR',
        'RESOURCE_ALLOCATION_ERROR',
        'CACHE_ERROR',
        'DATA_QUALITY_ERROR'
    }
    
    return exception.error_code in recoverable_errors


def get_recovery_strategy(exception: IntegrationException) -> str:
    """获取错误恢复策略
    
    Args:
        exception: 集成层异常
        
    Returns:
        str: 恢复策略
    """
    recovery_strategies = {
        'CONNECTION_ERROR': 'retry_with_backoff',
        'TIMEOUT_ERROR': 'retry_with_increased_timeout',
        'RESOURCE_ALLOCATION_ERROR': 'wait_and_retry',
        'CACHE_ERROR': 'bypass_cache',
        'DATA_QUALITY_ERROR': 'use_fallback_data',
        'SIGNAL_CONFLICT_ERROR': 'apply_conflict_resolution',
        'VALIDATION_TIMEOUT_ERROR': 'use_cached_validation'
    }
    
    return recovery_strategies.get(exception.error_code, 'manual_intervention')
