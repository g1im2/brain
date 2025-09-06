"""
三层金融交易系统集成层模块

提供系统协调、信号路由、数据流管理等核心集成功能，
实现顶层宏观战略、中间层组合管理、底层个股战术系统的统一协调。

主要组件：
- SystemCoordinator: 系统协调器
- SignalRouter: 信号路由器  
- DataFlowManager: 数据流管理器
- TemporalValidationCoordinator: 时间维度验证协调器
- 各层适配器组件
"""

from .interfaces import (
    ISystemCoordinator,
    ISignalRouter,
    IDataFlowManager,
    ITemporalValidationCoordinator,
    ISystemAdapter
)

from .models import (
    AnalysisCycleResult,
    SystemStatus,
    MarketEvent,
    ValidationResult,
    SignalRoutingResult,
    DataFlowStatus
)

from .coordinators import (
    SystemCoordinator,
    WorkflowEngine,
    EventEngine
)

from .routers import (
    SignalRouter,
    ConflictResolver,
    SignalProcessor
)

from .managers import (
    DataFlowManager,
    CacheManager,
    ResourceManager
)

from .validators import (
    TemporalValidationCoordinator,
    ValidationResultAggregator,
    ValidationRuleManager
)

from .adapters import (
    MacroAdapter,
    PortfolioAdapter,
    TacticalAdapter
)

from .monitors import (
    SystemMonitor,
    AlertManager
)

from .config import IntegrationConfig
from .exceptions import IntegrationException

__version__ = "1.0.0"
__author__ = "Financial Trading System Team"

__all__ = [
    # 接口
    'ISystemCoordinator',
    'ISignalRouter', 
    'IDataFlowManager',
    'ITemporalValidationCoordinator',
    'ISystemAdapter',
    
    # 数据模型
    'AnalysisCycleResult',
    'SystemStatus',
    'MarketEvent',
    'ValidationResult',
    'SignalRoutingResult',
    'DataFlowStatus',
    
    # 核心组件
    'SystemCoordinator',
    'SignalRouter',
    'DataFlowManager',
    'TemporalValidationCoordinator',
    
    # 适配器
    'MacroAdapter',
    'PortfolioAdapter', 
    'TacticalAdapter',
    
    # 监控组件
    'SystemMonitor',
    'AlertManager',
    
    # 配置和异常
    'IntegrationConfig',
    'IntegrationException'
]
