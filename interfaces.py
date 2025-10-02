"""
集成层核心接口定义

定义系统协调、信号路由、数据流管理等核心组件的抽象接口，
确保各组件之间的标准化交互和可扩展性。
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import asyncio

try:
    from .models import (
        AnalysisCycleResult,
        SystemStatus,
        MarketEvent,
        ValidationResult,
        SignalRoutingResult,
        DataFlowStatus,
        ResourceAllocation,
        ConflictResolution
    )
except Exception:  # 兼容在容器内以脚本方式运行（无包上下文）
    from models import (
        AnalysisCycleResult,
        SystemStatus,
        MarketEvent,
        ValidationResult,
        SignalRoutingResult,
        DataFlowStatus,
        ResourceAllocation,
        ConflictResolution
    )


class ISystemCoordinator(ABC):
    """系统协调器接口
    
    负责统一协调三层系统的分析周期、资源管理和异常处理
    """
    
    @abstractmethod
    async def coordinate_full_analysis_cycle(self) -> AnalysisCycleResult:
        """协调完整分析周期
        
        Returns:
            AnalysisCycleResult: 分析周期执行结果
        """
        pass
    
    @abstractmethod
    async def coordinate_real_time_updates(self, events: List[MarketEvent]) -> None:
        """协调实时更新处理
        
        Args:
            events: 市场事件列表
        """
        pass
    
    @abstractmethod
    async def coordinate_system_startup(self) -> bool:
        """协调系统启动流程
        
        Returns:
            bool: 启动是否成功
        """
        pass
    
    @abstractmethod
    async def coordinate_system_shutdown(self) -> bool:
        """协调系统关闭流程
        
        Returns:
            bool: 关闭是否成功
        """
        pass
    
    @abstractmethod
    async def allocate_system_resources(self) -> ResourceAllocation:
        """分配系统资源
        
        Returns:
            ResourceAllocation: 资源分配结果
        """
        pass
    
    @abstractmethod
    async def handle_system_exception(self, exception: Exception) -> ConflictResolution:
        """处理系统异常
        
        Args:
            exception: 系统异常
            
        Returns:
            ConflictResolution: 异常处理结果
        """
        pass


class ISignalRouter(ABC):
    """信号路由器接口
    
    负责管理各层之间的信号传递、冲突解决和质量控制
    """
    
    @abstractmethod
    async def route_macro_signals(self, macro_state: Any) -> Any:
        """路由宏观信号
        
        Args:
            macro_state: 宏观状态
            
        Returns:
            Any: 组合指令
        """
        pass
    
    @abstractmethod
    async def route_portfolio_signals(self, instruction: Any) -> Any:
        """路由组合信号
        
        Args:
            instruction: 组合指令
            
        Returns:
            Any: 战术分配
        """
        pass
    
    @abstractmethod
    async def route_analysis_results(self, results: List[Any]) -> List[Any]:
        """路由分析结果

        Args:
            results: 分析结果列表

        Returns:
            List[Any]: 路由后的结果列表
        """
        pass
    
    @abstractmethod
    async def detect_signal_conflicts(self, signals: List[Any]) -> Dict[str, Any]:
        """检测信号冲突
        
        Args:
            signals: 信号列表
            
        Returns:
            Dict[str, Any]: 冲突检测结果
        """
        pass
    
    @abstractmethod
    async def resolve_signal_conflicts(self, conflicts: Dict[str, Any]) -> List[Any]:
        """解决信号冲突
        
        Args:
            conflicts: 冲突信息
            
        Returns:
            List[Any]: 解决后的信号列表
        """
        pass


class IDataFlowManager(ABC):
    """数据流管理器接口
    
    负责统一管理三层系统的数据流、缓存策略和质量控制
    """
    
    @abstractmethod
    async def orchestrate_data_pipeline(self) -> DataFlowStatus:
        """编排数据管道
        
        Returns:
            DataFlowStatus: 数据管道状态
        """
        pass
    
    @abstractmethod
    async def manage_data_dependencies(self) -> Dict[str, Any]:
        """管理数据依赖关系
        
        Returns:
            Dict[str, Any]: 依赖关系映射
        """
        pass
    
    @abstractmethod
    async def optimize_data_flow(self) -> Dict[str, Any]:
        """优化数据流
        
        Returns:
            Dict[str, Any]: 优化结果
        """
        pass
    
    @abstractmethod
    async def manage_intelligent_cache(self) -> Dict[str, Any]:
        """管理智能缓存
        
        Returns:
            Dict[str, Any]: 缓存状态
        """
        pass
    
    @abstractmethod
    async def monitor_data_quality(self) -> Dict[str, Any]:
        """监控数据质量
        
        Returns:
            Dict[str, Any]: 质量报告
        """
        pass


class ITemporalValidationCoordinator(ABC):
    """时间维度验证协调器接口
    
    负责协调回测引擎和量子沙盘的双重验证
    """
    
    @abstractmethod
    async def coordinate_dual_validation(self, signals: List[Any]) -> ValidationResult:
        """协调双重验证
        
        Args:
            signals: 待验证信号列表
            
        Returns:
            ValidationResult: 验证结果
        """
        pass
    
    @abstractmethod
    async def synchronize_validation_timing(self) -> bool:
        """同步验证时机
        
        Returns:
            bool: 同步是否成功
        """
        pass
    
    @abstractmethod
    async def aggregate_validation_results(self, 
                                         historical: Dict[str, Any],
                                         forward: Dict[str, Any]) -> Dict[str, Any]:
        """聚合验证结果
        
        Args:
            historical: 历史验证结果
            forward: 前瞻验证结果
            
        Returns:
            Dict[str, Any]: 聚合结果
        """
        pass
    
    @abstractmethod
    async def calculate_confidence_score(self, results: Dict[str, Any]) -> float:
        """计算置信度评分
        
        Args:
            results: 验证结果
            
        Returns:
            float: 置信度评分 (0-1)
        """
        pass


class ISystemAdapter(ABC):
    """系统适配器接口
    
    定义各层系统适配器的通用接口
    """
    
    @abstractmethod
    async def connect_to_system(self) -> bool:
        """连接到目标系统
        
        Returns:
            bool: 连接是否成功
        """
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """健康检查
        
        Returns:
            bool: 系统是否健康
        """
        pass
    
    @abstractmethod
    async def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态
        
        Returns:
            Dict[str, Any]: 系统状态信息
        """
        pass
    
    @abstractmethod
    async def send_request(self, request: Any) -> Any:
        """发送请求
        
        Args:
            request: 请求对象
            
        Returns:
            Any: 响应结果
        """
        pass
    
    @abstractmethod
    async def handle_response(self, response: Any) -> Any:
        """处理响应
        
        Args:
            response: 响应对象
            
        Returns:
            Any: 处理后的结果
        """
        pass
