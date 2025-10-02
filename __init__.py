"""
三层金融交易系统集成层模块

提供系统协调、信号路由、数据流管理等核心集成功能，
实现顶层宏观战略、中间层组合管理、底层策略分析系统的统一协调。

注意：为避免在包导入阶段引入大量依赖并导致测试环境导入失败，
此文件不再进行子模块的聚合导入。请从对应子模块中显式导入所需组件，例如：
- from services.brain.config import IntegrationConfig
- from services.brain.coordinators.system_coordinator import SystemCoordinator
- from services.brain.routers.signal_router import SignalRouter
- from services.brain.managers.data_flow_manager import DataFlowManager
"""

__version__ = "1.0.0"
__author__ = "Financial Trading System Team"

# 为保持向前兼容，保留 __all__，但不在此聚合导出，避免重型导入
__all__: list[str] = []
