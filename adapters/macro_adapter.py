"""
宏观系统适配器实现

负责与宏观战略系统的HTTP接口适配和通信管理。
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

from interfaces import ISystemAdapter
from config import IntegrationConfig
from exceptions import AdapterException, ConnectionException, HealthCheckException
from adapters.http_client import HttpClient

logger = logging.getLogger(__name__)


class MacroAdapter(ISystemAdapter):
    """宏观系统适配器实现类

    提供与宏观战略系统的标准化HTTP接口，
    负责宏观分析请求的发送和结果的接收处理。
    """

    def __init__(self, config: IntegrationConfig):
        """初始化宏观适配器

        Args:
            config: 集成配置对象
        """
        self.config = config
        self._http_client = HttpClient('macro', config)
        self._is_connected = False
        self._last_health_check = None

        logger.info("MacroAdapter initialized with HTTP client")

    async def connect_to_system(self) -> bool:
        """连接到宏观系统

        Returns:
            bool: 连接是否成功
        """
        try:
            logger.info("Connecting to macro system...")

            # 启动HTTP客户端
            await self._http_client.start()

            # 执行健康检查
            health_result = await self.health_check()

            if health_result:
                self._is_connected = True
                logger.info("Successfully connected to macro system")
                return True
            else:
                logger.error("Failed to connect to macro system - health check failed")
                return False

        except Exception as e:
            logger.error(f"Failed to connect to macro system: {e}")
            self._is_connected = False
            return False

    async def disconnect_from_system(self) -> bool:
        """断开与宏观系统的连接

        Returns:
            bool: 断开是否成功
        """
        try:
            logger.info("Disconnecting from macro system...")

            # 停止HTTP客户端
            await self._http_client.stop()

            self._is_connected = False
            logger.info("Successfully disconnected from macro system")
            return True

        except Exception as e:
            logger.error(f"Failed to disconnect from macro system: {e}")
            return False

    async def health_check(self) -> bool:
        """执行健康检查

        Returns:
            bool: 系统是否健康
        """
        try:
            health_data = await self._http_client.health_check()
            self._last_health_check = datetime.now()

            if health_data.get('status') == 'healthy':
                logger.debug("Macro system health check passed")
                return True
            else:
                logger.warning("Macro system health check failed")
                return False

        except Exception as e:
            logger.error(f"Macro system health check error: {e}")
            raise HealthCheckException(f"Health check failed: {e}")

    async def send_request(self, request: Any) -> Any:
        """发送请求到宏观系统

        Args:
            request: 请求对象

        Returns:
            Any: 响应结果
        """
        if not self._is_connected:
            raise ConnectionException("Not connected to macro system")

        try:
            # 根据请求类型选择合适的端点
            action = request.get('action', 'unknown')

            if action == 'get_state':
                return await self._http_client.get('state')
            elif action == 'trigger_analysis':
                return await self._http_client.post('analysis', request.get('params', {}))
            elif action == 'get_indicators':
                params = {'indicators': request.get('indicators', [])}
                return await self._http_client.get('indicators', params)
            elif action == 'get_cycle_position':
                return await self._http_client.get('cycle/position')
            else:
                raise AdapterException(f"Unknown action: {action}")

        except Exception as e:
            logger.error(f"Macro request failed: {e}")
            raise AdapterException(f"Request failed: {e}")

    async def get_macro_state(self) -> Dict[str, Any]:
        """获取宏观状态

        Returns:
            Dict[str, Any]: 宏观状态数据
        """
        try:
            response = await self._http_client.get('state')
            return response.get('data', {})

        except Exception as e:
            logger.error(f"Failed to get macro state: {e}")
            raise

    async def trigger_macro_analysis(self, analysis_params: Dict[str, Any] = None) -> Dict[str, Any]:
        """触发宏观分析

        Args:
            analysis_params: 分析参数

        Returns:
            Dict[str, Any]: 分析结果
        """
        try:
            response = await self._http_client.post('analysis', analysis_params or {})
            return response.get('data', {})

        except Exception as e:
            logger.error(f"Failed to trigger macro analysis: {e}")
            raise

    async def get_economic_indicators(self, indicators: List[str] = None) -> Dict[str, Any]:
        """获取经济指标

        Args:
            indicators: 指标列表

        Returns:
            Dict[str, Any]: 经济指标数据
        """
        try:
            params = {'indicators': indicators} if indicators else {}
            response = await self._http_client.get('indicators', params)
            return response.get('data', {})

        except Exception as e:
            logger.error(f"Failed to get economic indicators: {e}")
            raise

    async def get_market_cycle_position(self) -> Dict[str, Any]:
        """获取市场周期位置

        Returns:
            Dict[str, Any]: 市场周期位置数据
        """
        try:
            response = await self._http_client.get('cycle/position')
            return response.get('data', {})

        except Exception as e:
            logger.error(f"Failed to get market cycle position: {e}")
            raise

    def get_connection_statistics(self) -> Dict[str, Any]:
        """获取连接统计信息"""
        stats = self._http_client.get_statistics()
        return {
            'adapter_type': 'macro',
            'is_connected': self._is_connected,
            'last_health_check': self._last_health_check.isoformat() if self._last_health_check else None,
            'http_statistics': stats
        }



















    async def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        try:
            healthy = await self.health_check()
        except Exception:
            healthy = False
        return {
            'system': 'macro',
            'status': 'healthy' if healthy else 'unhealthy',
            'is_connected': self._is_connected,
            'last_health_check': self._last_health_check.isoformat() if self._last_health_check else None,
            'http_statistics': self._http_client.get_statistics()
        }

    async def handle_response(self, response: Any) -> Any:
        """处理响应: 基础格式校验后透传"""
        if not isinstance(response, dict):
            raise AdapterException("MacroAdapter", "Invalid response format")
        return response
