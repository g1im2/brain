"""
战术系统适配器实现

负责与个股战术系统的接口适配和通信管理。
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

from ..interfaces import ISystemAdapter
from ..config import IntegrationConfig
from ..exceptions import AdapterException, ConnectionException, HealthCheckException

logger = logging.getLogger(__name__)


class TacticalAdapter(ISystemAdapter):
    """战术系统适配器实现类
    
    提供与个股战术系统的标准化接口，
    负责战术分析请求的发送和结果的接收处理。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化战术适配器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_connected = False
        self._connection_pool = None
        self._last_health_check = None
        
        # 战术系统配置
        self._tactical_system_config = {
            'endpoint': 'tactical_analysis',
            'timeout': self.config.adapter.request_timeout,
            'max_retries': self.config.adapter.max_retries,
            'retry_delay': self.config.adapter.retry_delay
        }
        
        # 请求统计
        self._request_statistics = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'average_response_time': 0.0
        }
        
        # 分析器状态
        self._analyzer_status = {
            'livermore_analyzer': 'active',
            'multi_indicator_analyzer': 'active',
            'dow_theory_analyzer': 'active',
            'hong_hao_analyzer': 'active'
        }
        
        logger.info("TacticalAdapter initialized")
    
    async def connect_to_system(self) -> bool:
        """连接到战术系统
        
        Returns:
            bool: 连接是否成功
        """
        try:
            logger.info("Connecting to tactical analysis system...")
            
            # 模拟连接过程
            await asyncio.sleep(0.1)
            
            # 初始化连接池
            if self.config.adapter.enable_connection_pooling:
                self._connection_pool = await self._create_connection_pool()
            
            # 检查分析器状态
            await self._check_analyzers_status()
            
            # 执行初始健康检查
            health_status = await self.health_check()
            if not health_status:
                raise ConnectionException("TacticalAdapter", "tactical_analysis", "Health check failed")
            
            self._is_connected = True
            logger.info("Successfully connected to tactical analysis system")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to tactical analysis system: {e}")
            self._is_connected = False
            raise ConnectionException("TacticalAdapter", "tactical_analysis", str(e))
    
    async def disconnect_from_system(self) -> bool:
        """断开与战术系统的连接
        
        Returns:
            bool: 断开是否成功
        """
        try:
            if self._connection_pool:
                await self._close_connection_pool()
                self._connection_pool = None
            
            self._is_connected = False
            logger.info("Disconnected from tactical analysis system")
            return True
            
        except Exception as e:
            logger.error(f"Failed to disconnect from tactical analysis system: {e}")
            return False
    
    async def health_check(self) -> bool:
        """健康检查
        
        Returns:
            bool: 系统是否健康
        """
        try:
            start_time = datetime.now()
            
            # 模拟健康检查请求
            health_request = {
                'type': 'health_check',
                'timestamp': start_time.isoformat(),
                'adapter': 'tactical_adapter'
            }
            
            # 发送健康检查请求
            response = await self._send_health_check_request(health_request)
            
            # 验证响应
            is_healthy = self._validate_health_response(response)
            
            self._last_health_check = datetime.now()
            response_time = (self._last_health_check - start_time).total_seconds()
            
            if is_healthy:
                logger.debug(f"Tactical system health check passed, response time: {response_time:.3f}s")
            else:
                logger.warning("Tactical system health check failed")
                raise HealthCheckException("TacticalAdapter", "tactical_analysis", "unhealthy")
            
            return is_healthy
            
        except HealthCheckException:
            raise
        except Exception as e:
            logger.error(f"Tactical system health check error: {e}")
            raise HealthCheckException("TacticalAdapter", "tactical_analysis", str(e))
    
    async def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态
        
        Returns:
            Dict[str, Any]: 系统状态信息
        """
        try:
            status_request = {
                'type': 'status_request',
                'timestamp': datetime.now().isoformat()
            }
            
            response = await self.send_request(status_request)
            
            return {
                'system': 'tactical_analysis',
                'status': response.get('status', 'unknown'),
                'last_health_check': self._last_health_check.isoformat() if self._last_health_check else None,
                'is_connected': self._is_connected,
                'analyzer_status': self._analyzer_status.copy(),
                'statistics': self._request_statistics.copy(),
                'response_data': response
            }
            
        except Exception as e:
            logger.error(f"Failed to get tactical system status: {e}")
            return {
                'system': 'tactical_analysis',
                'status': 'error',
                'error': str(e),
                'is_connected': self._is_connected,
                'analyzer_status': self._analyzer_status.copy()
            }
    
    async def send_request(self, request: Any) -> Any:
        """发送请求
        
        Args:
            request: 请求对象
            
        Returns:
            Any: 响应结果
        """
        if not self._is_connected:
            raise AdapterException("TacticalAdapter", "Not connected to tactical system")
        
        start_time = datetime.now()
        retry_count = 0
        max_retries = self._tactical_system_config['max_retries']
        
        while retry_count <= max_retries:
            try:
                self._request_statistics['total_requests'] += 1
                
                # 发送请求到战术系统
                response = await self._send_tactical_request(request)
                
                # 处理响应
                processed_response = await self.handle_response(response)
                
                # 更新统计
                response_time = (datetime.now() - start_time).total_seconds()
                self._update_request_statistics(True, response_time)
                
                logger.debug(f"Tactical request completed successfully, response time: {response_time:.3f}s")
                return processed_response
                
            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    self._update_request_statistics(False, 0)
                    logger.error(f"Tactical request failed after {max_retries} retries: {e}")
                    raise AdapterException("TacticalAdapter", f"Request failed: {e}")
                
                # 等待重试
                await asyncio.sleep(self._tactical_system_config['retry_delay'] * retry_count)
                logger.warning(f"Tactical request failed, retrying ({retry_count}/{max_retries}): {e}")
    
    async def handle_response(self, response: Any) -> Any:
        """处理响应
        
        Args:
            response: 响应对象
            
        Returns:
            Any: 处理后的结果
        """
        try:
            # 验证响应格式
            if not self._validate_response_format(response):
                raise AdapterException("TacticalAdapter", "Invalid response format")
            
            # 提取交易信号数据
            trading_signals = self._extract_trading_signals(response)
            
            # 标准化数据格式
            standardized_signals = self._standardize_trading_signals(trading_signals)
            
            return standardized_signals
            
        except Exception as e:
            logger.error(f"Failed to handle tactical response: {e}")
            raise AdapterException("TacticalAdapter", f"Response handling failed: {e}")
    
    async def request_tactical_analysis(self, portfolio_instruction: Dict[str, Any], 
                                      symbols: List[str]) -> List[Dict[str, Any]]:
        """请求战术分析
        
        Args:
            portfolio_instruction: 组合指令
            symbols: 股票代码列表
            
        Returns:
            List[Dict[str, Any]]: 交易信号列表
        """
        try:
            request = {
                'type': 'tactical_analysis',
                'portfolio_instruction': portfolio_instruction,
                'symbols': symbols,
                'timestamp': datetime.now().isoformat(),
                'request_id': f"tactical_req_{datetime.now().timestamp()}"
            }
            
            response = await self.send_request(request)
            
            logger.info(f"Tactical analysis completed for {len(symbols)} symbols, request: {request['request_id']}")
            return response
            
        except Exception as e:
            logger.error(f"Tactical analysis request failed: {e}")
            raise AdapterException("TacticalAdapter", f"Analysis request failed: {e}")
    
    async def request_validation(self, signals: List[Dict[str, Any]], 
                               validation_type: str = "dual") -> Dict[str, Any]:
        """请求信号验证
        
        Args:
            signals: 交易信号列表
            validation_type: 验证类型 (historical, forward, dual)
            
        Returns:
            Dict[str, Any]: 验证结果
        """
        try:
            request = {
                'type': 'signal_validation',
                'signals': signals,
                'validation_type': validation_type,
                'timestamp': datetime.now().isoformat(),
                'request_id': f"validation_req_{datetime.now().timestamp()}"
            }
            
            response = await self.send_request(request)
            
            logger.info(f"Signal validation completed for {len(signals)} signals, request: {request['request_id']}")
            return response
            
        except Exception as e:
            logger.error(f"Signal validation request failed: {e}")
            raise AdapterException("TacticalAdapter", f"Validation request failed: {e}")
    
    def get_request_statistics(self) -> Dict[str, Any]:
        """获取请求统计信息
        
        Returns:
            Dict[str, Any]: 请求统计
        """
        return self._request_statistics.copy()
    
    def get_analyzer_status(self) -> Dict[str, str]:
        """获取分析器状态
        
        Returns:
            Dict[str, str]: 分析器状态
        """
        return self._analyzer_status.copy()
    
    # 私有方法实现
    async def _create_connection_pool(self):
        """创建连接池"""
        # 模拟连接池创建
        logger.debug("Created connection pool for tactical system")
        return {"pool_size": self.config.adapter.pool_size, "active_connections": 0}
    
    async def _close_connection_pool(self):
        """关闭连接池"""
        # 模拟连接池关闭
        logger.debug("Closed connection pool for tactical system")
    
    async def _check_analyzers_status(self):
        """检查分析器状态"""
        # 模拟检查各个分析器状态
        for analyzer in self._analyzer_status:
            # 这里应该实际检查分析器状态
            self._analyzer_status[analyzer] = 'active'
    
    async def _send_health_check_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """发送健康检查请求"""
        # 模拟健康检查请求
        await asyncio.sleep(0.01)
        return {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'system': 'tactical_analysis',
            'version': '1.0.0',
            'analyzers': self._analyzer_status
        }
    
    def _validate_health_response(self, response: Dict[str, Any]) -> bool:
        """验证健康检查响应"""
        return (response.get('status') == 'healthy' and 
                'timestamp' in response and 
                response.get('system') == 'tactical_analysis')
    
    async def _send_tactical_request(self, request: Any) -> Any:
        """发送战术系统请求"""
        # 模拟向战术系统发送请求
        await asyncio.sleep(0.15)  # 模拟分析时间
        
        # 根据请求类型返回模拟响应
        if isinstance(request, dict):
            request_type = request.get('type', 'unknown')
            
            if request_type == 'tactical_analysis':
                symbols = request.get('symbols', ['000001.SZ'])
                signals = []
                
                for symbol in symbols:
                    # 模拟生成交易信号
                    signal = {
                        'symbol': symbol,
                        'signal_type': 'buy',
                        'strength': 0.75,
                        'confidence': 0.8,
                        'target_weight': 0.05,
                        'stop_loss': None,
                        'take_profit': None,
                        'analyzer_scores': {
                            'livermore': 0.8,
                            'multi_indicator': 0.7,
                            'dow_theory': 0.75,
                            'hong_hao': 0.8
                        }
                    }
                    signals.append(signal)
                
                return {
                    'status': 'success',
                    'data': {
                        'signals': signals,
                        'analysis_summary': {
                            'total_signals': len(signals),
                            'buy_signals': len([s for s in signals if s['signal_type'] == 'buy']),
                            'sell_signals': len([s for s in signals if s['signal_type'] == 'sell']),
                            'average_confidence': sum(s['confidence'] for s in signals) / len(signals) if signals else 0
                        }
                    },
                    'timestamp': datetime.now().isoformat()
                }
            
            elif request_type == 'signal_validation':
                signals = request.get('signals', [])
                validation_type = request.get('validation_type', 'dual')
                
                return {
                    'status': 'success',
                    'data': {
                        'validation_type': validation_type,
                        'validation_score': 0.85,
                        'confidence_adjustment': 0.05,
                        'risk_assessment': {
                            'overall_risk': 0.3,
                            'concentration_risk': 0.2,
                            'market_risk': 0.4
                        },
                        'recommendations': [
                            "信号质量良好，建议执行",
                            "注意控制仓位集中度"
                        ],
                        'validated_signals': len(signals)
                    },
                    'timestamp': datetime.now().isoformat()
                }
            
            elif request_type == 'status_request':
                return {
                    'status': 'running',
                    'last_analysis': datetime.now().isoformat(),
                    'active_analyzers': list(self._analyzer_status.keys()),
                    'processed_symbols': 100,
                    'generated_signals': 25
                }
        
        return {'status': 'success', 'data': {}}
    
    def _validate_response_format(self, response: Any) -> bool:
        """验证响应格式"""
        if not isinstance(response, dict):
            return False
        
        required_fields = ['status']
        return all(field in response for field in required_fields)
    
    def _extract_trading_signals(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """提取交易信号数据"""
        if response.get('status') != 'success':
            raise AdapterException("TacticalAdapter", f"Tactical analysis failed: {response.get('error', 'Unknown error')}")
        
        data = response.get('data', {})
        return data.get('signals', [])
    
    def _standardize_trading_signals(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """标准化交易信号数据"""
        standardized_signals = []
        
        for signal in signals:
            standardized = {
                'symbol': signal.get('symbol', ''),
                'signal_type': signal.get('signal_type', 'hold'),
                'strength': float(signal.get('strength', 0.5)),
                'confidence': float(signal.get('confidence', 0.5)),
                'target_weight': float(signal.get('target_weight', 0.0)),
                'stop_loss': signal.get('stop_loss'),
                'take_profit': signal.get('take_profit'),
                'timestamp': datetime.now().isoformat(),
                'source': 'tactical_analysis_system',
                'analyzer_scores': signal.get('analyzer_scores', {}),
                'metadata': {
                    'original_signal': signal
                }
            }
            
            # 验证数据范围
            if not 0 <= standardized['strength'] <= 1:
                standardized['strength'] = max(0, min(1, standardized['strength']))
            
            if not 0 <= standardized['confidence'] <= 1:
                standardized['confidence'] = max(0, min(1, standardized['confidence']))
            
            if not 0 <= standardized['target_weight'] <= 1:
                standardized['target_weight'] = max(0, min(1, standardized['target_weight']))
            
            standardized_signals.append(standardized)
        
        return standardized_signals
    
    def _update_request_statistics(self, success: bool, response_time: float) -> None:
        """更新请求统计"""
        if success:
            self._request_statistics['successful_requests'] += 1
        else:
            self._request_statistics['failed_requests'] += 1
        
        # 更新平均响应时间
        total_successful = self._request_statistics['successful_requests']
        if total_successful > 0 and success:
            current_avg = self._request_statistics['average_response_time']
            self._request_statistics['average_response_time'] = (
                (current_avg * (total_successful - 1) + response_time) / total_successful
            )
