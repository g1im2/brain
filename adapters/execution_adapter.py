"""
Execution服务适配器

负责与Execution服务（股票技术分析服务）的通信
"""

import logging
from typing import Dict, Any, List, Optional

from config import IntegrationConfig
from adapters.http_client import HttpClient

logger = logging.getLogger(__name__)


class ExecutionAdapter:
    """Execution服务适配器"""

    def __init__(self, config: IntegrationConfig):
        """初始化适配器

        Args:
            config: 集成配置对象
        """
        self.config = config
        self._http_client = HttpClient('execution', config)
        logger.info("ExecutionAdapter initialized with HTTP client")
    
    async def trigger_batch_analysis(self, analysis_params: Dict[str, Any]) -> Dict[str, Any]:
        """触发批量股票分析

        支持两种模式：
        1. 全量分析：不传入symbols参数（或传入None），Execution服务会自动从数据库获取所有股票
        2. 指定分析：传入symbols列表，只分析指定的股票

        Args:
            analysis_params: 分析参数
                - symbols: List[str] | None - 股票代码列表（可选，不传入则分析所有股票）
                - analyzers: List[str] - 分析器列表（如['livermore', 'multi_indicator']或['all']）
                - config: Dict - 配置参数
                    - parallel_limit: int - 并发限制
                    - save_all_dates: bool - 是否保存所有历史日期的分析结果
                    - cache_enabled: bool - 是否启用缓存

        Returns:
            Dict[str, Any]: 分析结果
                - task_id: str - 任务ID
                - status: str - 任务状态
                - message: str - 消息

        Raises:
            Exception: 调用失败时抛出异常
        """
        try:
            symbols = analysis_params.get('symbols', None)
            analyzers = analysis_params.get('analyzers', ['all'])
            config = analysis_params.get('config', {})

            # 构建请求体
            request_body = {
                'analyzers': analyzers,
                'config': config
            }

            # 只有在明确提供symbols时才添加到请求体中
            if symbols is not None:
                request_body['symbols'] = symbols
                logger.info(f"Triggering batch analysis for {len(symbols)} symbols with analyzers: {analyzers}")
            else:
                logger.info(f"Triggering batch analysis for ALL stocks with analyzers: {analyzers}")

            # 调用Execution服务的批量分析API
            response = await self._http_client.post('analyze/batch', request_body)

            logger.info(f"Batch analysis triggered successfully: {response}")
            return response.get('data', {})

        except Exception as e:
            logger.error(f"Failed to trigger batch analysis: {e}")
            raise
    
    async def get_analysis_result(self, task_id: str) -> Dict[str, Any]:
        """获取分析结果
        
        Args:
            task_id: 任务ID
        
        Returns:
            Dict[str, Any]: 分析结果
        
        Raises:
            Exception: 调用失败时抛出异常
        """
        try:
            response = await self._http_client.get(f'analyze/batch/{task_id}')
            return response.get('data', {})
            
        except Exception as e:
            logger.error(f"Failed to get analysis result for task {task_id}: {e}")
            raise
    
    async def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """获取任务状态
        
        Args:
            task_id: 任务ID
        
        Returns:
            Dict[str, Any]: 任务状态
                - task_id: str
                - status: str
                - progress: int
                - created_at: str
                - updated_at: str
        
        Raises:
            Exception: 调用失败时抛出异常
        """
        try:
            response = await self._http_client.get(f'tasks/{task_id}/status')
            return response.get('data', {})
            
        except Exception as e:
            logger.error(f"Failed to get task status for {task_id}: {e}")
            raise
    
    async def analyze_single(self, symbol: str, analyzers: List[str] = None, 
                            config: Dict[str, Any] = None) -> Dict[str, Any]:
        """分析单个股票
        
        Args:
            symbol: 股票代码
            analyzers: 分析器列表
            config: 配置参数
        
        Returns:
            Dict[str, Any]: 分析结果
        
        Raises:
            Exception: 调用失败时抛出异常
        """
        try:
            request_body = {
                'symbol': symbol,
                'analyzers': analyzers or ['all'],
                'config': config or {}
            }
            
            response = await self._http_client.post('analyze/single', request_body)
            return response.get('data', {})
            
        except Exception as e:
            logger.error(f"Failed to analyze symbol {symbol}: {e}")
            raise
    
    async def get_analysis_history(self, symbol: str, analyzer_type: str = None,
                                   page: int = 1, page_size: int = 20) -> Dict[str, Any]:
        """获取分析历史
        
        Args:
            symbol: 股票代码
            analyzer_type: 分析器类型（可选）
            page: 页码
            page_size: 每页大小
        
        Returns:
            Dict[str, Any]: 分析历史
        
        Raises:
            Exception: 调用失败时抛出异常
        """
        try:
            params = {
                'symbol': symbol,
                'page': page,
                'page_size': page_size
            }
            
            if analyzer_type:
                params['analyzer_type'] = analyzer_type
            
            response = await self._http_client.get('analyze/history', params)
            return response.get('data', {})
            
        except Exception as e:
            logger.error(f"Failed to get analysis history for {symbol}: {e}")
            raise
    
    async def health_check(self) -> bool:
        """健康检查
        
        Returns:
            bool: 服务是否健康
        """
        try:
            response = await self._http_client.get('health')
            return response.get('status') == 'healthy'
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

