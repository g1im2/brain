"""
Flowhub数据抓取服务适配器实现

负责与Flowhub数据抓取服务的接口适配和通信管理。
提供批量数据抓取、任务状态监控、结果获取等功能。
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

from ..interfaces import ISystemAdapter
from ..config import IntegrationConfig
from ..exceptions import AdapterException, ConnectionException, HealthCheckException
from .http_client import HttpClient
from .flowhub_request_mapper import FlowhubRequestMapper

logger = logging.getLogger(__name__)


class FlowhubAdapter(ISystemAdapter):
    """Flowhub数据抓取服务适配器实现类
    
    提供与Flowhub数据抓取服务的标准化接口，
    负责批量数据抓取任务的创建、监控和结果获取。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化Flowhub适配器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_connected = False
        self._last_health_check = None
        
        # HTTP客户端和请求映射器
        self._http_client: Optional[HttpClient] = None
        self._request_mapper = FlowhubRequestMapper()
        
        # Flowhub服务配置
        self._flowhub_system_config = {
            'endpoint': 'data_fetch',
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
        
        # 任务状态缓存
        self._job_status_cache = {}
        
        logger.info("FlowhubAdapter initialized with HTTP client integration")
    
    async def connect_to_system(self) -> bool:
        """连接到Flowhub数据抓取服务
        
        Returns:
            bool: 连接是否成功
        """
        try:
            logger.info("Connecting to Flowhub data fetch service...")
            
            # 初始化HTTP客户端
            if not self._http_client:
                self._http_client = HttpClient('flowhub', self.config)
                await self._http_client.start()
                logger.info("HTTP client for flowhub service initialized")
            
            # 执行初始健康检查
            health_status = await self.health_check()
            if not health_status:
                raise ConnectionException("FlowhubAdapter", "data_fetch", "Health check failed")
            
            self._is_connected = True
            logger.info("Successfully connected to Flowhub data fetch service")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Flowhub service: {e}")
            self._is_connected = False
            raise ConnectionException("FlowhubAdapter", "data_fetch", str(e))
    
    async def disconnect_from_system(self) -> bool:
        """断开与Flowhub服务的连接
        
        Returns:
            bool: 断开是否成功
        """
        try:
            logger.info("Disconnecting from Flowhub data fetch service...")
            
            if self._http_client:
                await self._http_client.stop()
                self._http_client = None
            
            self._is_connected = False
            logger.info("Successfully disconnected from Flowhub service")
            return True
            
        except Exception as e:
            logger.error(f"Failed to disconnect from Flowhub service: {e}")
            return False
    
    async def health_check(self) -> bool:
        """检查Flowhub服务健康状态
        
        Returns:
            bool: 服务是否健康
        """
        try:
            if not self._http_client:
                return False
            
            # 调用健康检查端点
            response = await self._http_client.get('/health')
            
            # 检查响应状态
            is_healthy = response.get('status') == 'healthy'
            
            self._last_health_check = datetime.now()
            
            if is_healthy:
                logger.debug("Flowhub service health check passed")
            else:
                logger.warning(f"Flowhub service health check failed: {response}")
            
            return is_healthy
            
        except Exception as e:
            logger.error(f"Flowhub service health check failed: {e}")
            return False
    
    async def create_batch_stock_data_job(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """创建批量股票数据抓取任务
        
        Args:
            request: 批量股票数据抓取请求
            
        Returns:
            Dict[str, Any]: 任务创建结果
        """
        try:
            if not self._is_connected:
                raise AdapterException("FlowhubAdapter", "Not connected to Flowhub service")
            
            # 映射请求格式
            flowhub_request = self._request_mapper.map_batch_stock_data_request(request)
            
            logger.info(f"Creating batch stock data job with request: {flowhub_request}")
            
            # 发送HTTP请求
            response = await self._http_client.post('/batch-stock-data', json=flowhub_request)
            
            # 映射响应格式
            mapped_response = self._request_mapper.map_job_response(response, 'batch_stock_data')
            
            # 更新统计
            self._request_statistics['total_requests'] += 1
            self._request_statistics['successful_requests'] += 1
            
            logger.info(f"Batch stock data job created successfully: {mapped_response['job_id']}")
            return mapped_response
            
        except Exception as e:
            self._request_statistics['total_requests'] += 1
            self._request_statistics['failed_requests'] += 1
            logger.error(f"Failed to create batch stock data job: {e}")
            raise AdapterException("FlowhubAdapter", f"Batch stock data job creation failed: {e}")
    
    async def create_batch_basic_data_job(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """创建批量基础数据抓取任务
        
        Args:
            request: 批量基础数据抓取请求
            
        Returns:
            Dict[str, Any]: 任务创建结果
        """
        try:
            if not self._is_connected:
                raise AdapterException("FlowhubAdapter", "Not connected to Flowhub service")
            
            # 映射请求格式
            flowhub_request = self._request_mapper.map_batch_basic_data_request(request)
            
            logger.info(f"Creating batch basic data job with request: {flowhub_request}")
            
            # 发送HTTP请求
            response = await self._http_client.post('/batch-basic-data', json=flowhub_request)
            
            # 映射响应格式
            mapped_response = self._request_mapper.map_job_response(response, 'batch_basic_data')
            
            # 更新统计
            self._request_statistics['total_requests'] += 1
            self._request_statistics['successful_requests'] += 1
            
            logger.info(f"Batch basic data job created successfully: {mapped_response['job_id']}")
            return mapped_response
            
        except Exception as e:
            self._request_statistics['total_requests'] += 1
            self._request_statistics['failed_requests'] += 1
            logger.error(f"Failed to create batch basic data job: {e}")
            raise AdapterException("FlowhubAdapter", f"Batch basic data job creation failed: {e}")
    
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """获取任务执行状态
        
        Args:
            job_id: 任务ID
            
        Returns:
            Dict[str, Any]: 任务状态信息
        """
        try:
            if not self._is_connected:
                raise AdapterException("FlowhubAdapter", "Not connected to Flowhub service")
            
            logger.debug(f"Getting status for job: {job_id}")
            
            # 发送HTTP请求
            response = await self._http_client.get(f'/{job_id}/status')
            
            # 映射响应格式
            mapped_response = self._request_mapper.map_status_response(response)
            
            # 缓存状态
            self._job_status_cache[job_id] = mapped_response
            
            logger.debug(f"Job {job_id} status: {mapped_response['status']}")
            return mapped_response
            
        except Exception as e:
            logger.error(f"Failed to get job status for {job_id}: {e}")
            raise AdapterException("FlowhubAdapter", f"Get job status failed: {e}")
    
    async def get_job_result(self, job_id: str) -> Dict[str, Any]:
        """获取任务执行结果
        
        Args:
            job_id: 任务ID
            
        Returns:
            Dict[str, Any]: 任务执行结果
        """
        try:
            if not self._is_connected:
                raise AdapterException("FlowhubAdapter", "Not connected to Flowhub service")
            
            logger.debug(f"Getting result for job: {job_id}")
            
            # 发送HTTP请求
            response = await self._http_client.get(f'/{job_id}/result')
            
            # 映射响应格式
            mapped_response = self._request_mapper.map_result_response(response)
            
            logger.info(f"Job {job_id} result retrieved successfully")
            return mapped_response
            
        except Exception as e:
            logger.error(f"Failed to get job result for {job_id}: {e}")
            raise AdapterException("FlowhubAdapter", f"Get job result failed: {e}")
    
    async def wait_for_job_completion(self, job_id: str, timeout: int = 3600, 
                                    check_interval: int = 30) -> Dict[str, Any]:
        """等待任务完成
        
        Args:
            job_id: 任务ID
            timeout: 超时时间（秒）
            check_interval: 检查间隔（秒）
            
        Returns:
            Dict[str, Any]: 最终任务结果
        """
        try:
            logger.info(f"Waiting for job {job_id} to complete (timeout: {timeout}s)")
            
            start_time = datetime.now()
            
            while True:
                # 检查任务状态
                status_response = await self.get_job_status(job_id)
                status = status_response.get('status')
                
                if status in ['succeeded', 'failed']:
                    # 任务完成，获取结果
                    result = await self.get_job_result(job_id)
                    logger.info(f"Job {job_id} completed with status: {status}")
                    return result
                
                # 检查超时
                elapsed = (datetime.now() - start_time).total_seconds()
                if elapsed >= timeout:
                    logger.warning(f"Job {job_id} timed out after {timeout} seconds")
                    raise AdapterException("FlowhubAdapter", f"Job {job_id} timed out")
                
                # 等待下次检查
                await asyncio.sleep(check_interval)
                
        except Exception as e:
            logger.error(f"Failed to wait for job completion: {e}")
            raise AdapterException("FlowhubAdapter", f"Wait for job completion failed: {e}")
    
    def get_request_statistics(self) -> Dict[str, Any]:
        """获取请求统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        return self._request_statistics.copy()
    
    def get_cached_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """获取缓存的任务状态

        Args:
            job_id: 任务ID

        Returns:
            Optional[Dict[str, Any]]: 缓存的状态信息
        """
        return self._job_status_cache.get(job_id)

    async def cancel_job(self, job_id: str) -> bool:
        """取消任务

        Args:
            job_id: 任务ID

        Returns:
            bool: 取消是否成功
        """
        try:
            if not self._is_connected:
                raise AdapterException("FlowhubAdapter", "Not connected to Flowhub service")

            logger.info(f"Cancelling job: {job_id}")

            # 发送DELETE请求取消任务
            await self._http_client.delete(f'/{job_id}')

            # 从缓存中移除
            self._job_status_cache.pop(job_id, None)

            logger.info(f"Job {job_id} cancelled successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            return False

    async def create_daily_data_fetch_job(self, symbols: List[str] = None,
                                        incremental: bool = True) -> Dict[str, Any]:
        """创建每日数据抓取任务（用于定时调度）

        Args:
            symbols: 股票代码列表，None表示所有股票
            incremental: 是否增量更新

        Returns:
            Dict[str, Any]: 任务创建结果
        """
        try:
            # 构建请求
            request = self._request_mapper.get_default_batch_request('batch_stock_data')
            request['incremental'] = incremental

            if symbols:
                request['symbols'] = symbols

            # 创建批量股票数据抓取任务
            return await self.create_batch_stock_data_job(request)

        except Exception as e:
            logger.error(f"Failed to create daily data fetch job: {e}")
            raise AdapterException("FlowhubAdapter", f"Daily data fetch job creation failed: {e}")

    async def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态信息

        Returns:
            Dict[str, Any]: 系统状态
        """
        try:
            if not self._http_client:
                return {'status': 'disconnected', 'message': 'HTTP client not initialized'}

            # 调用详细状态端点
            response = await self._http_client.get('/api/v1/status')

            # 添加适配器统计信息
            response['adapter_statistics'] = self._request_statistics
            response['cached_jobs'] = len(self._job_status_cache)
            response['last_health_check'] = self._last_health_check.isoformat() if self._last_health_check else None

            return response

        except Exception as e:
            logger.error(f"Failed to get system status: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'adapter_statistics': self._request_statistics
            }
