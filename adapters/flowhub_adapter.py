"""
Flowhub数据抓取服务适配器实现

负责与Flowhub数据抓取服务的接口适配和通信管理。
提供批量数据抓取、任务状态监控、结果获取等功能。
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

from interfaces import ISystemAdapter
from config import IntegrationConfig
from exceptions import AdapterException, ConnectionException, HealthCheckException
from adapters.http_client import HttpClient
from adapters.flowhub_request_mapper import FlowhubRequestMapper

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

    # ==================== 宏观数据任务创建方法 ====================

    async def create_macro_data_job(self, data_type: str, incremental: bool = True, max_history: bool = False, **kwargs) -> Dict[str, Any]:
        """创建宏观数据抓取任务

        Args:
            data_type: 数据类型 (e.g. 'gdp-data', 'price-index-data')
            incremental: 是否增量更新
            max_history: 是否使用最大历史数据范围（首次抓取时使用）
            **kwargs: 其他参数

        Returns:
            Dict[str, Any]: 任务创建结果
        """
        try:
            # 导入配置管理器
            from ..scheduler.macro_data_config import MacroDataConfig

            # 构建请求参数
            params = {
                'incremental': incremental
            }

            # 如果使用最大历史范围，添加历史范围参数
            if max_history:
                history_range = MacroDataConfig.get_max_history_range(data_type)
                if history_range:
                    params.update({
                        'start_date': history_range.get('start'),
                        'end_date': history_range.get('end'),
                        'max_history': True
                    })
                    logger.info(f"Using max history range for {data_type}: {history_range['start']} to {history_range['end']}")
                else:
                    logger.warning(f"No history range configured for {data_type}, using default range")

            # 根据数据类型添加特定参数
            if data_type == 'gdp-data':
                # GDP数据使用默认季度范围
                params.update({
                    'start_quarter': kwargs.get('start_quarter', '2020Q1'),
                    'end_quarter': kwargs.get('end_quarter', '2024Q4')
                })
            elif data_type in ['price-index-data', 'money-supply-data', 'social-financing-data',
                             'investment-data', 'industrial-data', 'sentiment-index-data', 'inventory-cycle-data']:
                # 月度数据使用默认月份范围
                params.update({
                    'start_month': kwargs.get('start_month', '202001'),
                    'end_month': kwargs.get('end_month', '202412')
                })
            elif data_type in ['interest-rate-data', 'stock-index-data', 'market-flow-data', 'commodity-price-data']:
                # 日度数据使用默认日期范围
                params.update({
                    'start_date': kwargs.get('start_date', '2024-01-01'),
                    'end_date': kwargs.get('end_date', '2024-12-31')
                })
            elif data_type in ['innovation-data', 'demographic-data']:
                # 年度数据使用默认年份范围
                params.update({
                    'start_year': kwargs.get('start_year', '2020'),
                    'end_year': kwargs.get('end_year', '2024')
                })

            # 添加特定类型参数
            if 'index_types' in kwargs:
                params['index_types'] = kwargs['index_types']
            if 'rate_types' in kwargs:
                params['rate_types'] = kwargs['rate_types']
            if 'index_codes' in kwargs:
                params['index_codes'] = kwargs['index_codes']
            if 'flow_types' in kwargs:
                params['flow_types'] = kwargs['flow_types']
            if 'commodity_types' in kwargs:
                params['commodity_types'] = kwargs['commodity_types']
            if 'indicators' in kwargs:
                params['indicators'] = kwargs['indicators']

            # 发送请求
            endpoint = f'/api/v1/jobs/{data_type}'
            response = await self._make_request('POST', endpoint, params)

            # 更新统计信息
            self._request_statistics['total_requests'] += 1
            self._request_statistics['successful_requests'] += 1

            logger.info(f"Macro data job created for {data_type}: {response.get('job_id')}")
            return response

        except Exception as e:
            self._request_statistics['total_requests'] += 1
            self._request_statistics['failed_requests'] += 1
            logger.error(f"Failed to create macro data job for {data_type}: {e}")
            raise AdapterException("FlowhubAdapter", f"Macro data job creation failed for {data_type}: {e}")
