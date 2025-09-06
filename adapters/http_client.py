"""
HTTP客户端基类

提供统一的HTTP服务调用接口。
"""

import asyncio
import logging
from typing import Dict, Any, Optional, Union
from datetime import datetime

import aiohttp

from ..config import IntegrationConfig
from ..exceptions import AdapterException, ConnectionException


class HttpClient:
    """HTTP客户端基类"""
    
    def __init__(self, service_name: str, config: IntegrationConfig):
        """初始化HTTP客户端
        
        Args:
            service_name: 服务名称
            config: 集成配置对象
        """
        self.service_name = service_name
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{service_name}")
        self._session: Optional[aiohttp.ClientSession] = None
        self._base_url = self._get_service_url()
        self._api_prefix = self._get_api_prefix()
        
        # 统计信息
        self._request_count = 0
        self._success_count = 0
        self._error_count = 0
    
    def _get_service_url(self) -> str:
        """获取服务URL"""
        service_urls = {
            'macro': self.config.service.macro_service_url,
            'portfolio': self.config.service.portfolio_service_url,
            'execution': self.config.service.execution_service_url,
            'flowhub': self.config.service.flowhub_service_url
        }
        
        if self.service_name not in service_urls:
            raise AdapterException(f"Unknown service: {self.service_name}")
        
        return service_urls[self.service_name]
    
    def _get_api_prefix(self) -> str:
        """获取API前缀"""
        api_prefixes = {
            'macro': '/api/v1/macro',
            'portfolio': '/api/v1/portfolio',
            'execution': '/api/v1/execution',
            'flowhub': '/api/v1/jobs'
        }
        
        return api_prefixes.get(self.service_name, '/api/v1')
    
    async def start(self):
        """启动HTTP客户端"""
        if self._session:
            return
        
        timeout = aiohttp.ClientTimeout(total=self.config.adapter.request_timeout)
        connector = aiohttp.TCPConnector(
            limit=self.config.adapter.pool_size,
            limit_per_host=5
        )
        
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={'Content-Type': 'application/json'}
        )
        
        self.logger.info(f"HTTP client for {self.service_name} started")
    
    async def stop(self):
        """停止HTTP客户端"""
        if self._session:
            await self._session.close()
            self._session = None
        
        self.logger.info(f"HTTP client for {self.service_name} stopped")
    
    async def get(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """GET请求
        
        Args:
            endpoint: API端点
            params: 查询参数
            
        Returns:
            Dict[str, Any]: 响应数据
        """
        return await self._request('GET', endpoint, params=params)
    
    async def post(self, endpoint: str, data: Dict[str, Any] = None) -> Dict[str, Any]:
        """POST请求
        
        Args:
            endpoint: API端点
            data: 请求数据
            
        Returns:
            Dict[str, Any]: 响应数据
        """
        return await self._request('POST', endpoint, json=data)
    
    async def put(self, endpoint: str, data: Dict[str, Any] = None) -> Dict[str, Any]:
        """PUT请求
        
        Args:
            endpoint: API端点
            data: 请求数据
            
        Returns:
            Dict[str, Any]: 响应数据
        """
        return await self._request('PUT', endpoint, json=data)
    
    async def delete(self, endpoint: str) -> Dict[str, Any]:
        """DELETE请求
        
        Args:
            endpoint: API端点
            
        Returns:
            Dict[str, Any]: 响应数据
        """
        return await self._request('DELETE', endpoint)
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查
        
        Returns:
            Dict[str, Any]: 健康状态
        """
        try:
            start_time = datetime.utcnow()
            url = f"{self._base_url}/health"
            
            async with self._session.get(url) as response:
                response_time = (datetime.utcnow() - start_time).total_seconds() * 1000
                
                if response.status == 200:
                    data = await response.json()
                    return {
                        'service': self.service_name,
                        'status': 'healthy',
                        'response_time': response_time,
                        'data': data
                    }
                else:
                    return {
                        'service': self.service_name,
                        'status': 'unhealthy',
                        'error': f"HTTP {response.status}",
                        'response_time': response_time
                    }
        
        except Exception as e:
            return {
                'service': self.service_name,
                'status': 'unhealthy',
                'error': str(e)
            }
    
    async def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """执行HTTP请求
        
        Args:
            method: HTTP方法
            endpoint: API端点
            **kwargs: 请求参数
            
        Returns:
            Dict[str, Any]: 响应数据
            
        Raises:
            ConnectionException: 连接失败
            AdapterException: 请求失败
        """
        if not self._session:
            await self.start()
        
        # 构建完整URL
        if endpoint.startswith('/'):
            url = f"{self._base_url}{endpoint}"
        else:
            url = f"{self._base_url}{self._api_prefix}/{endpoint}"
        
        self._request_count += 1
        
        # 重试逻辑
        for attempt in range(self.config.adapter.max_retries + 1):
            try:
                self.logger.debug(f"{method} {url} (attempt {attempt + 1})")
                
                async with self._session.request(method, url, **kwargs) as response:
                    if response.status == 200:
                        self._success_count += 1
                        data = await response.json()
                        return data
                    elif response.status == 404:
                        raise AdapterException(f"Endpoint not found: {endpoint}")
                    elif response.status >= 500:
                        # 服务器错误，可以重试
                        if attempt < self.config.adapter.max_retries:
                            await asyncio.sleep(self.config.adapter.retry_delay * (attempt + 1))
                            continue
                        else:
                            raise AdapterException(f"Server error: HTTP {response.status}")
                    else:
                        # 客户端错误，不重试
                        error_text = await response.text()
                        raise AdapterException(f"Request failed: HTTP {response.status}, {error_text}")
            
            except aiohttp.ClientError as e:
                if attempt < self.config.adapter.max_retries:
                    self.logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                    await asyncio.sleep(self.config.adapter.retry_delay * (attempt + 1))
                    continue
                else:
                    self._error_count += 1
                    raise ConnectionException(f"Connection failed to {self.service_name}: {e}")
            
            except Exception as e:
                self._error_count += 1
                raise AdapterException(f"Request failed: {e}")
        
        # 不应该到达这里
        self._error_count += 1
        raise AdapterException("Max retries exceeded")
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        return {
            'service': self.service_name,
            'request_count': self._request_count,
            'success_count': self._success_count,
            'error_count': self._error_count,
            'success_rate': self._success_count / max(self._request_count, 1)
        }
