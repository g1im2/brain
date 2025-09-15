"""
FlowhubAdapter集成测试

测试FlowhubAdapter与flowhub服务的集成功能
"""

import asyncio
import pytest
import logging
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from adapters.flowhub_adapter import FlowhubAdapter
from adapters.flowhub_request_mapper import FlowhubRequestMapper
from config import IntegrationConfig
from exceptions import AdapterException, ConnectionException

logger = logging.getLogger(__name__)


class TestFlowhubAdapter:
    """FlowhubAdapter测试类"""
    
    @pytest.fixture
    def config(self):
        """创建测试配置"""
        config = MagicMock(spec=IntegrationConfig)
        config.adapter.request_timeout = 30
        config.adapter.max_retries = 3
        config.adapter.retry_delay = 1
        return config
    
    @pytest.fixture
    def flowhub_adapter(self, config):
        """创建FlowhubAdapter实例"""
        return FlowhubAdapter(config)
    
    @pytest.fixture
    def mock_http_client(self):
        """创建模拟HTTP客户端"""
        mock_client = AsyncMock()
        mock_client.start = AsyncMock()
        mock_client.stop = AsyncMock()
        mock_client.get = AsyncMock()
        mock_client.post = AsyncMock()
        mock_client.delete = AsyncMock()
        return mock_client
    
    @pytest.mark.asyncio
    async def test_connect_to_system_success(self, flowhub_adapter, mock_http_client):
        """测试成功连接到系统"""
        # 模拟健康检查成功
        mock_http_client.get.return_value = {'status': 'healthy'}
        
        with patch.object(flowhub_adapter, '_http_client', mock_http_client):
            result = await flowhub_adapter.connect_to_system()
            
            assert result is True
            assert flowhub_adapter._is_connected is True
            mock_http_client.start.assert_called_once()
            mock_http_client.get.assert_called_with('/health')
    
    @pytest.mark.asyncio
    async def test_connect_to_system_health_check_failed(self, flowhub_adapter, mock_http_client):
        """测试健康检查失败时的连接"""
        # 模拟健康检查失败
        mock_http_client.get.return_value = {'status': 'unhealthy'}
        
        with patch.object(flowhub_adapter, '_http_client', mock_http_client):
            with pytest.raises(ConnectionException):
                await flowhub_adapter.connect_to_system()
            
            assert flowhub_adapter._is_connected is False
    
    @pytest.mark.asyncio
    async def test_health_check_success(self, flowhub_adapter, mock_http_client):
        """测试健康检查成功"""
        mock_http_client.get.return_value = {'status': 'healthy'}
        flowhub_adapter._http_client = mock_http_client
        
        result = await flowhub_adapter.health_check()
        
        assert result is True
        assert flowhub_adapter._last_health_check is not None
        mock_http_client.get.assert_called_with('/health')
    
    @pytest.mark.asyncio
    async def test_health_check_failed(self, flowhub_adapter, mock_http_client):
        """测试健康检查失败"""
        mock_http_client.get.return_value = {'status': 'unhealthy'}
        flowhub_adapter._http_client = mock_http_client
        
        result = await flowhub_adapter.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_create_batch_stock_data_job_success(self, flowhub_adapter, mock_http_client):
        """测试成功创建批量股票数据抓取任务"""
        # 模拟连接状态
        flowhub_adapter._is_connected = True
        flowhub_adapter._http_client = mock_http_client
        
        # 模拟API响应
        mock_response = {
            'job_id': 'test_job_123',
            'status': 'queued',
            'progress': 0,
            'created_at': 1234567890,
            'status_url': '/api/v1/jobs/test_job_123/status',
            'result_url': '/api/v1/jobs/test_job_123/result'
        }
        mock_http_client.post.return_value = mock_response
        
        # 测试请求
        request = {
            'symbols': ['000001.SZ', '000002.SZ'],
            'start_date': '2023-01-01',
            'end_date': '2023-12-31',
            'incremental': True
        }
        
        result = await flowhub_adapter.create_batch_stock_data_job(request)
        
        # 验证结果
        assert result['job_id'] == 'test_job_123'
        assert result['status'] == 'queued'
        assert result['request_type'] == 'batch_stock_data'
        
        # 验证HTTP调用
        mock_http_client.post.assert_called_once()
        call_args = mock_http_client.post.call_args
        assert call_args[0][0] == '/batch-stock-data'
        assert 'json' in call_args[1]
        
        # 验证统计更新
        stats = flowhub_adapter.get_request_statistics()
        assert stats['total_requests'] == 1
        assert stats['successful_requests'] == 1
    
    @pytest.mark.asyncio
    async def test_create_batch_basic_data_job_success(self, flowhub_adapter, mock_http_client):
        """测试成功创建批量基础数据抓取任务"""
        # 模拟连接状态
        flowhub_adapter._is_connected = True
        flowhub_adapter._http_client = mock_http_client
        
        # 模拟API响应
        mock_response = {
            'job_id': 'test_basic_job_456',
            'status': 'queued',
            'progress': 0,
            'created_at': 1234567890,
            'status_url': '/api/v1/jobs/test_basic_job_456/status',
            'result_url': '/api/v1/jobs/test_basic_job_456/result'
        }
        mock_http_client.post.return_value = mock_response
        
        # 测试请求
        request = {
            'incremental': True,
            'force_update': False
        }
        
        result = await flowhub_adapter.create_batch_basic_data_job(request)
        
        # 验证结果
        assert result['job_id'] == 'test_basic_job_456'
        assert result['status'] == 'queued'
        assert result['request_type'] == 'batch_basic_data'
        
        # 验证HTTP调用
        mock_http_client.post.assert_called_once()
        call_args = mock_http_client.post.call_args
        assert call_args[0][0] == '/batch-basic-data'
    
    @pytest.mark.asyncio
    async def test_get_job_status_success(self, flowhub_adapter, mock_http_client):
        """测试成功获取任务状态"""
        # 模拟连接状态
        flowhub_adapter._is_connected = True
        flowhub_adapter._http_client = mock_http_client
        
        # 模拟API响应
        mock_response = {
            'job_id': 'test_job_123',
            'status': 'running',
            'progress': 50,
            'created_at': 1234567890,
            'updated_at': 1234567950,
            'message': 'Processing...'
        }
        mock_http_client.get.return_value = mock_response
        
        result = await flowhub_adapter.get_job_status('test_job_123')
        
        # 验证结果
        assert result['job_id'] == 'test_job_123'
        assert result['status'] == 'running'
        assert result['progress'] == 50
        
        # 验证HTTP调用
        mock_http_client.get.assert_called_with('/test_job_123/status')
        
        # 验证缓存
        cached_status = flowhub_adapter.get_cached_job_status('test_job_123')
        assert cached_status is not None
        assert cached_status['status'] == 'running'
    
    @pytest.mark.asyncio
    async def test_get_job_result_success(self, flowhub_adapter, mock_http_client):
        """测试成功获取任务结果"""
        # 模拟连接状态
        flowhub_adapter._is_connected = True
        flowhub_adapter._http_client = mock_http_client
        
        # 模拟API响应
        mock_response = {
            'job_id': 'test_job_123',
            'status': 'succeeded',
            'progress': 100,
            'completed_at': 1234568000,
            'result': {
                'total_symbols': 100,
                'successful': 95,
                'failed': 5,
                'total_records': 50000
            }
        }
        mock_http_client.get.return_value = mock_response
        
        result = await flowhub_adapter.get_job_result('test_job_123')
        
        # 验证结果
        assert result['job_id'] == 'test_job_123'
        assert result['status'] == 'succeeded'
        assert result['data_summary']['total_symbols'] == 100
        assert result['data_summary']['successful'] == 95
        
        # 验证HTTP调用
        mock_http_client.get.assert_called_with('/test_job_123/result')
    
    @pytest.mark.asyncio
    async def test_create_daily_data_fetch_job(self, flowhub_adapter, mock_http_client):
        """测试创建每日数据抓取任务"""
        # 模拟连接状态
        flowhub_adapter._is_connected = True
        flowhub_adapter._http_client = mock_http_client
        
        # 模拟API响应
        mock_response = {
            'job_id': 'daily_job_789',
            'status': 'queued',
            'progress': 0,
            'created_at': 1234567890,
            'status_url': '/api/v1/jobs/daily_job_789/status',
            'result_url': '/api/v1/jobs/daily_job_789/result'
        }
        mock_http_client.post.return_value = mock_response
        
        result = await flowhub_adapter.create_daily_data_fetch_job(
            symbols=['000001.SZ'],
            incremental=True
        )
        
        # 验证结果
        assert result['job_id'] == 'daily_job_789'
        assert result['status'] == 'queued'
        
        # 验证HTTP调用
        mock_http_client.post.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_not_connected_error(self, flowhub_adapter):
        """测试未连接时的错误处理"""
        flowhub_adapter._is_connected = False
        
        with pytest.raises(AdapterException) as exc_info:
            await flowhub_adapter.create_batch_stock_data_job({})
        
        assert "Not connected" in str(exc_info.value)


class TestFlowhubRequestMapper:
    """FlowhubRequestMapper测试类"""
    
    @pytest.fixture
    def mapper(self):
        """创建请求映射器实例"""
        return FlowhubRequestMapper()
    
    def test_map_batch_stock_data_request(self, mapper):
        """测试批量股票数据请求映射"""
        brain_request = {
            'symbols': ['000001.SZ', '000002.SZ'],
            'start_date': '2023-01-01',
            'end_date': '2023-12-31',
            'incremental': True,
            'force_update': False
        }
        
        result = mapper.map_batch_stock_data_request(brain_request)
        
        assert result['symbols'] == ['000001.SZ', '000002.SZ']
        assert result['start_date'] == '2023-01-01'
        assert result['end_date'] == '2023-12-31'
        assert result['incremental'] is True
        assert result['force_update'] is False
    
    def test_map_batch_basic_data_request(self, mapper):
        """测试批量基础数据请求映射"""
        brain_request = {
            'incremental': True,
            'force_update': False
        }
        
        result = mapper.map_batch_basic_data_request(brain_request)
        
        assert result['incremental'] is True
        assert result['force_update'] is False
        assert 'symbols' not in result  # 不提供symbols表示所有股票
    
    def test_get_default_batch_request(self, mapper):
        """测试获取默认批量请求"""
        result = mapper.get_default_batch_request('batch_stock_data')
        
        assert 'incremental' in result
        assert 'force_update' in result
        assert 'start_date' in result
        assert 'end_date' in result
        assert result['incremental'] is True
        assert result['force_update'] is False


if __name__ == '__main__':
    # 运行测试
    pytest.main([__file__, '-v'])
