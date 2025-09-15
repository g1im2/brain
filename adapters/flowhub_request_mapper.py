"""
Flowhub服务请求映射器

将Brain层的数据拉取请求转换为Flowhub服务的API格式，
处理请求参数转换和响应格式标准化。
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)


class FlowhubRequestMapper:
    """Flowhub服务请求映射器
    
    将Brain层的数据拉取请求转换为Flowhub服务的API格式，
    处理参数转换、响应格式标准化和数据类型映射。
    """
    
    def __init__(self):
        """初始化请求映射器"""
        # 数据类型映射 (Brain层 -> Flowhub服务)
        self._data_type_mapping = {
            'batch_stock_data': 'batch_daily_ohlc',
            'batch_basic_data': 'batch_daily_basic',
            'daily_stock_data': 'daily_ohlc',
            'daily_basic_data': 'daily_basic'
        }
        
        # 反向映射 (Flowhub服务 -> Brain层)
        self._reverse_data_type_mapping = {v: k for k, v in self._data_type_mapping.items()}
        
        logger.info("FlowhubRequestMapper initialized")
    
    def map_batch_stock_data_request(self, brain_request: Dict[str, Any]) -> Dict[str, Any]:
        """映射批量股票数据抓取请求
        
        Args:
            brain_request: Brain层的批量股票数据请求
            
        Returns:
            Dict[str, Any]: Flowhub API格式的请求
        """
        try:
            flowhub_request = {}
            
            # 股票代码列表 (可选，不提供则抓取所有股票)
            if 'symbols' in brain_request and brain_request['symbols']:
                flowhub_request['symbols'] = brain_request['symbols']
            
            # 日期范围
            if 'start_date' in brain_request:
                flowhub_request['start_date'] = brain_request['start_date']
            
            if 'end_date' in brain_request:
                flowhub_request['end_date'] = brain_request['end_date']
            else:
                # 默认到今天
                flowhub_request['end_date'] = datetime.now().strftime('%Y-%m-%d')
            
            # 增量更新标志
            flowhub_request['incremental'] = brain_request.get('incremental', True)
            
            # 强制更新标志
            flowhub_request['force_update'] = brain_request.get('force_update', False)
            
            logger.debug(f"Mapped batch stock data request: {flowhub_request}")
            return flowhub_request
            
        except Exception as e:
            logger.error(f"Failed to map batch stock data request: {e}")
            raise ValueError(f"Request mapping failed: {e}")
    
    def map_batch_basic_data_request(self, brain_request: Dict[str, Any]) -> Dict[str, Any]:
        """映射批量基础数据抓取请求
        
        Args:
            brain_request: Brain层的批量基础数据请求
            
        Returns:
            Dict[str, Any]: Flowhub API格式的请求
        """
        try:
            flowhub_request = {}
            
            # 股票代码列表 (可选，不提供则抓取所有股票)
            if 'symbols' in brain_request and brain_request['symbols']:
                flowhub_request['symbols'] = brain_request['symbols']
            
            # 增量更新标志 (基础数据通常使用增量更新)
            flowhub_request['incremental'] = brain_request.get('incremental', True)
            
            # 强制更新标志
            flowhub_request['force_update'] = brain_request.get('force_update', False)
            
            logger.debug(f"Mapped batch basic data request: {flowhub_request}")
            return flowhub_request
            
        except Exception as e:
            logger.error(f"Failed to map batch basic data request: {e}")
            raise ValueError(f"Request mapping failed: {e}")
    
    def map_job_response(self, flowhub_response: Dict[str, Any], request_type: str) -> Dict[str, Any]:
        """映射任务创建响应
        
        Args:
            flowhub_response: Flowhub服务的响应
            request_type: 请求类型
            
        Returns:
            Dict[str, Any]: Brain层标准格式的响应
        """
        try:
            brain_response = {
                'job_id': flowhub_response.get('job_id'),
                'status': flowhub_response.get('status'),
                'progress': flowhub_response.get('progress', 0),
                'created_at': flowhub_response.get('created_at'),
                'request_type': request_type,
                'status_url': flowhub_response.get('status_url'),
                'result_url': flowhub_response.get('result_url')
            }
            
            # 添加预估信息
            if 'estimated_completion' in flowhub_response:
                brain_response['estimated_completion'] = flowhub_response['estimated_completion']
            
            logger.debug(f"Mapped job response: {brain_response}")
            return brain_response
            
        except Exception as e:
            logger.error(f"Failed to map job response: {e}")
            raise ValueError(f"Response mapping failed: {e}")
    
    def map_status_response(self, flowhub_response: Dict[str, Any]) -> Dict[str, Any]:
        """映射任务状态响应
        
        Args:
            flowhub_response: Flowhub服务的状态响应
            
        Returns:
            Dict[str, Any]: Brain层标准格式的状态响应
        """
        try:
            brain_response = {
                'job_id': flowhub_response.get('job_id'),
                'status': flowhub_response.get('status'),
                'progress': flowhub_response.get('progress', 0),
                'created_at': flowhub_response.get('created_at'),
                'updated_at': flowhub_response.get('updated_at'),
                'message': flowhub_response.get('message', '')
            }
            
            # 添加预估完成时间
            if 'estimated_completion' in flowhub_response:
                brain_response['estimated_completion'] = flowhub_response['estimated_completion']
            
            # 添加错误信息
            if 'error' in flowhub_response:
                brain_response['error'] = flowhub_response['error']
                brain_response['error_code'] = flowhub_response.get('error_code', 'UNKNOWN_ERROR')
            
            logger.debug(f"Mapped status response: {brain_response}")
            return brain_response
            
        except Exception as e:
            logger.error(f"Failed to map status response: {e}")
            raise ValueError(f"Response mapping failed: {e}")
    
    def map_result_response(self, flowhub_response: Dict[str, Any]) -> Dict[str, Any]:
        """映射任务结果响应
        
        Args:
            flowhub_response: Flowhub服务的结果响应
            
        Returns:
            Dict[str, Any]: Brain层标准格式的结果响应
        """
        try:
            brain_response = {
                'job_id': flowhub_response.get('job_id'),
                'status': flowhub_response.get('status'),
                'progress': flowhub_response.get('progress', 0),
                'completed_at': flowhub_response.get('completed_at'),
                'result': flowhub_response.get('result', {})
            }
            
            # 处理成功结果
            if flowhub_response.get('status') == 'succeeded':
                result = flowhub_response.get('result', {})
                brain_response['data_summary'] = {
                    'total_symbols': result.get('total_symbols', 0),
                    'successful': result.get('successful', 0),
                    'failed': result.get('failed', 0),
                    'records_count': result.get('total_records', 0)
                }
                
                # 详细结果
                if 'results' in result:
                    brain_response['detailed_results'] = result['results']
            
            # 处理失败结果
            elif flowhub_response.get('status') == 'failed':
                brain_response['error'] = flowhub_response.get('error', 'Unknown error')
                brain_response['error_code'] = flowhub_response.get('error_code', 'EXECUTION_FAILED')
                brain_response['failed_at'] = flowhub_response.get('failed_at')
            
            logger.debug(f"Mapped result response: {brain_response}")
            return brain_response
            
        except Exception as e:
            logger.error(f"Failed to map result response: {e}")
            raise ValueError(f"Response mapping failed: {e}")
    
    def get_default_batch_request(self, data_type: str = 'batch_stock_data') -> Dict[str, Any]:
        """获取默认的批量请求参数
        
        Args:
            data_type: 数据类型
            
        Returns:
            Dict[str, Any]: 默认请求参数
        """
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        
        default_request = {
            'incremental': True,
            'force_update': False
        }
        
        # 对于股票数据，添加默认日期范围
        if data_type == 'batch_stock_data':
            default_request.update({
                'start_date': yesterday.strftime('%Y-%m-%d'),
                'end_date': today.strftime('%Y-%m-%d')
            })
        
        logger.debug(f"Generated default {data_type} request: {default_request}")
        return default_request
