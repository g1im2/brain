"""
系统协调处理器
"""

from aiohttp import web

from .base import BaseHandler


class SystemHandler(BaseHandler):
    """系统协调处理器"""
    
    async def startup(self, request: web.Request) -> web.Response:
        """启动系统
        
        Args:
            request: HTTP请求对象
            
        Returns:
            web.Response: 启动结果响应
        """
        try:
            coordinator = self.get_app_component(request, 'coordinator')
            
            if coordinator._is_running:
                return self.success_response(
                    {'status': 'already_running'}, 
                    "系统已经在运行中"
                )
            
            # 启动系统协调器
            startup_result = await coordinator.coordinate_system_startup()
            
            if startup_result:
                return self.success_response(
                    {'status': 'started', 'startup_result': startup_result},
                    "系统启动成功"
                )
            else:
                return self.error_response("系统启动失败", 500)
                
        except Exception as e:
            self.logger.error(f"System startup failed: {e}")
            return self.error_response("系统启动失败", 500)
    
    async def shutdown(self, request: web.Request) -> web.Response:
        """关闭系统
        
        Args:
            request: HTTP请求对象
            
        Returns:
            web.Response: 关闭结果响应
        """
        try:
            coordinator = self.get_app_component(request, 'coordinator')
            
            if not coordinator._is_running:
                return self.success_response(
                    {'status': 'already_stopped'}, 
                    "系统已经停止"
                )
            
            # 停止系统协调器
            await coordinator.stop()
            
            return self.success_response(
                {'status': 'stopped'},
                "系统关闭成功"
            )
                
        except Exception as e:
            self.logger.error(f"System shutdown failed: {e}")
            return self.error_response("系统关闭失败", 500)
    
    async def get_status(self, request: web.Request) -> web.Response:
        """获取系统状态
        
        Args:
            request: HTTP请求对象
            
        Returns:
            web.Response: 系统状态响应
        """
        try:
            coordinator = self.get_app_component(request, 'coordinator')
            
            # 获取系统状态
            system_status = coordinator.get_system_status()
            
            status_data = {
                'is_running': coordinator._is_running,
                'active_cycles': len(coordinator._active_cycles),
                'system_status': {
                    'overall_health': system_status.overall_health.value,
                    'macro_system_status': system_status.macro_system_status.value,
                    'portfolio_system_status': system_status.portfolio_system_status.value,
                    'tactical_system_status': system_status.tactical_system_status.value,
                    'data_pipeline_status': system_status.data_pipeline_status.value,
                    'last_update_time': system_status.last_update_time.isoformat()
                }
            }
            
            return self.success_response(status_data)
                
        except Exception as e:
            self.logger.error(f"Get system status failed: {e}")
            return self.error_response("获取系统状态失败", 500)
    
    async def trigger_analysis(self, request: web.Request) -> web.Response:
        """触发分析周期
        
        Args:
            request: HTTP请求对象
            
        Returns:
            web.Response: 分析周期触发结果响应
        """
        try:
            coordinator = self.get_app_component(request, 'coordinator')
            
            # 获取请求参数
            data = await self.get_request_json(request)
            force = data.get('force', False)
            
            # 触发完整分析周期
            cycle_result = await coordinator.coordinate_full_analysis_cycle()
            
            result_data = {
                'cycle_id': cycle_result.cycle_id,
                'status': cycle_result.status.value,
                'start_time': cycle_result.start_time.isoformat(),
                'end_time': cycle_result.end_time.isoformat() if cycle_result.end_time else None,
                'duration': cycle_result.duration,
                'success': cycle_result.success
            }
            
            return self.success_response(result_data, "分析周期触发成功")
                
        except Exception as e:
            self.logger.error(f"Trigger analysis failed: {e}")
            return self.error_response("触发分析周期失败", 500)
    
    async def get_analysis_history(self, request: web.Request) -> web.Response:
        """获取分析周期历史
        
        Args:
            request: HTTP请求对象
            
        Returns:
            web.Response: 分析周期历史响应
        """
        try:
            coordinator = self.get_app_component(request, 'coordinator')
            
            # 获取查询参数
            query_params = self.get_query_params(request)
            limit = int(query_params.get('limit', 10))
            offset = int(query_params.get('offset', 0))
            
            # 获取活动周期（作为历史的一部分）
            active_cycles = list(coordinator._active_cycles.values())
            
            # 转换为响应格式
            history_data = []
            for cycle in active_cycles[offset:offset+limit]:
                history_data.append({
                    'cycle_id': cycle.cycle_id,
                    'status': cycle.status.value,
                    'start_time': cycle.start_time.isoformat(),
                    'end_time': cycle.end_time.isoformat() if cycle.end_time else None,
                    'duration': cycle.duration,
                    'success': cycle.success
                })
            
            result_data = {
                'history': history_data,
                'total': len(coordinator._active_cycles),
                'limit': limit,
                'offset': offset
            }
            
            return self.success_response(result_data)
                
        except Exception as e:
            self.logger.error(f"Get analysis history failed: {e}")
            return self.error_response("获取分析历史失败", 500)
    
    async def get_resources(self, request: web.Request) -> web.Response:
        """获取系统资源状态
        
        Args:
            request: HTTP请求对象
            
        Returns:
            web.Response: 系统资源状态响应
        """
        try:
            coordinator = self.get_app_component(request, 'coordinator')
            
            # 获取资源分配信息
            resource_allocation = coordinator._resource_allocation
            
            if resource_allocation:
                resource_data = {
                    'allocation_id': resource_allocation.allocation_id,
                    'strategy': resource_allocation.strategy,
                    'cpu_allocation': resource_allocation.cpu_allocation,
                    'memory_allocation': resource_allocation.memory_allocation,
                    'network_allocation': resource_allocation.network_allocation,
                    'created_time': resource_allocation.created_time.isoformat(),
                    'is_active': resource_allocation.is_active
                }
            else:
                resource_data = None
            
            return self.success_response(resource_data)
                
        except Exception as e:
            self.logger.error(f"Get resources failed: {e}")
            return self.error_response("获取资源状态失败", 500)
