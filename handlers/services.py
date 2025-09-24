"""
服务管理处理器
"""

from aiohttp import web

from handlers.base import BaseHandler


class ServiceHandler(BaseHandler):
    """服务管理处理器"""
    
    async def list_services(self, request: web.Request) -> web.Response:
        """获取服务列表"""
        try:
            service_registry = self.get_app_component(request, 'service_registry')
            services = await service_registry.get_all_services()
            return self.success_response(services)
        except Exception as e:
            self.logger.error(f"List services failed: {e}")
            return self.error_response("获取服务列表失败", 500)
    
    async def get_service_status(self, request: web.Request) -> web.Response:
        """获取服务状态"""
        try:
            service_name = self.get_path_params(request)['service']
            service_registry = self.get_app_component(request, 'service_registry')
            status = await service_registry.get_service_status(service_name)
            return self.success_response(status)
        except Exception as e:
            self.logger.error(f"Get service status failed: {e}")
            return self.error_response("获取服务状态失败", 500)
    
    async def health_check(self, request: web.Request) -> web.Response:
        """服务健康检查"""
        try:
            service_name = self.get_path_params(request)['service']
            service_registry = self.get_app_component(request, 'service_registry')
            health = await service_registry.health_check_service(service_name)
            return self.success_response(health)
        except Exception as e:
            self.logger.error(f"Service health check failed: {e}")
            return self.error_response("服务健康检查失败", 500)
    
    async def reconnect_service(self, request: web.Request) -> web.Response:
        """重连服务"""
        try:
            service_name = self.get_path_params(request)['service']
            service_registry = self.get_app_component(request, 'service_registry')
            result = await service_registry.reconnect_service(service_name)
            return self.success_response(result, "服务重连成功")
        except Exception as e:
            self.logger.error(f"Service reconnect failed: {e}")
            return self.error_response("服务重连失败", 500)
    
    async def get_service_config(self, request: web.Request) -> web.Response:
        """获取服务配置"""
        try:
            service_name = self.get_path_params(request)['service']
            service_registry = self.get_app_component(request, 'service_registry')
            config = await service_registry.get_service_config(service_name)
            return self.success_response(config)
        except Exception as e:
            self.logger.error(f"Get service config failed: {e}")
            return self.error_response("获取服务配置失败", 500)
