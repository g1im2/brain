"""
信号路由处理器
"""

from aiohttp import web

from .base import BaseHandler


class SignalHandler(BaseHandler):
    """信号路由处理器"""
    
    async def route_signal(self, request: web.Request) -> web.Response:
        """路由信号"""
        try:
            data = await self.get_request_json(request)
            signal_router = self.get_app_component(request, 'signal_router')
            
            # 验证必需字段
            error = self.validate_required_fields(data, ['signal_type', 'source', 'target'])
            if error:
                return self.error_response(error, 400)
            
            result = await signal_router.route_signal(data)
            return self.success_response(result, "信号路由成功")
        except Exception as e:
            self.logger.error(f"Route signal failed: {e}")
            return self.error_response("信号路由失败", 500)
    
    async def get_conflicts(self, request: web.Request) -> web.Response:
        """获取信号冲突"""
        try:
            signal_router = self.get_app_component(request, 'signal_router')
            conflicts = await signal_router.get_signal_conflicts()
            return self.success_response(conflicts)
        except Exception as e:
            self.logger.error(f"Get conflicts failed: {e}")
            return self.error_response("获取信号冲突失败", 500)
    
    async def resolve_conflicts(self, request: web.Request) -> web.Response:
        """解决信号冲突"""
        try:
            data = await self.get_request_json(request)
            signal_router = self.get_app_component(request, 'signal_router')
            result = await signal_router.resolve_conflicts(data)
            return self.success_response(result, "信号冲突解决成功")
        except Exception as e:
            self.logger.error(f"Resolve conflicts failed: {e}")
            return self.error_response("解决信号冲突失败", 500)
    
    async def get_signal_history(self, request: web.Request) -> web.Response:
        """获取信号历史"""
        try:
            query_params = self.get_query_params(request)
            signal_router = self.get_app_component(request, 'signal_router')
            history = await signal_router.get_signal_history(query_params)
            return self.success_response(history)
        except Exception as e:
            self.logger.error(f"Get signal history failed: {e}")
            return self.error_response("获取信号历史失败", 500)
    
    async def get_signal_stats(self, request: web.Request) -> web.Response:
        """获取信号统计"""
        try:
            signal_router = self.get_app_component(request, 'signal_router')
            stats = await signal_router.get_signal_statistics()
            return self.success_response(stats)
        except Exception as e:
            self.logger.error(f"Get signal stats failed: {e}")
            return self.error_response("获取信号统计失败", 500)
    
    async def cleanup_expired_signals(self, request: web.Request) -> web.Response:
        """清理过期信号"""
        try:
            signal_router = self.get_app_component(request, 'signal_router')
            result = await signal_router.cleanup_expired_signals()
            return self.success_response(result, "过期信号清理成功")
        except Exception as e:
            self.logger.error(f"Cleanup expired signals failed: {e}")
            return self.error_response("清理过期信号失败", 500)
