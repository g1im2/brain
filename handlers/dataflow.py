"""
数据流管理处理器
"""

from aiohttp import web

from handlers.base import BaseHandler


class DataFlowHandler(BaseHandler):
    """数据流管理处理器"""
    
    async def get_status(self, request: web.Request) -> web.Response:
        """获取数据流状态"""
        try:
            data_flow_manager = self.get_app_component(request, 'data_flow_manager')
            status = await data_flow_manager.get_data_flow_status()
            return self.success_response(status)
        except Exception as e:
            self.logger.error(f"Get dataflow status failed: {e}")
            return self.error_response("获取数据流状态失败", 500)
    
    async def optimize(self, request: web.Request) -> web.Response:
        """优化数据流"""
        try:
            data_flow_manager = self.get_app_component(request, 'data_flow_manager')
            result = await data_flow_manager.optimize_data_flow()
            return self.success_response(result, "数据流优化成功")
        except Exception as e:
            self.logger.error(f"Optimize dataflow failed: {e}")
            return self.error_response("数据流优化失败", 500)
    
    async def get_metrics(self, request: web.Request) -> web.Response:
        """获取数据流指标"""
        try:
            data_flow_manager = self.get_app_component(request, 'data_flow_manager')
            metrics = await data_flow_manager.collect_performance_metrics()
            return self.success_response(metrics)
        except Exception as e:
            self.logger.error(f"Get dataflow metrics failed: {e}")
            return self.error_response("获取数据流指标失败", 500)
    
    async def clear_cache(self, request: web.Request) -> web.Response:
        """清理缓存"""
        try:
            data_flow_manager = self.get_app_component(request, 'data_flow_manager')
            result = await data_flow_manager.clear_cache()
            return self.success_response(result, "缓存清理成功")
        except Exception as e:
            self.logger.error(f"Clear cache failed: {e}")
            return self.error_response("缓存清理失败", 500)
    
    async def get_cache_stats(self, request: web.Request) -> web.Response:
        """获取缓存统计"""
        try:
            data_flow_manager = self.get_app_component(request, 'data_flow_manager')
            stats = await data_flow_manager.get_cache_statistics()
            return self.success_response(stats)
        except Exception as e:
            self.logger.error(f"Get cache stats failed: {e}")
            return self.error_response("获取缓存统计失败", 500)
    
    async def trigger_sync(self, request: web.Request) -> web.Response:
        """触发数据同步"""
        try:
            data = await self.get_request_json(request)
            data_flow_manager = self.get_app_component(request, 'data_flow_manager')
            result = await data_flow_manager.trigger_data_sync(data)
            return self.success_response(result, "数据同步触发成功")
        except Exception as e:
            self.logger.error(f"Trigger sync failed: {e}")
            return self.error_response("触发数据同步失败", 500)
