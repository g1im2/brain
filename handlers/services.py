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

    async def get_execution_analysis_history(self, request: web.Request) -> web.Response:
        """代理查询 Execution 的分析历史（GET /api/v1/analyze/history，支持分页/排序/过滤）"""
        try:
            p = self.get_query_params(request)
            symbol = p.get('symbol')
            if not symbol:
                return self.error_response("symbol 参数是必需的", 400)
            analyzer_type = p.get('analyzer_type')
            start_date = p.get('start_date')
            end_date = p.get('end_date')
            page = int(p.get('page', 1))
            page_size = int(p.get('page_size', 20))
            sort_by = p.get('sort_by', 'analysis_date')
            sort_order = p.get('sort_order', 'desc')
            signal = p.get('signal')
            min_confidence = p.get('min_confidence')
            max_confidence = p.get('max_confidence')
            if min_confidence is not None:
                min_confidence = float(min_confidence)
            if max_confidence is not None:
                max_confidence = float(max_confidence)

            coordinator = self.get_app_component(request, 'coordinator')
            strategy_adapter = getattr(coordinator, '_strategy_adapter', None)
            if not strategy_adapter:
                return self.error_response("StrategyAdapter 未初始化", 503)

            result = await strategy_adapter.query_analysis_history(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                analyzer_type=analyzer_type,
                page=page,
                page_size=page_size,
                sort_by=sort_by,
                sort_order=sort_order,
                signal=signal,
                min_confidence=min_confidence,
                max_confidence=max_confidence
            )
            return self.success_response(result)
        except Exception as e:
            self.logger.error(f"Query execution analysis history failed: {e}")
            return self.error_response("查询失败", 500)

    async def get_execution_backtest_history(self, request: web.Request) -> web.Response:
        """ Execution   GET /api/v1/backtest/history     """
        try:
            p = self.get_query_params(request)
            strategy_type = p.get('strategy_type')
            start_date = p.get('start_date')
            end_date = p.get('end_date')
            page = int(p.get('page', 1))
            page_size = int(p.get('page_size', 20))
            sort_by = p.get('sort_by', 'created_at')
            sort_order = p.get('sort_order', 'desc')
            min_total_return = p.get('min_total_return')
            max_drawdown = p.get('max_drawdown')
            if min_total_return is not None:
                min_total_return = float(min_total_return)
            if max_drawdown is not None:
                max_drawdown = float(max_drawdown)

            coordinator = self.get_app_component(request, 'coordinator')
            strategy_adapter = getattr(coordinator, '_strategy_adapter', None)
            if not strategy_adapter:
                return self.error_response("StrategyAdapter ", 503)

            result = await strategy_adapter.query_backtest_history(
                strategy_type=strategy_type,
                start_date=start_date,
                end_date=end_date,
                page=page,
                page_size=page_size,
                sort_by=sort_by,
                sort_order=sort_order,
                min_total_return=min_total_return,
                max_drawdown=max_drawdown
            )
            return self.success_response(result)
        except Exception as e:
            self.logger.error(f"Query execution backtest history failed: {e}")
            return self.error_response("", 500)

    async def execution_analyze_callback(self, request: web.Request) -> web.Response:
        """ Execution 
        app['last_execution_callback']
        """
        try:
            payload = await request.json()
            request.app['last_execution_callback'] = payload
            # 
            event_engine = request.app.get('event_engine')
            if event_engine:
                await event_engine.publish('execution.analyze.completed', payload)
            return self.success_response({'received': True})
        except Exception as e:
            self.logger.error(f"Execution analyze callback failed: {e}")
            return self.error_response("", 500)

    async def execution_backtest_callback(self, request: web.Request) -> web.Response:
        """ Execution 
        """
        try:
            payload = await request.json()
            request.app['last_execution_callback'] = payload
            event_engine = request.app.get('event_engine')
            if event_engine:
                await event_engine.publish('execution.backtest.completed', payload)
            return self.success_response({'received': True})
        except Exception as e:
            self.logger.error(f"Execution backtest callback failed: {e}")
            return self.error_response("", 500)

    async def get_last_execution_callback(self, request: web.Request) -> web.Response:
        """调试用途：获取最近一次接收到的Execution回调载荷"""
        try:
            payload = request.app.get('last_execution_callback')
            return self.success_response(payload or {})
        except Exception as e:
            self.logger.error(f"Get last execution callback failed: {e}")
            return self.error_response("获取回调信息失败", 500)
