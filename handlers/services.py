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

    async def start_service(self, request: web.Request) -> web.Response:
        """启动服务（占位实现）"""
        try:
            service_name = self.get_path_params(request)['service']
            return self.success_response({'service': service_name, 'action': 'start'}, "服务启动请求已受理")
        except Exception as e:
            self.logger.error(f"Start service failed: {e}")
            return self.error_response("服务启动失败", 500)

    async def stop_service(self, request: web.Request) -> web.Response:
        """停止服务（占位实现）"""
        try:
            service_name = self.get_path_params(request)['service']
            return self.success_response({'service': service_name, 'action': 'stop'}, "服务停止请求已受理")
        except Exception as e:
            self.logger.error(f"Stop service failed: {e}")
            return self.error_response("服务停止失败", 500)

    async def restart_service(self, request: web.Request) -> web.Response:
        """重启服务（占位实现）"""
        try:
            service_name = self.get_path_params(request)['service']
            return self.success_response({'service': service_name, 'action': 'restart'}, "服务重启请求已受理")
        except Exception as e:
            self.logger.error(f"Restart service failed: {e}")
            return self.error_response("服务重启失败", 500)

    async def deploy_service(self, request: web.Request) -> web.Response:
        """部署服务（占位实现）"""
        try:
            payload = await self.get_request_json(request)
            return self.success_response({'deployment': payload}, "部署请求已受理")
        except Exception as e:
            self.logger.error(f"Deploy service failed: {e}")
            return self.error_response("服务部署失败", 500)

    async def get_execution_analysis_history(self, request: web.Request) -> web.Response:
        """代理查询 Execution 的分析历史（GET /api/v1/analyze/history，支持分页/排序/过滤）"""
        try:
            params = self.get_query_params(request)
            # 参数统一转成对应类型（仅在存在时转换）
            def to_float(key):
                if key in params and params[key] is not None:
                    params[key] = float(params[key])
            def to_int(key, default=None):
                if key in params and params[key] is not None:
                    params[key] = int(params[key])
                elif default is not None and key not in params:
                    params[key] = default

            to_int('page', 1)
            to_int('page_size', 20)
            to_float('min_confidence')
            to_float('max_confidence')
            to_float('min_strength')
            to_float('max_strength')
            to_float('min_turnover_rate')
            to_float('min_market_cap')
            to_float('max_pe')
            to_float('min_amount')
            to_float('min_price')
            to_float('max_price')

            coordinator = self.get_app_component(request, 'coordinator')
            strategy_adapter = getattr(coordinator, '_strategy_adapter', None)
            if not strategy_adapter:
                return self.error_response("StrategyAdapter 未初始化", 503)

            result = await strategy_adapter.query_analysis_history(**params)
            # Execution 已返回标准 {success, data, ...}，保持原样透传
            if isinstance(result, dict) and 'success' in result:
                return web.json_response(result)
            return self.success_response(result)
        except Exception as e:
            self.logger.error(f"Query execution analysis history failed: {e}")
            return self.error_response("查询失败", 500)

    async def get_execution_signal_stream(self, request: web.Request) -> web.Response:
        """代理查询 Execution 信号流（GET /api/v1/analyze/signal/stream）"""
        try:
            params = self.get_query_params(request)
            def to_float(key):
                if key in params and params[key] is not None:
                    params[key] = float(params[key])
            def to_int(key):
                if key in params and params[key] is not None:
                    params[key] = int(params[key])

            to_int('page_size')
            to_float('min_confidence')
            to_float('max_confidence')
            to_float('min_strength')
            to_float('max_strength')
            to_float('min_turnover_rate')
            to_float('min_market_cap')
            to_float('max_pe')
            to_float('min_amount')
            to_float('min_price')
            to_float('max_price')
            coordinator = self.get_app_component(request, 'coordinator')
            strategy_adapter = getattr(coordinator, '_strategy_adapter', None)
            if not strategy_adapter:
                return self.error_response("StrategyAdapter 未初始化", 503)

            result = await strategy_adapter.query_signal_stream(**params)
            if isinstance(result, dict) and 'success' in result:
                return web.json_response(result)
            return self.success_response(result)
        except Exception as e:
            self.logger.error(f"Query execution signal stream failed: {e}")
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
