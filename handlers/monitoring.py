"""
监控告警处理器
"""

from aiohttp import web

from handlers.base import BaseHandler


class MonitoringHandler(BaseHandler):
    """监控告警处理器"""
    
    async def get_metrics(self, request: web.Request) -> web.Response:
        """获取监控指标"""
        try:
            system_monitor = self.get_app_component(request, 'system_monitor')
            metrics = await system_monitor.collect_performance_metrics()
            return self.success_response(metrics)
        except Exception as e:
            self.logger.error(f"Get metrics failed: {e}")
            return self.error_response("获取监控指标失败", 500)
    
    async def get_alerts(self, request: web.Request) -> web.Response:
        """获取告警信息"""
        try:
            system_monitor = self.get_app_component(request, 'system_monitor')
            alerts = system_monitor.get_active_alerts()
            return self.success_response(alerts)
        except Exception as e:
            self.logger.error(f"Get alerts failed: {e}")
            return self.error_response("获取告警信息失败", 500)
    
    async def ack_alert(self, request: web.Request) -> web.Response:
        """确认告警"""
        try:
            data = await self.get_request_json(request)
            system_monitor = self.get_app_component(request, 'system_monitor')
            
            # 验证必需字段
            error = self.validate_required_fields(data, ['alert_id'])
            if error:
                return self.error_response(error, 400)
            
            result = system_monitor.acknowledge_alert(data['alert_id'], data.get('notes', ''))
            return self.success_response(result, "告警确认成功")
        except Exception as e:
            self.logger.error(f"Ack alert failed: {e}")
            return self.error_response("确认告警失败", 500)
    
    async def get_performance(self, request: web.Request) -> web.Response:
        """获取系统性能指标"""
        try:
            system_monitor = self.get_app_component(request, 'system_monitor')
            performance = system_monitor.get_system_performance()
            return self.success_response(performance)
        except Exception as e:
            self.logger.error(f"Get performance failed: {e}")
            return self.error_response("获取性能指标失败", 500)
    
    async def get_health_status(self, request: web.Request) -> web.Response:
        """获取服务健康状态"""
        try:
            system_monitor = self.get_app_component(request, 'system_monitor')
            health = await system_monitor.check_system_health()
            return self.success_response({
                'overall_health': health.overall_health.value,
                'macro_system_status': health.macro_system_status.value,
                'portfolio_system_status': health.portfolio_system_status.value,
                'strategy_system_status': health.strategy_system_status.value,
                'tactical_system_status': health.tactical_system_status.value,
                'data_pipeline_status': health.data_pipeline_status.value,
                'last_update_time': health.last_update_time.isoformat(),
                'active_sessions': health.active_sessions,
                'performance_metrics': health.performance_metrics,
                'error_count': health.error_count,
                'warning_count': health.warning_count,
                'metadata': health.metadata
            })
        except Exception as e:
            self.logger.error(f"Get health status failed: {e}")
            return self.error_response("获取健康状态失败", 500)
    
    async def set_alert_rule(self, request: web.Request) -> web.Response:
        """设置告警规则"""
        try:
            data = await self.get_request_json(request)
            system_monitor = self.get_app_component(request, 'system_monitor')
            
            # 验证必需字段
            error = self.validate_required_fields(data, ['name', 'condition', 'threshold'])
            if error:
                return self.error_response(error, 400)
            
            rule = system_monitor.set_alert_rule(data)
            return self.success_response(rule, "告警规则设置成功")
        except Exception as e:
            self.logger.error(f"Set alert rule failed: {e}")
            return self.error_response("设置告警规则失败", 500)
    
    async def get_alert_rules(self, request: web.Request) -> web.Response:
        """获取告警规则"""
        try:
            system_monitor = self.get_app_component(request, 'system_monitor')
            rules = system_monitor.get_alert_rules()
            return self.success_response(rules)
        except Exception as e:
            self.logger.error(f"Get alert rules failed: {e}")
            return self.error_response("获取告警规则失败", 500)
