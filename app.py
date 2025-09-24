"""
Integration Service 应用工厂

创建和配置 aiohttp 应用实例，集成所有必要的组件。
"""

import logging
from aiohttp import web
from aiohttp_cors import setup as cors_setup, ResourceOptions

from config import IntegrationConfig
from middleware import setup_middleware
from routes import setup_routes
from coordinators.system_coordinator import SystemCoordinator
from scheduler.integration_scheduler import IntegrationScheduler
from adapters.service_registry import ServiceRegistry
from managers.data_flow_manager import DataFlowManager
from routers.signal_router import SignalRouter
from monitors.system_monitor import SystemMonitor

logger = logging.getLogger(__name__)


async def create_app(config: IntegrationConfig) -> web.Application:
    """创建 aiohttp 应用实例
    
    Args:
        config: 集成配置对象
        
    Returns:
        web.Application: 配置完成的应用实例
    """
    logger.info("Creating Integration Service application")
    
    # 创建应用
    app = web.Application()
    
    # 存储配置
    app['config'] = config
    
    # 设置CORS
    cors = cors_setup(app, defaults={
        "*": ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
            allow_methods="*"
        )
    })
    
    # 设置中间件
    setup_middleware(app)
    
    # 设置路由
    setup_routes(app, cors)
    
    # 初始化核心组件
    await init_components(app, config)
    
    # 设置启动和清理钩子
    app.on_startup.append(startup_handler)
    app.on_cleanup.append(cleanup_handler)
    
    logger.info("Integration Service application created successfully")
    return app


async def init_components(app: web.Application, config: IntegrationConfig):
    """初始化核心组件
    
    Args:
        app: aiohttp 应用实例
        config: 集成配置对象
    """
    logger.info("Initializing core components")
    
    try:
        # 服务注册表
        app['service_registry'] = ServiceRegistry(config)
        
        # 系统协调器
        app['coordinator'] = SystemCoordinator(config)
        
        # 信号路由器
        app['signal_router'] = SignalRouter(config)
        
        # 数据流管理器
        app['data_flow_manager'] = DataFlowManager(config)
        
        # 系统监控器
        app['system_monitor'] = SystemMonitor(config)
        
        # 定时任务调度器
        if config.service.scheduler_enabled:
            app['scheduler'] = IntegrationScheduler(config, app['coordinator'])

        logger.info("Core components initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize components: {e}")
        raise


async def startup_handler(app: web.Application):
    """应用启动处理器"""
    logger.info("Starting Integration Service components")
    
    try:
        config = app['config']
        
        # 启动服务注册表
        await app['service_registry'].start()
        
        # 启动系统协调器
        await app['coordinator'].start()
        
        # 启动信号路由器
        await app['signal_router'].start()
        
        # 启动数据流管理器
        await app['data_flow_manager'].start()
        
        # 启动系统监控器
        await app['system_monitor'].start()
        
        # 启动定时任务调度器
        if 'scheduler' in app:
            await app['scheduler'].start()
        
        logger.info("All components started successfully")
        
        # 执行系统启动协调
        startup_result = await app['coordinator'].coordinate_system_startup()
        if startup_result:
            logger.info("System startup coordination completed successfully")
        else:
            logger.warning("System startup coordination completed with warnings")
        
    except Exception as e:
        logger.error(f"Failed to start components: {e}")
        raise


async def cleanup_handler(app: web.Application):
    """应用清理处理器"""
    logger.info("Stopping Integration Service components")
    
    try:
        # 停止定时任务调度器
        if 'scheduler' in app:
            await app['scheduler'].stop()
        
        # 停止系统监控器
        if 'system_monitor' in app:
            await app['system_monitor'].stop()
        
        # 停止数据流管理器
        if 'data_flow_manager' in app:
            await app['data_flow_manager'].stop()
        
        # 停止信号路由器
        if 'signal_router' in app:
            await app['signal_router'].stop()
        
        # 停止系统协调器
        if 'coordinator' in app:
            await app['coordinator'].stop()
        
        # 停止服务注册表
        if 'service_registry' in app:
            await app['service_registry'].stop()
        
        logger.info("All components stopped successfully")
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


def create_test_app(config: IntegrationConfig = None) -> web.Application:
    """创建测试应用实例（同步版本）
    
    Args:
        config: 可选的配置对象，如果不提供则使用测试配置
        
    Returns:
        web.Application: 测试应用实例
    """
    if config is None:
        config = IntegrationConfig(environment="testing")
    
    import asyncio
    
    # 在新的事件循环中创建应用
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        app = loop.run_until_complete(create_app(config))
        return app
    finally:
        loop.close()
