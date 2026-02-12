"""
Integration Service 应用工厂

创建和配置 aiohttp 应用实例，集成所有必要的组件。
"""

import logging
import asyncio
import inspect
from aiohttp import web
from aiohttp_cors import setup as cors_setup, ResourceOptions

from asyncron import start_scheduler
import redis.asyncio as redis
from config import IntegrationConfig
from container import setup_container
from interfaces import ISystemCoordinator, ISignalRouter, IDataFlowManager
from middleware import setup_middleware
from routes import setup_routes
from scheduler.integration_scheduler import IntegrationScheduler
from scheduler.macro_data_tasks import MacroDataTaskScheduler
from scheduler.portfolio_data_tasks import PortfolioDataTaskScheduler
from scheduler.analysis_trigger import AnalysisTriggerScheduler
from adapters.service_registry import ServiceRegistry
from adapters.macro_adapter import MacroAdapter
from adapters.execution_adapter import ExecutionAdapter
from monitors.system_monitor import SystemMonitor
from initializers.data_initializer import DataInitializationCoordinator
from task_orchestrator import TaskOrchestrator
from auth_service import AuthService

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
    app['cors_enabled'] = True

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
        # 依赖注入容器
        app['container'] = await setup_container(config)

        # 服务注册表
        app['service_registry'] = ServiceRegistry(config)
        app['task_orchestrator'] = TaskOrchestrator(app)

        # Redis客户端（供调度器/分析触发使用）
        try:
            app['redis'] = redis.from_url(
                config.redis.url,
                encoding='utf-8',
                decode_responses=True
            )
            await app['redis'].ping()
            logger.info(f"Redis client initialized: {config.redis.url}")
        except Exception as redis_err:
            logger.warning(f"Failed to initialize Redis client: {redis_err}")
            app['redis'] = None

        # 鉴权服务（JWT + Refresh + Redis 会话索引）
        app['auth_service'] = AuthService(config=config, redis_client=app.get('redis'))
        await app['auth_service'].initialize()
        logger.info("Auth service initialized")

        # 核心组件（通过DI解析）
        app['coordinator'] = await app['container'].resolve(ISystemCoordinator)
        app['signal_router'] = await app['container'].resolve(ISignalRouter)
        app['data_flow_manager'] = await app['container'].resolve(IDataFlowManager)

        # 系统监控器
        app['system_monitor'] = SystemMonitor(config)

        # 服务适配器
        # Macro服务适配器
        app['macro_adapter'] = MacroAdapter(config)
        logger.info(f"MacroAdapter initialized with URL: {config.service.macro_service_url}")

        # Execution服务适配器
        app['execution_adapter'] = ExecutionAdapter(config)
        logger.info(f"ExecutionAdapter initialized with URL: {config.service.execution_service_url}")

        # 监控器组件引用
        app['system_monitor'].set_component_references(
            system_coordinator=app.get('coordinator'),
            signal_router=app.get('signal_router'),
            data_flow_manager=app.get('data_flow_manager'),
            macro_adapter=app.get('macro_adapter')
        )

        # 定时任务调度器
        if config.service.scheduler_enabled:
            app['scheduler'] = IntegrationScheduler(config, app['coordinator'], app)
            # 宏观数据调度器
            app['macro_scheduler'] = MacroDataTaskScheduler(config, app['coordinator'], app)
            # 投资组合数据调度器
            app['portfolio_scheduler'] = PortfolioDataTaskScheduler(config, app['coordinator'], app)
            # 分析触发调度器
            app['analysis_trigger'] = AnalysisTriggerScheduler(app)

        logger.info("Core components initialized successfully")

    except Exception as e:
        logger.error(f"Failed to initialize components: {e}")
        raise


async def startup_handler(app: web.Application):
    """应用启动处理器"""
    logger.info("Starting Integration Service components")

    # 优先调度数据初始化任务（即使后续组件启动失败也不影响初始化尝试）
    try:
        config = app['config']
        if getattr(config.service, 'init_data_on_startup', True) and 'data_initializer' not in app:
            initializer = DataInitializationCoordinator(config)
            app['data_initializer'] = initializer
            asyncio.create_task(initializer.run())
            logger.info("Background data initialization task scheduled")
    except Exception as e:
        logger.warning(f"Failed to schedule data initialization: {e}")

    try:
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

        # 收集所有调度器的planer，统一启动（避免多次调用start_scheduler导致线程冲突）
        planers = []

        # 添加集成调度器的planer（不调用其start方法）
        if 'scheduler' in app:
            integration_planer = app['scheduler'].get_planer()
            planers.append(integration_planer)
            logger.info("Integration scheduler planer added")

        # 添加宏观数据调度器的planer
        if 'macro_scheduler' in app:
            macro_planer = app['macro_scheduler'].get_planer()
            planers.append(macro_planer)
            logger.info("Macro data scheduler planer added")

        # 添加投资组合数据调度器的planer
        if 'portfolio_scheduler' in app:
            portfolio_planer = app['portfolio_scheduler'].get_planer()
            planers.append(portfolio_planer)
            logger.info("Portfolio data scheduler planer added")

        # 添加分析触发调度器的planer
        if 'analysis_trigger' in app:
            analysis_trigger_planer = app['analysis_trigger'].get_planer()
            planers.append(analysis_trigger_planer)
            logger.info("Analysis trigger scheduler planer added")

        # 统一启动所有调度器（只调用一次start_scheduler）
        if planers:
            start_scheduler(planers)
            logger.info(f"All {len(planers)} schedulers started successfully")

            # 标记IntegrationScheduler为已运行状态
            if 'scheduler' in app:
                app['scheduler']._is_running = True

        logger.info("All components started successfully")

        # 执行系统启动协调
        startup_result = await app['coordinator'].coordinate_system_startup()
        if startup_result:
            logger.info("System startup coordination completed successfully")
        else:
            logger.warning("System startup coordination completed with warnings")

        # 启动后校验当天数据抓取是否完成，必要时补抓
        try:
            if 'scheduler' in app:
                asyncio.create_task(app['scheduler'].ensure_daily_data_fetched_today())
                logger.info("Scheduled daily data fetch check on startup")
        except Exception as e:
            logger.warning(f"Failed to schedule daily data fetch check: {e}")

    except Exception as e:
        logger.error(f"Failed to start components: {e}")
        # 不再向上抛出异常，避免阻断服务整体启动（初始化任务已在上方调度）
        # raise


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

        # 关闭Redis客户端
        redis_client = app.get('redis')
        if redis_client:
            close_result = redis_client.close()
            if inspect.isawaitable(close_result):
                await close_result

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

    # 在新的事件循环中创建应用
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        app = loop.run_until_complete(create_app(config))
        return app
    finally:
        loop.close()
