"""
Integration Service 微服务主入口

提供三层金融交易系统的统一协调和管理服务。
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path

from aiohttp import web

from app import create_app
from config import IntegrationConfig


def setup_logging(config: IntegrationConfig):
    """设置日志配置"""
    log_level = getattr(logging, config.get('logging.level', 'INFO').upper())
    
    # 创建日志目录
    log_file = config.get('logging.file', 'logs/integration.log')
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 配置日志格式
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # 设置第三方库日志级别
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)


async def main():
    """主启动函数"""
    try:
        # 加载配置
        environment = sys.argv[1] if len(sys.argv) > 1 else "development"
        config = IntegrationConfig(environment=environment)
        
        # 设置日志
        setup_logging(config)
        logger = logging.getLogger(__name__)
        
        logger.info(f"Starting Integration Service in {environment} environment")
        
        # 验证配置
        if not config.validate():
            logger.error("Configuration validation failed")
            sys.exit(1)
        
        # 创建应用
        app = await create_app(config)
        
        # 启动Web服务
        runner = web.AppRunner(app)
        await runner.setup()
        
        host = config.service.host
        port = config.service.port
        
        site = web.TCPSite(runner, host, port)
        await site.start()
        
        logger.info(f"Integration Service started on http://{host}:{port}")
        logger.info(f"Health check: http://{host}:{port}/health")
        logger.info(f"API documentation: http://{host}:{port}/api/v1/status")
        
        # 设置信号处理
        def signal_handler():
            logger.info("Received shutdown signal")
            asyncio.create_task(shutdown(runner, app))
        
        if sys.platform != 'win32':
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, signal_handler)
        
        # 保持运行
        try:
            await asyncio.Future()  # 永远等待
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        
    except Exception as e:
        logging.error(f"Failed to start Integration Service: {e}")
        sys.exit(1)


async def shutdown(runner: web.AppRunner, app: web.Application):
    """优雅关闭"""
    logger = logging.getLogger(__name__)
    logger.info("Shutting down Integration Service...")
    
    try:
        # 停止接收新请求
        await runner.cleanup()
        
        # 清理应用资源
        if 'coordinator' in app:
            await app['coordinator'].stop()
        
        if 'scheduler' in app:
            await app['scheduler'].stop()
        
        logger.info("Integration Service shutdown complete")
        
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    
    finally:
        # 强制退出
        asyncio.get_event_loop().stop()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
