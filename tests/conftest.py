"""
测试配置和夹具

提供测试所需的通用配置和夹具。
"""

import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from typing import AsyncGenerator, Generator

from ..config import IntegrationConfig
from ..container import DIContainer, ServiceRegistry
from ..models import SystemStatus, SystemHealthStatus


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """创建事件循环"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def test_config() -> IntegrationConfig:
    """测试配置"""
    config = IntegrationConfig(environment="testing")
    
    # 覆盖测试特定配置
    config.system_coordinator.max_concurrent_cycles = 1
    config.system_coordinator.cycle_timeout = 10
    config.signal_router.max_signal_queue_size = 10
    config.data_flow_manager.cache_size_mb = 10
    config.monitoring.enable_alerting = False
    
    return config


@pytest_asyncio.fixture
async def di_container(test_config: IntegrationConfig) -> AsyncGenerator[DIContainer, None]:
    """依赖注入容器"""
    container = DIContainer()
    await ServiceRegistry.register_core_services(container, test_config)
    yield container


@pytest.fixture
def mock_system_coordinator():
    """模拟系统协调器"""
    mock = AsyncMock()
    mock.coordinate_full_analysis_cycle.return_value = MagicMock(
        cycle_id="test-cycle-001",
        status="completed",
        is_completed=True
    )
    mock.coordinate_system_startup.return_value = True
    mock.coordinate_system_shutdown.return_value = True
    return mock


@pytest.fixture
def mock_signal_router():
    """模拟信号路由器"""
    mock = AsyncMock()
    mock.route_macro_signals.return_value = {"instruction": "test"}
    mock.route_portfolio_signals.return_value = {"allocation": "test"}
    mock.detect_signal_conflicts.return_value = {"conflicts": []}
    return mock


@pytest.fixture
def mock_data_flow_manager():
    """模拟数据流管理器"""
    mock = AsyncMock()
    mock.orchestrate_data_pipeline.return_value = MagicMock(
        pipeline_id="test-pipeline",
        status="running",
        data_quality_score=0.95
    )
    mock.manage_data_dependencies.return_value = {"dependencies": {}}
    return mock


@pytest.fixture
def sample_system_status() -> SystemStatus:
    """示例系统状态"""
    return SystemStatus(
        overall_health=SystemHealthStatus.HEALTHY,
        macro_system_status=SystemHealthStatus.HEALTHY,
        portfolio_system_status=SystemHealthStatus.HEALTHY,
        strategy_system_status=SystemHealthStatus.HEALTHY,
        data_pipeline_status=SystemHealthStatus.HEALTHY,
        last_update_time=datetime.now(),
        active_sessions=1,
        performance_metrics={
            "cpu_usage": 0.3,
            "memory_usage": 0.4,
            "response_time": 100.0
        }
    )


@pytest.fixture
def mock_flowhub_adapter():
    """模拟Flowhub适配器"""
    mock = AsyncMock()
    mock.connect_to_system.return_value = True
    mock.health_check.return_value = True
    mock.create_daily_data_fetch_job.return_value = {"job_id": "test-job-001"}
    mock.create_macro_data_job.return_value = {"job_id": "test-macro-job-001"}
    return mock


@pytest.fixture
def mock_strategy_adapter():
    """模拟策略适配器"""
    mock = AsyncMock()
    mock.connect_to_system.return_value = True
    mock.health_check.return_value = True
    mock.get_system_status.return_value = {"status": "healthy"}
    return mock


@pytest.fixture
def mock_portfolio_adapter():
    """模拟组合适配器"""
    mock = AsyncMock()
    mock.connect_to_system.return_value = True
    mock.health_check.return_value = True
    mock.get_system_status.return_value = {"status": "healthy"}
    return mock


@pytest.fixture
def mock_macro_adapter():
    """模拟宏观适配器"""
    mock = AsyncMock()
    mock.connect_to_system.return_value = True
    mock.health_check.return_value = True
    mock.get_system_status.return_value = {"status": "healthy"}
    return mock


class AsyncContextManagerMock:
    """异步上下文管理器模拟"""
    
    def __init__(self, return_value=None):
        self.return_value = return_value
    
    async def __aenter__(self):
        return self.return_value
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.fixture
def async_context_mock():
    """异步上下文管理器模拟工厂"""
    return AsyncContextManagerMock


# 测试标记
pytest_plugins = ["pytest_asyncio"]

# 测试配置
def pytest_configure(config):
    """pytest配置"""
    config.addinivalue_line(
        "markers", "unit: 单元测试"
    )
    config.addinivalue_line(
        "markers", "integration: 集成测试"
    )
    config.addinivalue_line(
        "markers", "slow: 慢速测试"
    )
    config.addinivalue_line(
        "markers", "external: 需要外部依赖的测试"
    )


# 测试收集配置
def pytest_collection_modifyitems(config, items):
    """修改测试收集"""
    for item in items:
        # 为异步测试添加标记
        if asyncio.iscoroutinefunction(item.function):
            item.add_marker(pytest.mark.asyncio)
        
        # 根据文件路径添加标记
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        elif "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
