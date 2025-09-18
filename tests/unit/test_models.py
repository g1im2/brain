"""
数据模型单元测试

测试数据模型的验证逻辑和属性计算。
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import patch

from ...models import (
    MarketEvent, EventType, SystemStatus, SystemHealthStatus,
    ValidationResult, ValidationStatus, StandardSignal,
    AnalysisCycleResult, SignalRoutingResult, DataFlowStatus
)


class TestMarketEvent:
    """市场事件测试"""
    
    def test_valid_market_event(self):
        """测试有效的市场事件"""
        event = MarketEvent(
            event_id="test-001",
            event_type=EventType.MACRO_DATA_UPDATE,
            timestamp=datetime.now(),
            symbols=["AAPL", "GOOGL"],
            data={"price": 150.0},
            priority=3
        )
        
        assert event.event_id == "test-001"
        assert event.event_type == EventType.MACRO_DATA_UPDATE
        assert event.priority == 3
        assert len(event.symbols) == 2
    
    def test_invalid_priority_raises_error(self):
        """测试无效优先级抛出错误"""
        with pytest.raises(ValueError, match="Priority must be between 1 and 5"):
            MarketEvent(
                event_id="test-001",
                event_type=EventType.MACRO_DATA_UPDATE,
                timestamp=datetime.now(),
                symbols=["AAPL"],
                data={},
                priority=6  # 无效优先级
            )
    
    def test_empty_event_id_raises_error(self):
        """测试空事件ID抛出错误"""
        with pytest.raises(ValueError, match="Event ID cannot be empty"):
            MarketEvent(
                event_id="",  # 空ID
                event_type=EventType.MACRO_DATA_UPDATE,
                timestamp=datetime.now(),
                symbols=["AAPL"],
                data={}
            )


class TestValidationResult:
    """验证结果测试"""
    
    def test_valid_validation_result(self):
        """测试有效的验证结果"""
        result = ValidationResult(
            validation_id="val-001",
            validation_type="historical",
            validation_status=ValidationStatus.COMPLETED,
            validation_score=0.85,
            confidence_adjustment=0.1
        )
        
        assert result.validation_id == "val-001"
        assert result.validation_score == 0.85
        assert result.confidence_adjustment == 0.1
    
    def test_invalid_validation_score_raises_error(self):
        """测试无效验证分数抛出错误"""
        with pytest.raises(ValueError, match="Validation score must be between 0 and 1"):
            ValidationResult(
                validation_id="val-001",
                validation_type="historical",
                validation_status=ValidationStatus.COMPLETED,
                validation_score=1.5,  # 无效分数
                confidence_adjustment=0.1
            )
    
    def test_invalid_confidence_adjustment_raises_error(self):
        """测试无效置信度调整抛出错误"""
        with pytest.raises(ValueError, match="Confidence adjustment must be between -1 and 1"):
            ValidationResult(
                validation_id="val-001",
                validation_type="historical",
                validation_status=ValidationStatus.COMPLETED,
                validation_score=0.85,
                confidence_adjustment=1.5  # 无效调整值
            )


class TestStandardSignal:
    """标准信号测试"""
    
    def test_valid_standard_signal(self):
        """测试有效的标准信号"""
        signal = StandardSignal(
            signal_id="sig-001",
            signal_type="buy",
            source="macro",
            target="portfolio",
            strength=0.8,
            confidence=0.9,
            priority=4
        )
        
        assert signal.signal_id == "sig-001"
        assert signal.strength == 0.8
        assert signal.confidence == 0.9
        assert signal.priority == 4
        assert not signal.is_expired  # 没有设置过期时间
    
    def test_signal_expiry(self):
        """测试信号过期"""
        past_time = datetime.now() - timedelta(hours=1)
        signal = StandardSignal(
            signal_id="sig-001",
            signal_type="buy",
            source="macro",
            target="portfolio",
            strength=0.8,
            confidence=0.9,
            expiry_time=past_time
        )
        
        assert signal.is_expired
    
    def test_invalid_signal_strength_raises_error(self):
        """测试无效信号强度抛出错误"""
        with pytest.raises(ValueError, match="Signal strength must be between 0 and 1"):
            StandardSignal(
                signal_id="sig-001",
                signal_type="buy",
                source="macro",
                target="portfolio",
                strength=1.5,  # 无效强度
                confidence=0.9
            )
    
    def test_invalid_signal_priority_raises_error(self):
        """测试无效信号优先级抛出错误"""
        with pytest.raises(ValueError, match="Signal priority must be between 1 and 5"):
            StandardSignal(
                signal_id="sig-001",
                signal_type="buy",
                source="macro",
                target="portfolio",
                strength=0.8,
                confidence=0.9,
                priority=0  # 无效优先级
            )


class TestAnalysisCycleResult:
    """分析周期结果测试"""
    
    def test_analysis_cycle_duration_calculation(self):
        """测试分析周期时长计算"""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=30)
        
        result = AnalysisCycleResult(
            cycle_id="cycle-001",
            start_time=start_time,
            end_time=end_time,
            status="completed"
        )
        
        assert result.duration == 30.0
        assert result.is_completed
    
    def test_incomplete_cycle_duration(self):
        """测试未完成周期的时长"""
        result = AnalysisCycleResult(
            cycle_id="cycle-001",
            start_time=datetime.now(),
            status="running"
        )
        
        assert result.duration is None
        assert not result.is_completed


class TestSystemStatus:
    """系统状态测试"""
    
    def test_system_status_creation(self):
        """测试系统状态创建"""
        status = SystemStatus(
            overall_health=SystemHealthStatus.HEALTHY,
            macro_system_status=SystemHealthStatus.HEALTHY,
            portfolio_system_status=SystemHealthStatus.WARNING,
            strategy_system_status=SystemHealthStatus.HEALTHY,
            data_pipeline_status=SystemHealthStatus.HEALTHY,
            last_update_time=datetime.now(),
            active_sessions=5,
            performance_metrics={
                "cpu_usage": 0.6,
                "memory_usage": 0.7
            }
        )
        
        assert status.overall_health == SystemHealthStatus.HEALTHY
        assert status.active_sessions == 5
        assert status.performance_metrics["cpu_usage"] == 0.6


class TestDataFlowStatus:
    """数据流状态测试"""
    
    def test_data_flow_status_creation(self):
        """测试数据流状态创建"""
        status = DataFlowStatus(
            pipeline_id="pipeline-001",
            status="running",
            data_sources=["source1", "source2"],
            data_quality_score=0.95,
            throughput=1000.0,
            latency=50.0,
            cache_hit_rate=0.8,
            error_rate=0.01,
            last_update_time=datetime.now(),
            processed_records=10000,
            failed_records=100
        )
        
        assert status.pipeline_id == "pipeline-001"
        assert status.data_quality_score == 0.95
        assert len(status.data_sources) == 2
        assert status.processed_records == 10000


# 参数化测试示例
@pytest.mark.parametrize("priority,expected_valid", [
    (1, True),
    (3, True),
    (5, True),
    (0, False),
    (6, False),
    (-1, False)
])
def test_market_event_priority_validation(priority, expected_valid):
    """参数化测试市场事件优先级验证"""
    if expected_valid:
        event = MarketEvent(
            event_id="test-001",
            event_type=EventType.MACRO_DATA_UPDATE,
            timestamp=datetime.now(),
            symbols=["AAPL"],
            data={},
            priority=priority
        )
        assert event.priority == priority
    else:
        with pytest.raises(ValueError):
            MarketEvent(
                event_id="test-001",
                event_type=EventType.MACRO_DATA_UPDATE,
                timestamp=datetime.now(),
                symbols=["AAPL"],
                data={},
                priority=priority
            )
