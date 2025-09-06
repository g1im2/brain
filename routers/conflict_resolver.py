"""
冲突解决器实现

负责检测和解决信号冲突，确保信号传递的一致性和可靠性。
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum

from ..models import StandardSignal
from ..config import IntegrationConfig
from ..exceptions import SignalConflictException

logger = logging.getLogger(__name__)


class ConflictType(Enum):
    """冲突类型枚举"""
    DIRECTION_CONFLICT = "direction_conflict"  # 方向冲突
    STRENGTH_CONFLICT = "strength_conflict"    # 强度冲突
    TIMING_CONFLICT = "timing_conflict"        # 时机冲突
    PRIORITY_CONFLICT = "priority_conflict"    # 优先级冲突
    RESOURCE_CONFLICT = "resource_conflict"    # 资源冲突


class ConflictResolver:
    """冲突解决器实现类
    
    检测和解决各种类型的信号冲突，确保信号传递的一致性。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化冲突解决器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False
        
        # 冲突解决策略
        self._resolution_strategies = {
            ConflictType.DIRECTION_CONFLICT: self._resolve_direction_conflict,
            ConflictType.STRENGTH_CONFLICT: self._resolve_strength_conflict,
            ConflictType.TIMING_CONFLICT: self._resolve_timing_conflict,
            ConflictType.PRIORITY_CONFLICT: self._resolve_priority_conflict,
            ConflictType.RESOURCE_CONFLICT: self._resolve_resource_conflict
        }
        
        # 冲突检测规则
        self._detection_rules = {
            'direction_threshold': 0.5,  # 方向冲突阈值
            'strength_threshold': 0.3,   # 强度冲突阈值
            'timing_window': 300,        # 时机冲突窗口（秒）
            'priority_difference': 2     # 优先级差异阈值
        }
        
        # 统计信息
        self._conflict_statistics = {
            'total_conflicts': 0,
            'resolved_conflicts': 0,
            'by_type': {conflict_type.value: 0 for conflict_type in ConflictType}
        }
        
        logger.info("ConflictResolver initialized")
    
    async def start(self) -> bool:
        """启动冲突解决器
        
        Returns:
            bool: 启动是否成功
        """
        try:
            self._is_running = True
            logger.info("ConflictResolver started successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to start ConflictResolver: {e}")
            return False
    
    async def stop(self) -> bool:
        """停止冲突解决器
        
        Returns:
            bool: 停止是否成功
        """
        try:
            self._is_running = False
            logger.info("ConflictResolver stopped successfully")
            return True
        except Exception as e:
            logger.error(f"Error stopping ConflictResolver: {e}")
            return False
    
    async def detect_conflicts(self, signals: List[Any]) -> Dict[str, Any]:
        """检测信号冲突
        
        Args:
            signals: 信号列表
            
        Returns:
            Dict[str, Any]: 冲突检测结果
        """
        try:
            if not self._is_running:
                raise SignalConflictException([], "resolver_not_running")
            
            conflicts = []
            
            # 转换为标准信号格式
            standard_signals = await self._convert_to_standard_signals(signals)
            
            # 检测各种类型的冲突
            direction_conflicts = await self._detect_direction_conflicts(standard_signals)
            strength_conflicts = await self._detect_strength_conflicts(standard_signals)
            timing_conflicts = await self._detect_timing_conflicts(standard_signals)
            priority_conflicts = await self._detect_priority_conflicts(standard_signals)
            resource_conflicts = await self._detect_resource_conflicts(standard_signals)
            
            conflicts.extend(direction_conflicts)
            conflicts.extend(strength_conflicts)
            conflicts.extend(timing_conflicts)
            conflicts.extend(priority_conflicts)
            conflicts.extend(resource_conflicts)
            
            # 更新统计信息
            self._conflict_statistics['total_conflicts'] += len(conflicts)
            for conflict in conflicts:
                conflict_type = conflict['type']
                if conflict_type in self._conflict_statistics['by_type']:
                    self._conflict_statistics['by_type'][conflict_type] += 1
            
            result = {
                'conflicts': conflicts,
                'conflict_count': len(conflicts),
                'has_conflicts': len(conflicts) > 0,
                'detection_time': datetime.now(),
                'signals_analyzed': len(standard_signals)
            }
            
            if conflicts:
                logger.warning(f"Detected {len(conflicts)} conflicts in {len(signals)} signals")
            
            return result
            
        except Exception as e:
            logger.error(f"Conflict detection failed: {e}")
            raise SignalConflictException([], "detection_failed", {"error": str(e)})
    
    async def resolve_conflicts(self, conflicts: Dict[str, Any]) -> List[Any]:
        """解决信号冲突
        
        Args:
            conflicts: 冲突信息
            
        Returns:
            List[Any]: 解决后的信号列表
        """
        try:
            if not conflicts.get('has_conflicts', False):
                return []
            
            resolved_signals = []
            conflict_list = conflicts.get('conflicts', [])
            
            # 按冲突类型分组
            conflicts_by_type = {}
            for conflict in conflict_list:
                conflict_type = ConflictType(conflict['type'])
                if conflict_type not in conflicts_by_type:
                    conflicts_by_type[conflict_type] = []
                conflicts_by_type[conflict_type].append(conflict)
            
            # 按优先级顺序解决冲突
            resolution_order = [
                ConflictType.PRIORITY_CONFLICT,
                ConflictType.DIRECTION_CONFLICT,
                ConflictType.TIMING_CONFLICT,
                ConflictType.STRENGTH_CONFLICT,
                ConflictType.RESOURCE_CONFLICT
            ]
            
            for conflict_type in resolution_order:
                if conflict_type in conflicts_by_type:
                    type_conflicts = conflicts_by_type[conflict_type]
                    strategy = self._resolution_strategies[conflict_type]
                    
                    for conflict in type_conflicts:
                        try:
                            resolved_signal = await strategy(conflict)
                            if resolved_signal:
                                resolved_signals.append(resolved_signal)
                                self._conflict_statistics['resolved_conflicts'] += 1
                        except Exception as e:
                            logger.error(f"Failed to resolve {conflict_type.value} conflict: {e}")
            
            logger.info(f"Resolved {len(resolved_signals)} conflicts")
            return resolved_signals
            
        except Exception as e:
            logger.error(f"Conflict resolution failed: {e}")
            raise SignalConflictException([], "resolution_failed", {"error": str(e)})
    
    def get_conflict_statistics(self) -> Dict[str, Any]:
        """获取冲突统计信息
        
        Returns:
            Dict[str, Any]: 冲突统计
        """
        return self._conflict_statistics.copy()
    
    # 私有方法实现
    async def _convert_to_standard_signals(self, signals: List[Any]) -> List[StandardSignal]:
        """转换为标准信号格式"""
        standard_signals = []
        for i, signal in enumerate(signals):
            if isinstance(signal, StandardSignal):
                standard_signals.append(signal)
            else:
                # 转换为标准格式
                standard_signal = StandardSignal(
                    signal_id=f"signal_{i}",
                    signal_type="unknown",
                    source="unknown",
                    target="unknown",
                    strength=0.5,
                    confidence=0.5,
                    data=signal if isinstance(signal, dict) else {"raw_signal": signal}
                )
                standard_signals.append(standard_signal)
        return standard_signals
    
    async def _detect_direction_conflicts(self, signals: List[StandardSignal]) -> List[Dict[str, Any]]:
        """检测方向冲突"""
        conflicts = []
        
        # 按目标分组信号
        signals_by_target = {}
        for signal in signals:
            target = signal.target
            if target not in signals_by_target:
                signals_by_target[target] = []
            signals_by_target[target].append(signal)
        
        # 检测同一目标的相反信号
        for target, target_signals in signals_by_target.items():
            if len(target_signals) > 1:
                for i in range(len(target_signals)):
                    for j in range(i + 1, len(target_signals)):
                        signal1, signal2 = target_signals[i], target_signals[j]
                        
                        # 检查是否为相反方向
                        if self._are_opposite_directions(signal1, signal2):
                            conflicts.append({
                                'type': ConflictType.DIRECTION_CONFLICT.value,
                                'signals': [signal1.signal_id, signal2.signal_id],
                                'target': target,
                                'description': f"Opposite directions for target {target}",
                                'severity': 'high'
                            })
        
        return conflicts
    
    async def _detect_strength_conflicts(self, signals: List[StandardSignal]) -> List[Dict[str, Any]]:
        """检测强度冲突"""
        conflicts = []
        
        # 检测强度差异过大的信号
        for i in range(len(signals)):
            for j in range(i + 1, len(signals)):
                signal1, signal2 = signals[i], signals[j]
                
                if (signal1.target == signal2.target and 
                    abs(signal1.strength - signal2.strength) > self._detection_rules['strength_threshold']):
                    conflicts.append({
                        'type': ConflictType.STRENGTH_CONFLICT.value,
                        'signals': [signal1.signal_id, signal2.signal_id],
                        'strength_diff': abs(signal1.strength - signal2.strength),
                        'description': f"Large strength difference: {signal1.strength} vs {signal2.strength}",
                        'severity': 'medium'
                    })
        
        return conflicts
    
    async def _detect_timing_conflicts(self, signals: List[StandardSignal]) -> List[Dict[str, Any]]:
        """检测时机冲突"""
        conflicts = []
        
        # 检测时间窗口内的冲突信号
        current_time = datetime.now()
        timing_window = self._detection_rules['timing_window']
        
        for signal in signals:
            time_diff = (current_time - signal.timestamp).total_seconds()
            if signal.expiry_time and current_time > signal.expiry_time:
                conflicts.append({
                    'type': ConflictType.TIMING_CONFLICT.value,
                    'signals': [signal.signal_id],
                    'description': f"Signal expired: {signal.signal_id}",
                    'severity': 'medium'
                })
        
        return conflicts
    
    async def _detect_priority_conflicts(self, signals: List[StandardSignal]) -> List[Dict[str, Any]]:
        """检测优先级冲突"""
        conflicts = []
        
        # 检测优先级差异过大的信号
        for i in range(len(signals)):
            for j in range(i + 1, len(signals)):
                signal1, signal2 = signals[i], signals[j]
                
                if (signal1.target == signal2.target and
                    abs(signal1.priority - signal2.priority) >= self._detection_rules['priority_difference']):
                    conflicts.append({
                        'type': ConflictType.PRIORITY_CONFLICT.value,
                        'signals': [signal1.signal_id, signal2.signal_id],
                        'priority_diff': abs(signal1.priority - signal2.priority),
                        'description': f"Priority conflict: {signal1.priority} vs {signal2.priority}",
                        'severity': 'low'
                    })
        
        return conflicts
    
    async def _detect_resource_conflicts(self, signals: List[StandardSignal]) -> List[Dict[str, Any]]:
        """检测资源冲突"""
        conflicts = []
        
        # 检测资源竞争
        resource_usage = {}
        for signal in signals:
            # 假设信号数据中包含资源需求信息
            if 'resource_requirements' in signal.data:
                requirements = signal.data['resource_requirements']
                for resource, amount in requirements.items():
                    if resource not in resource_usage:
                        resource_usage[resource] = 0
                    resource_usage[resource] += amount
        
        # 检查资源超限
        for resource, usage in resource_usage.items():
            if usage > 1.0:  # 假设资源上限为1.0
                conflicts.append({
                    'type': ConflictType.RESOURCE_CONFLICT.value,
                    'resource': resource,
                    'usage': usage,
                    'description': f"Resource {resource} over-allocated: {usage}",
                    'severity': 'high'
                })
        
        return conflicts
    
    def _are_opposite_directions(self, signal1: StandardSignal, signal2: StandardSignal) -> bool:
        """检查两个信号是否方向相反"""
        # 简化的方向检测逻辑
        type1, type2 = signal1.signal_type, signal2.signal_type
        
        opposite_pairs = [
            ('buy', 'sell'),
            ('long', 'short'),
            ('increase', 'decrease'),
            ('bullish', 'bearish')
        ]
        
        for pair in opposite_pairs:
            if (type1 in pair[0] and type2 in pair[1]) or (type1 in pair[1] and type2 in pair[0]):
                return True
        
        return False
    
    # 冲突解决策略实现
    async def _resolve_direction_conflict(self, conflict: Dict[str, Any]) -> Optional[Any]:
        """解决方向冲突"""
        # 基于优先级和置信度选择信号
        strategy = self.config.signal_router.conflict_resolution_strategy
        
        if strategy == "priority_based":
            # 选择优先级最高的信号
            return {"resolution": "priority_based", "conflict": conflict}
        elif strategy == "confidence_based":
            # 选择置信度最高的信号
            return {"resolution": "confidence_based", "conflict": conflict}
        else:
            # 混合策略
            return {"resolution": "hybrid", "conflict": conflict}
    
    async def _resolve_strength_conflict(self, conflict: Dict[str, Any]) -> Optional[Any]:
        """解决强度冲突"""
        # 取平均值或加权平均
        return {"resolution": "average_strength", "conflict": conflict}
    
    async def _resolve_timing_conflict(self, conflict: Dict[str, Any]) -> Optional[Any]:
        """解决时机冲突"""
        # 移除过期信号
        return {"resolution": "remove_expired", "conflict": conflict}
    
    async def _resolve_priority_conflict(self, conflict: Dict[str, Any]) -> Optional[Any]:
        """解决优先级冲突"""
        # 选择高优先级信号
        return {"resolution": "high_priority", "conflict": conflict}
    
    async def _resolve_resource_conflict(self, conflict: Dict[str, Any]) -> Optional[Any]:
        """解决资源冲突"""
        # 按比例分配资源
        return {"resolution": "proportional_allocation", "conflict": conflict}
