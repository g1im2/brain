"""
信号处理器实现

负责信号的验证、转换、过滤和增强处理。
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
import hashlib

from ..models import StandardSignal
from ..config import IntegrationConfig
from ..exceptions import SignalValidationException

logger = logging.getLogger(__name__)


class SignalProcessor:
    """信号处理器实现类
    
    负责信号的验证、转换、过滤和增强处理，
    确保信号质量和处理效率。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化信号处理器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False
        
        # 验证规则
        self._validation_rules = {
            'format_check': self._validate_format,
            'range_check': self._validate_range,
            'constraint_check': self._validate_constraints,
            'risk_check': self._validate_risk
        }
        
        # 处理统计
        self._processing_statistics = {
            'total_processed': 0,
            'validation_passed': 0,
            'validation_failed': 0,
            'processing_time_total': 0.0
        }
        
        # 信号缓存
        self._signal_cache: Dict[str, Any] = {}
        self._cache_ttl = timedelta(minutes=5)
        
        logger.info("SignalProcessor initialized")
    
    async def start(self) -> bool:
        """启动信号处理器
        
        Returns:
            bool: 启动是否成功
        """
        try:
            self._is_running = True
            logger.info("SignalProcessor started successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to start SignalProcessor: {e}")
            return False
    
    async def stop(self) -> bool:
        """停止信号处理器
        
        Returns:
            bool: 停止是否成功
        """
        try:
            self._is_running = False
            logger.info("SignalProcessor stopped successfully")
            return True
        except Exception as e:
            logger.error(f"Error stopping SignalProcessor: {e}")
            return False
    
    async def validate_signal(self, signal: StandardSignal) -> Dict[str, Any]:
        """验证信号
        
        Args:
            signal: 标准信号对象
            
        Returns:
            Dict[str, Any]: 验证结果
        """
        try:
            start_time = datetime.now()
            errors = []
            warnings = []
            
            # 执行各种验证规则
            for rule_name, rule_func in self._validation_rules.items():
                try:
                    result = await rule_func(signal)
                    if not result['valid']:
                        if result.get('severity') == 'error':
                            errors.extend(result.get('messages', []))
                        else:
                            warnings.extend(result.get('messages', []))
                except Exception as e:
                    errors.append(f"Validation rule {rule_name} failed: {str(e)}")
            
            # 计算验证结果
            is_valid = len(errors) == 0
            validation_score = 1.0 - (len(errors) * 0.3 + len(warnings) * 0.1)
            validation_score = max(0.0, min(1.0, validation_score))
            
            # 更新统计
            self._processing_statistics['total_processed'] += 1
            if is_valid:
                self._processing_statistics['validation_passed'] += 1
            else:
                self._processing_statistics['validation_failed'] += 1
            
            processing_time = (datetime.now() - start_time).total_seconds()
            self._processing_statistics['processing_time_total'] += processing_time
            
            result = {
                'valid': is_valid,
                'score': validation_score,
                'errors': errors,
                'warnings': warnings,
                'processing_time': processing_time,
                'timestamp': datetime.now()
            }
            
            logger.debug(f"Signal validation completed: {signal.signal_id}, valid: {is_valid}")
            return result
            
        except Exception as e:
            logger.error(f"Signal validation failed: {e}")
            raise SignalValidationException(signal.signal_id, [str(e)])
    
    async def process_signal(self, signal: Any) -> Any:
        """处理信号
        
        Args:
            signal: 原始信号
            
        Returns:
            Any: 处理后的信号
        """
        try:
            start_time = datetime.now()
            
            # 转换为标准格式
            if not isinstance(signal, StandardSignal):
                standard_signal = await self._convert_to_standard_signal(signal)
            else:
                standard_signal = signal
            
            # 检查缓存
            cache_key = self._generate_cache_key(standard_signal)
            cached_result = self._get_from_cache(cache_key)
            if cached_result:
                logger.debug(f"Using cached result for signal: {standard_signal.signal_id}")
                return cached_result
            
            # 信号增强
            enhanced_signal = await self._enhance_signal(standard_signal)
            
            # 信号过滤
            filtered_signal = await self._filter_signal(enhanced_signal)
            
            # 信号压缩（如果启用）
            if self.config.signal_router.enable_signal_compression:
                compressed_signal = await self._compress_signal(filtered_signal)
            else:
                compressed_signal = filtered_signal
            
            # 缓存结果
            self._cache_result(cache_key, compressed_signal)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.debug(f"Signal processing completed: {standard_signal.signal_id}, time: {processing_time:.3f}s")
            
            return compressed_signal
            
        except Exception as e:
            logger.error(f"Signal processing failed: {e}")
            raise
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """获取处理统计信息
        
        Returns:
            Dict[str, Any]: 处理统计
        """
        stats = self._processing_statistics.copy()
        if stats['total_processed'] > 0:
            stats['average_processing_time'] = stats['processing_time_total'] / stats['total_processed']
            stats['validation_success_rate'] = stats['validation_passed'] / stats['total_processed']
        else:
            stats['average_processing_time'] = 0.0
            stats['validation_success_rate'] = 0.0
        
        stats['cache_size'] = len(self._signal_cache)
        return stats
    
    # 私有方法实现
    async def _convert_to_standard_signal(self, signal: Any) -> StandardSignal:
        """转换为标准信号格式"""
        if isinstance(signal, dict):
            return StandardSignal(
                signal_id=signal.get('id', f"signal_{hash(str(signal))}"),
                signal_type=signal.get('type', 'unknown'),
                source=signal.get('source', 'unknown'),
                target=signal.get('target', 'unknown'),
                strength=signal.get('strength', 0.5),
                confidence=signal.get('confidence', 0.5),
                data=signal
            )
        else:
            return StandardSignal(
                signal_id=f"signal_{hash(str(signal))}",
                signal_type='raw',
                source='unknown',
                target='unknown',
                strength=0.5,
                confidence=0.5,
                data={'raw_signal': signal}
            )
    
    async def _enhance_signal(self, signal: StandardSignal) -> StandardSignal:
        """增强信号"""
        # 添加时间戳
        if 'timestamp' not in signal.data:
            signal.data['timestamp'] = datetime.now().isoformat()
        
        # 添加处理标记
        signal.data['processed'] = True
        signal.data['processor_version'] = '1.0'
        
        # 计算信号哈希
        signal.data['signal_hash'] = self._calculate_signal_hash(signal)
        
        return signal
    
    async def _filter_signal(self, signal: StandardSignal) -> StandardSignal:
        """过滤信号"""
        # 检查信号是否过期
        if signal.is_expired:
            logger.warning(f"Signal {signal.signal_id} is expired")
            signal.strength *= 0.5  # 降低过期信号强度
        
        # 检查信号强度阈值
        min_strength = 0.1
        if signal.strength < min_strength:
            logger.debug(f"Signal {signal.signal_id} strength below threshold: {signal.strength}")
            signal.strength = min_strength
        
        # 检查置信度阈值
        min_confidence = 0.1
        if signal.confidence < min_confidence:
            logger.debug(f"Signal {signal.signal_id} confidence below threshold: {signal.confidence}")
            signal.confidence = min_confidence
        
        return signal
    
    async def _compress_signal(self, signal: StandardSignal) -> StandardSignal:
        """压缩信号"""
        # 移除不必要的数据字段
        compressed_data = {}
        essential_fields = ['timestamp', 'processed', 'signal_hash']
        
        for field in essential_fields:
            if field in signal.data:
                compressed_data[field] = signal.data[field]
        
        # 保留核心业务数据
        if 'core_data' in signal.data:
            compressed_data['core_data'] = signal.data['core_data']
        
        signal.data = compressed_data
        return signal
    
    def _generate_cache_key(self, signal: StandardSignal) -> str:
        """生成缓存键"""
        key_data = {
            'signal_type': signal.signal_type,
            'source': signal.source,
            'target': signal.target,
            'data_hash': self._calculate_signal_hash(signal)
        }
        return hashlib.md5(json.dumps(key_data, sort_keys=True).encode()).hexdigest()
    
    def _calculate_signal_hash(self, signal: StandardSignal) -> str:
        """计算信号哈希"""
        hash_data = {
            'signal_type': signal.signal_type,
            'source': signal.source,
            'target': signal.target,
            'strength': round(signal.strength, 3),
            'confidence': round(signal.confidence, 3),
            'data': signal.data
        }
        return hashlib.md5(json.dumps(hash_data, sort_keys=True).encode()).hexdigest()
    
    def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        """从缓存获取结果"""
        if cache_key in self._signal_cache:
            cached_item = self._signal_cache[cache_key]
            if datetime.now() - cached_item['timestamp'] < self._cache_ttl:
                return cached_item['data']
            else:
                # 清理过期缓存
                del self._signal_cache[cache_key]
        return None
    
    def _cache_result(self, cache_key: str, result: Any) -> None:
        """缓存结果"""
        self._signal_cache[cache_key] = {
            'data': result,
            'timestamp': datetime.now()
        }
        
        # 限制缓存大小
        max_cache_size = 1000
        if len(self._signal_cache) > max_cache_size:
            # 移除最旧的缓存项
            oldest_key = min(self._signal_cache.keys(), 
                           key=lambda k: self._signal_cache[k]['timestamp'])
            del self._signal_cache[oldest_key]
    
    # 验证规则实现
    async def _validate_format(self, signal: StandardSignal) -> Dict[str, Any]:
        """验证信号格式"""
        errors = []
        
        # 检查必要字段
        if not signal.signal_id:
            errors.append("Signal ID is required")
        if not signal.signal_type:
            errors.append("Signal type is required")
        if not signal.source:
            errors.append("Signal source is required")
        if not signal.target:
            errors.append("Signal target is required")
        
        return {
            'valid': len(errors) == 0,
            'messages': errors,
            'severity': 'error'
        }
    
    async def _validate_range(self, signal: StandardSignal) -> Dict[str, Any]:
        """验证信号范围"""
        errors = []
        warnings = []
        
        # 检查强度范围
        if not 0 <= signal.strength <= 1:
            errors.append(f"Signal strength out of range: {signal.strength}")
        
        # 检查置信度范围
        if not 0 <= signal.confidence <= 1:
            errors.append(f"Signal confidence out of range: {signal.confidence}")
        
        # 检查优先级范围
        if not 1 <= signal.priority <= 5:
            warnings.append(f"Signal priority unusual: {signal.priority}")
        
        return {
            'valid': len(errors) == 0,
            'messages': errors + warnings,
            'severity': 'error' if errors else 'warning'
        }
    
    async def _validate_constraints(self, signal: StandardSignal) -> Dict[str, Any]:
        """验证信号约束"""
        warnings = []
        
        # 检查信号时效性
        if signal.expiry_time and signal.expiry_time < datetime.now():
            warnings.append("Signal has expired")
        
        # 检查数据完整性
        if not signal.data:
            warnings.append("Signal data is empty")
        
        return {
            'valid': True,  # 约束检查通常不会导致验证失败
            'messages': warnings,
            'severity': 'warning'
        }
    
    async def _validate_risk(self, signal: StandardSignal) -> Dict[str, Any]:
        """验证信号风险"""
        warnings = []
        
        # 检查高风险信号
        if signal.strength > 0.9 and signal.confidence < 0.5:
            warnings.append("High strength signal with low confidence")
        
        # 检查异常组合
        if signal.priority == 5 and signal.strength < 0.3:
            warnings.append("High priority signal with low strength")
        
        return {
            'valid': True,  # 风险检查通常不会导致验证失败
            'messages': warnings,
            'severity': 'warning'
        }
