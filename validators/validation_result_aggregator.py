"""
验证结果聚合器实现

负责聚合多种验证结果、计算综合评分、生成验证报告，
提供智能的验证结果分析和决策支持。
"""

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
from enum import Enum
from dataclasses import dataclass, field
import numpy as np
import threading

try:
    from ..models import ValidationResult, ValidationStatus
    from ..config import IntegrationConfig
    from ..exceptions import ValidationException
except ImportError:
    from models import ValidationResult, ValidationStatus
    from config import IntegrationConfig
    from exceptions import ValidationException

logger = logging.getLogger(__name__)


class AggregationMethod(Enum):
    """聚合方法枚举"""
    WEIGHTED_AVERAGE = "weighted_average"
    HARMONIC_MEAN = "harmonic_mean"
    GEOMETRIC_MEAN = "geometric_mean"
    MIN_MAX_NORMALIZED = "min_max_normalized"
    CONFIDENCE_WEIGHTED = "confidence_weighted"
    ADAPTIVE = "adaptive"


@dataclass
class ValidationWeight:
    """验证权重配置"""
    validation_type: str
    base_weight: float
    confidence_factor: float = 1.0
    time_decay_factor: float = 1.0
    quality_factor: float = 1.0
    
    @property
    def effective_weight(self) -> float:
        """计算有效权重"""
        return self.base_weight * self.confidence_factor * self.time_decay_factor * self.quality_factor


@dataclass
class AggregationResult:
    """聚合结果"""
    aggregation_id: str
    overall_score: float
    confidence_score: float
    risk_score: float
    recommendation: str
    component_scores: Dict[str, float]
    weight_distribution: Dict[str, float]
    quality_metrics: Dict[str, float]
    aggregation_method: AggregationMethod
    input_count: int
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ValidationResultAggregator:
    """验证结果聚合器实现类
    
    负责聚合多种验证结果、计算综合评分、生成验证报告，
    提供智能的验证结果分析和决策支持。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化验证结果聚合器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False
        
        # 权重配置
        self._validation_weights: Dict[str, ValidationWeight] = {}
        self._initialize_default_weights()
        
        # 聚合历史
        self._aggregation_history: List[AggregationResult] = []
        
        # 聚合统计
        self._aggregation_statistics = {
            'total_aggregations': 0,
            'successful_aggregations': 0,
            'failed_aggregations': 0,
            'average_processing_time': 0.0,
            'by_method': defaultdict(int)
        }
        
        # 质量阈值
        self._quality_thresholds = {
            'minimum_score': 0.3,
            'confidence_threshold': 0.5,
            'risk_threshold': 0.7,
            'consistency_threshold': 0.8
        }
        
        # 锁和同步
        self._aggregation_lock = threading.RLock()
        
        logger.info("ValidationResultAggregator initialized")
    
    async def start(self) -> bool:
        """启动验证结果聚合器
        
        Returns:
            bool: 启动是否成功
        """
        try:
            self._is_running = True
            logger.info("ValidationResultAggregator started successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to start ValidationResultAggregator: {e}")
            return False
    
    async def stop(self) -> bool:
        """停止验证结果聚合器
        
        Returns:
            bool: 停止是否成功
        """
        try:
            self._is_running = False
            logger.info("ValidationResultAggregator stopped successfully")
            return True
        except Exception as e:
            logger.error(f"Error stopping ValidationResultAggregator: {e}")
            return False
    
    async def aggregate_validation_results(self, 
                                         validation_results: List[ValidationResult],
                                         method: AggregationMethod = AggregationMethod.ADAPTIVE) -> AggregationResult:
        """聚合验证结果
        
        Args:
            validation_results: 验证结果列表
            method: 聚合方法
            
        Returns:
            AggregationResult: 聚合结果
        """
        if not self._is_running:
            raise ValidationException("ValidationResultAggregator is not running")
        
        if not validation_results:
            raise ValidationException("No validation results provided")
        
        start_time = datetime.now()
        aggregation_id = str(uuid.uuid4())
        
        try:
            logger.debug(f"Starting validation result aggregation: {aggregation_id}")
            
            # 预处理验证结果
            processed_results = await self._preprocess_results(validation_results)
            
            # 计算权重
            weights = await self._calculate_weights(processed_results)
            
            # 选择聚合方法
            if method == AggregationMethod.ADAPTIVE:
                method = await self._select_optimal_method(processed_results)
            
            # 执行聚合
            aggregation_result = await self._execute_aggregation(
                aggregation_id, processed_results, weights, method
            )
            
            # 计算质量指标
            quality_metrics = await self._calculate_quality_metrics(
                processed_results, aggregation_result
            )
            aggregation_result.quality_metrics = quality_metrics
            
            # 生成建议
            recommendation = await self._generate_recommendation(aggregation_result)
            aggregation_result.recommendation = recommendation
            
            # 记录聚合历史
            with self._aggregation_lock:
                self._aggregation_history.append(aggregation_result)
                self._update_aggregation_statistics(True, 
                    (datetime.now() - start_time).total_seconds(), method)
            
            logger.info(f"Validation result aggregation completed: {aggregation_id}, "
                       f"score: {aggregation_result.overall_score:.3f}")
            
            return aggregation_result
            
        except Exception as e:
            with self._aggregation_lock:
                self._update_aggregation_statistics(False, 0, method)
            logger.error(f"Validation result aggregation failed: {aggregation_id}, error: {e}")
            raise ValidationException(f"Aggregation failed: {e}")
    
    def configure_weights(self, weights: Dict[str, ValidationWeight]) -> bool:
        """配置验证权重
        
        Args:
            weights: 权重配置字典
            
        Returns:
            bool: 配置是否成功
        """
        try:
            with self._aggregation_lock:
                self._validation_weights.update(weights)
            logger.info(f"Validation weights configured: {list(weights.keys())}")
            return True
        except Exception as e:
            logger.error(f"Failed to configure weights: {e}")
            return False
    
    def get_aggregation_statistics(self) -> Dict[str, Any]:
        """获取聚合统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        with self._aggregation_lock:
            stats = self._aggregation_statistics.copy()
            stats['history_size'] = len(self._aggregation_history)
            stats['configured_weights'] = len(self._validation_weights)
            
            # 计算成功率
            total = stats['total_aggregations']
            if total > 0:
                stats['success_rate'] = stats['successful_aggregations'] / total
            else:
                stats['success_rate'] = 0.0
            
            return stats
    
    def get_recent_aggregations(self, count: int = 10) -> List[AggregationResult]:
        """获取最近的聚合结果
        
        Args:
            count: 返回数量
            
        Returns:
            List[AggregationResult]: 最近的聚合结果
        """
        with self._aggregation_lock:
            return self._aggregation_history[-count:] if self._aggregation_history else []
    
    # 私有方法实现
    def _initialize_default_weights(self) -> None:
        """初始化默认权重"""
        default_weights = {
            'historical': ValidationWeight(
                validation_type='historical',
                base_weight=0.6,
                confidence_factor=1.0,
                time_decay_factor=0.9
            ),
            'forward': ValidationWeight(
                validation_type='forward',
                base_weight=0.4,
                confidence_factor=1.0,
                time_decay_factor=1.0
            ),
            'technical': ValidationWeight(
                validation_type='technical',
                base_weight=0.3,
                confidence_factor=0.8,
                time_decay_factor=1.0
            ),
            'fundamental': ValidationWeight(
                validation_type='fundamental',
                base_weight=0.2,
                confidence_factor=0.9,
                time_decay_factor=0.8
            )
        }
        
        self._validation_weights.update(default_weights)
    
    async def _preprocess_results(self, results: List[ValidationResult]) -> List[ValidationResult]:
        """预处理验证结果"""
        processed_results = []
        
        for result in results:
            # 验证结果有效性
            if not self._is_valid_result(result):
                logger.warning(f"Invalid validation result: {result.validation_id}")
                continue
            
            # 标准化评分
            normalized_result = await self._normalize_result(result)
            processed_results.append(normalized_result)
        
        return processed_results
    
    def _is_valid_result(self, result: ValidationResult) -> bool:
        """检查验证结果有效性"""
        if result.validation_status != ValidationStatus.COMPLETED:
            return False
        
        if not (0 <= result.validation_score <= 1):
            return False
        
        if result.execution_time < 0:
            return False
        
        return True
    
    async def _normalize_result(self, result: ValidationResult) -> ValidationResult:
        """标准化验证结果"""
        # 确保评分在有效范围内
        normalized_score = max(0.0, min(1.0, result.validation_score))
        
        # 创建标准化的结果副本
        normalized_result = ValidationResult(
            validation_id=result.validation_id,
            validation_type=result.validation_type,
            validation_status=result.validation_status,
            validation_score=normalized_score,
            confidence_adjustment=result.confidence_adjustment,
            risk_assessment=result.risk_assessment,
            recommendations=result.recommendations,
            execution_time=result.execution_time,
            timestamp=result.timestamp
        )
        
        return normalized_result
    
    async def _calculate_weights(self, results: List[ValidationResult]) -> Dict[str, float]:
        """计算权重"""
        weights = {}
        total_weight = 0.0
        
        for result in results:
            validation_type = result.validation_type
            weight_config = self._validation_weights.get(validation_type)
            
            if weight_config:
                # 计算时间衰减因子
                time_factor = self._calculate_time_decay_factor(result, weight_config)
                
                # 计算质量因子
                quality_factor = self._calculate_quality_factor(result)
                
                # 更新权重配置
                weight_config.time_decay_factor = time_factor
                weight_config.quality_factor = quality_factor
                
                # 计算有效权重
                effective_weight = weight_config.effective_weight
                weights[validation_type] = effective_weight
                total_weight += effective_weight
            else:
                # 默认权重
                weights[validation_type] = 0.1
                total_weight += 0.1
        
        # 标准化权重
        if total_weight > 0:
            weights = {k: v / total_weight for k, v in weights.items()}
        
        return weights
    
    def _calculate_time_decay_factor(self, result: ValidationResult, 
                                   weight_config: ValidationWeight) -> float:
        """计算时间衰减因子"""
        if not result.timestamp:
            return 1.0
        
        # 计算时间差（小时）
        time_diff = (datetime.now() - result.timestamp).total_seconds() / 3600
        
        # 应用指数衰减
        decay_rate = 0.1  # 每小时衰减10%
        time_factor = np.exp(-decay_rate * time_diff)
        
        return max(0.1, time_factor)  # 最小保留10%权重
    
    def _calculate_quality_factor(self, result: ValidationResult) -> float:
        """计算质量因子"""
        quality_score = 1.0
        
        # 基于执行时间的质量评估
        if result.execution_time > 300:  # 超过5分钟
            quality_score *= 0.8
        elif result.execution_time > 60:  # 超过1分钟
            quality_score *= 0.9
        
        # 基于置信度调整的质量评估
        if abs(result.confidence_adjustment) > 0.3:
            quality_score *= 0.7
        elif abs(result.confidence_adjustment) > 0.1:
            quality_score *= 0.9
        
        return quality_score
    
    async def _select_optimal_method(self, results: List[ValidationResult]) -> AggregationMethod:
        """选择最优聚合方法"""
        # 基于结果特征选择方法
        score_variance = np.var([r.validation_score for r in results])
        result_count = len(results)
        
        if score_variance < 0.01:  # 结果一致性高
            return AggregationMethod.WEIGHTED_AVERAGE
        elif result_count < 3:  # 结果数量少
            return AggregationMethod.CONFIDENCE_WEIGHTED
        elif score_variance > 0.1:  # 结果差异大
            return AggregationMethod.HARMONIC_MEAN
        else:
            return AggregationMethod.GEOMETRIC_MEAN
    
    async def _execute_aggregation(self, aggregation_id: str, 
                                 results: List[ValidationResult],
                                 weights: Dict[str, float],
                                 method: AggregationMethod) -> AggregationResult:
        """执行聚合计算"""
        # 提取评分和权重
        scores = []
        result_weights = []
        component_scores = {}
        
        for result in results:
            scores.append(result.validation_score)
            weight = weights.get(result.validation_type, 0.1)
            result_weights.append(weight)
            component_scores[result.validation_type] = result.validation_score
        
        # 根据方法计算聚合评分
        if method == AggregationMethod.WEIGHTED_AVERAGE:
            overall_score = np.average(scores, weights=result_weights)
        elif method == AggregationMethod.HARMONIC_MEAN:
            overall_score = len(scores) / sum(1/s if s > 0 else float('inf') for s in scores)
        elif method == AggregationMethod.GEOMETRIC_MEAN:
            overall_score = np.power(np.prod(scores), 1/len(scores))
        elif method == AggregationMethod.CONFIDENCE_WEIGHTED:
            confidence_weights = [abs(r.confidence_adjustment) + 0.5 for r in results]
            overall_score = np.average(scores, weights=confidence_weights)
        else:
            overall_score = np.average(scores, weights=result_weights)
        
        # 计算置信度评分
        confidence_score = self._calculate_confidence_score(results, weights)
        
        # 计算风险评分
        risk_score = self._calculate_risk_score(results)
        
        # 创建聚合结果
        aggregation_result = AggregationResult(
            aggregation_id=aggregation_id,
            overall_score=float(overall_score),
            confidence_score=confidence_score,
            risk_score=risk_score,
            recommendation="",  # 稍后生成
            component_scores=component_scores,
            weight_distribution=weights,
            quality_metrics={},  # 稍后计算
            aggregation_method=method,
            input_count=len(results)
        )
        
        return aggregation_result
    
    def _calculate_confidence_score(self, results: List[ValidationResult], 
                                  weights: Dict[str, float]) -> float:
        """计算置信度评分"""
        confidence_scores = []
        confidence_weights = []
        
        for result in results:
            # 基础置信度
            base_confidence = 0.5 + result.confidence_adjustment
            confidence_scores.append(base_confidence)
            
            # 权重
            weight = weights.get(result.validation_type, 0.1)
            confidence_weights.append(weight)
        
        # 加权平均置信度
        weighted_confidence = np.average(confidence_scores, weights=confidence_weights)
        
        # 一致性调整
        consistency_factor = 1.0 - np.std(confidence_scores) / 2
        
        final_confidence = weighted_confidence * consistency_factor
        return max(0.0, min(1.0, final_confidence))
    
    def _calculate_risk_score(self, results: List[ValidationResult]) -> float:
        """计算风险评分"""
        risk_scores = []
        
        for result in results:
            # 从风险评估中提取风险评分
            risk_assessment = result.risk_assessment or {}
            risk_score = risk_assessment.get('overall_risk', 0.5)
            risk_scores.append(risk_score)
        
        # 取最大风险作为整体风险
        overall_risk = max(risk_scores) if risk_scores else 0.5
        
        # 考虑结果一致性对风险的影响
        if len(risk_scores) > 1:
            risk_variance = np.var(risk_scores)
            if risk_variance > 0.1:  # 风险评估不一致
                overall_risk = min(1.0, overall_risk + 0.1)
        
        return overall_risk
    
    async def _calculate_quality_metrics(self, results: List[ValidationResult],
                                       aggregation_result: AggregationResult) -> Dict[str, float]:
        """计算质量指标"""
        metrics = {}
        
        # 数据完整性
        metrics['completeness'] = len(results) / max(1, len(self._validation_weights))
        
        # 结果一致性
        scores = [r.validation_score for r in results]
        metrics['consistency'] = 1.0 - (np.std(scores) if len(scores) > 1 else 0.0)
        
        # 时效性
        if results:
            avg_age = np.mean([(datetime.now() - r.timestamp).total_seconds() / 3600 
                              for r in results if r.timestamp])
            metrics['timeliness'] = max(0.0, 1.0 - avg_age / 24)  # 24小时内为满分
        else:
            metrics['timeliness'] = 0.0
        
        # 置信度质量
        metrics['confidence_quality'] = aggregation_result.confidence_score
        
        # 综合质量评分
        metrics['overall_quality'] = np.mean(list(metrics.values()))
        
        return metrics
    
    async def _generate_recommendation(self, result: AggregationResult) -> str:
        """生成建议"""
        recommendations = []
        
        # 基于整体评分的建议
        if result.overall_score >= 0.8:
            recommendations.append("验证结果优秀，强烈建议执行")
        elif result.overall_score >= 0.6:
            recommendations.append("验证结果良好，建议执行")
        elif result.overall_score >= 0.4:
            recommendations.append("验证结果一般，谨慎执行")
        else:
            recommendations.append("验证结果较差，不建议执行")
        
        # 基于置信度的建议
        if result.confidence_score < 0.5:
            recommendations.append("置信度较低，建议增加验证样本")
        
        # 基于风险的建议
        if result.risk_score > 0.7:
            recommendations.append("风险较高，建议降低仓位或增加风控措施")
        
        # 基于质量指标的建议
        quality = result.quality_metrics.get('overall_quality', 0.5)
        if quality < 0.6:
            recommendations.append("验证质量有待提升，建议优化验证流程")
        
        return "; ".join(recommendations)
    
    def _update_aggregation_statistics(self, success: bool, processing_time: float,
                                     method: AggregationMethod) -> None:
        """更新聚合统计"""
        self._aggregation_statistics['total_aggregations'] += 1
        self._aggregation_statistics['by_method'][method.value] += 1
        
        if success:
            self._aggregation_statistics['successful_aggregations'] += 1
            
            # 更新平均处理时间
            total_successful = self._aggregation_statistics['successful_aggregations']
            current_avg = self._aggregation_statistics['average_processing_time']
            self._aggregation_statistics['average_processing_time'] = (
                (current_avg * (total_successful - 1) + processing_time) / total_successful
            )
        else:
            self._aggregation_statistics['failed_aggregations'] += 1
